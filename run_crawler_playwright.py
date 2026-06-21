#!/usr/bin/env python3
"""
使用 Playwright 实现的多标签页异步爬虫
"""
import asyncio
import logging
import time
from pathlib import Path
import sys
from multiprocessing import Process, Queue, Event
from queue import Empty

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import DB_CONFIG, SCRAPER_CONFIG, CHROME_DEBUG_PORT
from bs4 import BeautifulSoup
from scrapers.i5i5j_scraper_playwright import I5I5JScraperPlaywright
from scrapers.auth_recovery import AuthRecoveryCoordinator
from etl.db_manager import DatabaseManager
from datetime import datetime
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

shared_queue = Queue(maxsize=10000)
stop_event = Event()


def calc_district_stats(unit_prices, prices, areas):
    """根据单价、总价、面积列表计算区域价格指标，返回 (avg, median, weighted_avg)"""
    if not unit_prices:
        return 0, 0, 0
    avg = sum(unit_prices) / len(unit_prices)

    sorted_p = sorted(unit_prices)
    n = len(sorted_p)
    if n % 2 == 0:
        median = (sorted_p[n // 2 - 1] + sorted_p[n // 2]) / 2
    else:
        median = sorted_p[n // 2]

    total_price = sum(prices)
    total_area = sum(areas)
    weighted = total_price / total_area if total_area > 0 else 0

    return avg, median, weighted



async def process_page(page, region_code, region_name, page_num, shared_queue, auth):
    """处理单个页面，返回 (outcome, listing_count, elapsed_ms)
    outcome: 'success' | 'no_data' | 'suspicious'
    """
    t0 = time.perf_counter()
    try:
        url = f"https://bj.5i5j.com/ershoufang/{region_code}/n{page_num}/"

        page_label = f"{region_name} P{page_num}"
        if not await auth.navigate(page, url, page_label):
            return 'suspicious', 0, (time.perf_counter() - t0) * 1000

        # 点击认证处理：恢复成功 = 正常，恢复失败 = hard failure
        try:
            body_text = await page.inner_text('body')
            if '点击页面或移动鼠标' in body_text:
                logger.info(f"点击认证 {region_name} P{page_num}，尝试恢复...")
                await page.mouse.move(random.randint(100, 500), random.randint(100, 400))
                await asyncio.sleep(0.3)
                await page.mouse.click(random.randint(100, 500), random.randint(100, 400))
                try:
                    await page.wait_for_selector('ul.pList', timeout=15000)
                    logger.info(f"认证恢复成功 {region_name} P{page_num}")
                except Exception:
                    if not await auth.recover_after_redirect(page, url, page_label):
                        logger.warning(f"登录恢复失败 {region_name} P{page_num}")
                        return 'suspicious', 0, (time.perf_counter() - t0) * 1000
                    try:
                        await page.wait_for_selector('ul.pList', timeout=5000)
                    except Exception:
                        logger.warning(f"认证恢复失败 {region_name} P{page_num}")
                        return 'suspicious', 0, (time.perf_counter() - t0) * 1000
        except Exception:
            pass

        try:
            await page.wait_for_selector('ul.pList', timeout=5000)
        except Exception:
            pass

        # 部分登录跳转会在 domcontentloaded 之后延迟发生，再检查一次。
        if not await auth.recover_after_redirect(page, url, page_label):
            return 'suspicious', 0, (time.perf_counter() - t0) * 1000
        try:
            await page.wait_for_selector('ul.pList', timeout=5000)
        except Exception:
            pass

        html = await page.content()
        soup = BeautifulSoup(html, 'lxml')
        house_list = soup.find('ul', {'class': 'pList'})

        if not house_list:
            for sel in ['.n_no_data', '.no-result', '.empty-tip']:
                if soup.select_one(sel):
                    return 'no_data', 0, (time.perf_counter() - t0) * 1000
            logger.warning(f"可疑页面 {region_name} P{page_num}: 无列表且非已知无数据")
            return 'suspicious', 0, (time.perf_counter() - t0) * 1000

        try:
            page_data = I5I5JScraperPlaywright.extract_information(soup)
            if page_data:
                for house in page_data:
                    house['region'] = region_name
                    try:
                        shared_queue.put(house, block=True, timeout=10)
                    except Exception:
                        logger.warning(f"队列满 {house.get('house_id', 'unknown')}")
                return 'success', len(page_data), (time.perf_counter() - t0) * 1000
            else:
                return 'no_data', 0, (time.perf_counter() - t0) * 1000
        except Exception as e:
            logger.error(f"提取失败 {region_name} P{page_num}: {e}")
            return 'suspicious', 0, (time.perf_counter() - t0) * 1000
        finally:
            await asyncio.sleep(random.uniform(*SCRAPER_CONFIG['delay_range']))
            try:
                await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                await page.mouse.wheel(0, random.randint(-200, 200))
            except Exception:
                pass
    except Exception as e:
        logger.error(f"页面异常 {region_name} P{page_num}: {e}")
        return 'suspicious', 0, (time.perf_counter() - t0) * 1000

async def run_multi_tab_worker_playwright(debug_port, district_tasks, shared_queue, window_size=3):
    """
    使用 Playwright 的多标签页并行爬取工作线程
    """
    worker_start_time = time.time()
    logger.info(f"🚀 Playwright 多标签页采集线程启动 (端口: {debug_port})，任务: {district_tasks}")
    
    # 登录预检：保持连接供第一个 batch 复用，避免断连重连导致 cookie 丢失
    login_scraper = I5I5JScraperPlaywright(debug_port=debug_port)
    await login_scraper.connect()
    logger.info("🔐 开始检查登录状态")
    await login_scraper.check_and_login()
    logger.info("✅ 登录检查完成")

    async def refresh_login():
        auth_scraper = I5I5JScraperPlaywright(debug_port=debug_port)
        await auth_scraper.connect()
        try:
            await auth_scraper.check_and_login()
        finally:
            await auth_scraper.close()

    auth = AuthRecoveryCoordinator(
        refresh_login, logger, label="[二手房] "
    )
    login_used = False

    try:
        for dist_code, dist_name in district_tasks.items():
            current_page = 1
            max_page = 2000
            restart_interval = SCRAPER_CONFIG['restart_interval']
            is_region_finished = False
            region_suspicious = False
            total_listings = 0
            region_start_time = time.time()
            logger.info(f"🚀 {dist_name}: 开始扫描")

            while current_page <= max_page and not is_region_finished:
                end_page = min(current_page + restart_interval - 1, max_page)

                if not login_used:
                    scraper = login_scraper
                    login_used = True
                else:
                    scraper = I5I5JScraperPlaywright(debug_port=debug_port)
                    await scraper.connect()
                await scraper.create_pages(window_size)

                page_queue = asyncio.Queue()
                batch_no_data_count = 0
                max_no_data_pages = 3
                pages_processed = 0
                last_progress_log = 0
                progress_interval = 50

                for page_num in range(current_page, end_page + 1):
                    await page_queue.put(page_num)

                total_page_time = 0  # 累计页面耗时，用于算均速

                async def task_worker(page):
                    nonlocal is_region_finished, total_listings, batch_no_data_count, current_page
                    nonlocal pages_processed, last_progress_log, total_page_time, region_suspicious
                    while not is_region_finished:
                        try:
                            page_num = await asyncio.wait_for(page_queue.get(), timeout=5)
                            outcome, listings_count, page_ms = await process_page(
                                page, dist_code, dist_name, page_num, shared_queue, auth
                            )
                            total_listings += listings_count
                            pages_processed += 1
                            total_page_time += page_ms
                            current_page = max(current_page, page_num + 1)
                            page_queue.task_done()

                            if outcome == 'suspicious':
                                region_suspicious = True
                                batch_no_data_count = 0  # 重置，继续爬
                            elif outcome == 'no_data':
                                batch_no_data_count += 1
                                if batch_no_data_count >= max_no_data_pages:
                                    avg_s = total_page_time / pages_processed / 1000
                                    logger.info(f"🛑 {dist_name}: {pages_processed}页 {total_listings}条 均{avg_s:.1f}s/页 停止")
                                    is_region_finished = True
                                    while not page_queue.empty():
                                        try:
                                            page_queue.get_nowait()
                                            page_queue.task_done()
                                        except asyncio.QueueEmpty:
                                            break
                                    break
                            else:  # success
                                batch_no_data_count = 0
                                if pages_processed - last_progress_log >= progress_interval:
                                    avg_s = total_page_time / pages_processed / 1000
                                    logger.info(f"📖 {dist_name}: {pages_processed}页 {total_listings}条 均{avg_s:.1f}s/页")
                                    last_progress_log = pages_processed
                        except asyncio.TimeoutError:
                            break
                        except Exception as e:
                            region_suspicious = True
                            logger.error(f"❌ {dist_name} P{page_num}: {e}")
                            try:
                                page_queue.task_done()
                            except:
                                pass
                            continue
                
                # 创建任务列表
                tasks = []
                for page in scraper.pages:
                    task = task_worker(page)
                    tasks.append(task)
                
                # 并行执行任务
                await asyncio.gather(*tasks)
                
                # 关闭当前批次的所有页面，释放资源
                logger.info(f"🔄 关闭 {dist_name} 批次的所有标签页，释放资源")
                await scraper.close()
                logger.info(f"✅ {dist_name} 批次资源释放完成")
            
            # 输出区域摘要信息
            region_time = time.time() - region_start_time
            logger.info(f"\n{'=' * 80}")
            logger.info(f"📈 {dist_name} 爬取完成")
            logger.info(f"{'=' * 80}")
            logger.info(f"📊 总耗时: {region_time:.2f} 秒")
            logger.info(f"📊 共爬取 {total_listings} 条房源")
            if total_listings > 0:
                avg_listing_time = region_time / total_listings * 1000
                logger.info(f"📊 平均每条房源处理时间: {avg_listing_time:.2f} 毫秒")

            # 发送区域完成信号给消费者
            # ok=True 仅当：正常结束 且 无任何可疑页面
            region_ok = is_region_finished and not region_suspicious
            reason = 'completed' if region_ok else ('suspicious_pages' if region_suspicious else 'failed_or_interrupted')
            shared_queue.put({
                '__control__': 'region_done',
                'region': dist_name,
                'ok': region_ok,
                'reason': reason,
                'listings': total_listings,
            })
            logger.info(f"{'✅' if region_ok else '⚠️'} {dist_name} 区域信号: ok={region_ok} reason={reason} ({total_listings}条)")
        
        worker_total_time = time.time() - worker_start_time
        logger.info(f"\n{'=' * 80}")
        logger.info(f"✅ Playwright 多标签页采集线程完成 (端口: {debug_port})")
        logger.info(f"⏱️  采集线程总耗时: {worker_total_time:.2f} 秒")
        logger.info(f"{'=' * 80}")
        
    except Exception as e:
        logger.error(f"多标签页采集线程失败 (端口: {debug_port}): {e}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")


# 全局数据库写入消费者
def global_db_consumer(queue, stop_event, db_config, regions):
    """全局数据库写入消费者"""
    start_time = time.time()
    logger.info("🚗 数据库写入线程启动")
    db_manager = DatabaseManager(db_config)
    processed_count = 0
    
    # 实时账本：存储各区的价格数据
    ledger = {}

    # 区域完成状态跟踪
    region_outcomes = {}  # {region: {'ok': bool, 'reason': str}}

    # 批量处理参数
    batch_size = 500  # 每批次处理100条数据
    property_batch = []  # 房源批量数据
    price_batch = []  # 价格历史批量数据
    
    # 启动时一次性加载全量二手房价格到内存（必须在 status 更新前，否则 WHERE status=1 查不到）
    logger.info("📥 加载全量二手房价格到内存...")
    all_prices = db_manager.load_property_prices()
    logger.info(f"📥 已加载 {len(all_prices)} 条价格记录")

    price_check_map = {}  # {house_id: (web_price, web_unit_price)} 批次内价格比对缓存

    # 初始状态更新：将所有区域的房源状态设为待确认
    logger.info("📋 开始更新所有区域的房源状态为待确认")
    for region in regions:
        try:
            db_manager.update_property_status(region=region, status=2)
            logger.info(f"✅ {region} 区域状态更新完成")
        except Exception as e:
            logger.error(f"❌ 更新 {region} 区域状态失败: {e}")

    def process_batch():
        """处理批量数据，全部内存比对，无 DB 查询"""
        nonlocal property_batch, price_batch, price_check_map
        t0 = time.perf_counter()

        for house_id, (web_price, web_unit_price) in price_check_map.items():
            last_price = all_prices.get(house_id)
            if last_price is None:
                price_batch.append({
                    'house_id': house_id, 'price': web_price,
                    'unit_price': web_unit_price,
                    'record_date': datetime.now().strftime('%Y-%m-%d')
                })
            elif float(web_price) != float(last_price):
                price_batch.append({
                    'house_id': house_id, 'price': web_price,
                    'unit_price': web_unit_price,
                    'record_date': datetime.now().strftime('%Y-%m-%d')
                })
            # 更新内存中的价格，下次比对用最新值
            all_prices[house_id] = web_price

        t2 = time.perf_counter()
        if property_batch:
            db_manager.batch_insert_property_details(property_batch)
            property_batch = []
        prop_ms = (time.perf_counter() - t2) * 1000

        t3 = time.perf_counter()
        if price_batch:
            db_manager.batch_insert_price_history(price_batch)
            price_batch = []
        price_ms = (time.perf_counter() - t3) * 1000
        price_check_map = {}

        total_ms = (time.perf_counter() - t0) * 1000
        # logger.info(f"📊 批次: 写房={prop_ms:.0f}ms 写价={price_ms:.0f}ms 共{total_ms:.0f}ms")
    
    while True:
        try:
            # 从队列中获取数据，超时5秒
            try:
                item = queue.get(timeout=5)
            except Empty:
                # 队列为空，检查是否应该停止
                if stop_event.is_set():
                    logger.info("🛑 收到停止信号，处理剩余数据")
                    process_batch()
                    break
                continue
            
            if item is None:
                break

            # 处理控制消息
            if isinstance(item, dict) and item.get('__control__') == 'region_done':
                region = item['region']
                ok = item['ok']
                reason = item.get('reason', 'unknown')
                region_outcomes[region] = {'ok': ok, 'reason': reason}
                logger.info(f"📬 区域信号: {region} ok={ok} reason={reason}")
                continue

            listing = item
            house_id = listing.get('house_id')
            if not house_id:
                logger.warning("⚠️  缺少 house_id，跳过")
                continue
            
            web_price = listing.get('price', 0)
            web_update_time = listing.get('update_time', datetime.now().strftime('%Y-%m-%d'))
            region = listing.get('region', '')
            
            # 准备批量数据
            property_data = {
                'house_id': house_id,
                'title': listing['title'],
                'region': region,
                'biz_circle': listing.get('biz_circle', ''),
                'community': listing.get('community', ''),
                'community_id': listing.get('community_id'),
                'layout': listing.get('layout', ''),
                'area': listing.get('area', 0),
                'price': web_price,
                'unit_price': listing.get('unit_price', 0),
                'orientation': listing.get('orientation', ''),
                'decoration': listing.get('decoration', ''),
                'floor_info': listing.get('floor_info', ''),
                'building_type': listing.get('building_type', ''),
                'build_year': listing.get('build_year', None),
                'address_raw': listing.get('address_raw', ''),
                'last_update_date': web_update_time
            }
            property_batch.append(property_data)
            price_check_map[house_id] = (web_price, listing.get('unit_price', 0))

            # 达到批量大小，执行批量处理（含一次 DB 查询比对价格）
            if len(property_batch) >= batch_size:
                process_batch()
            
            # 2. 更新实时账本
            if region not in ledger:
                ledger[region] = {
                    'prices': [],
                    'unit_prices': [],
                    'areas': []
                }
            
            # 收集价格和面积信息用于计算均价
            if listing.get('unit_price') and listing.get('price') and listing.get('area'):
                ledger[region]['unit_prices'].append(listing['unit_price'])  # 单价列表
                ledger[region]['prices'].append(listing['price'])  # 总价列表
                ledger[region]['areas'].append(listing['area'])  # 面积列表
            
            processed_count += 1
            if processed_count % 1000 == 0:
                logger.info(f"📊 已处理 {processed_count} 条房源")
                
        except Empty:
            # 队列为空，检查是否需要停止
            logger.info("队列为空，检查是否需要停止")
            if stop_event.is_set():
                logger.info("队列为空且收到停止信号，退出消费者线程")
                break
            # 否则继续等待
            continue
        except Exception as e:
            import traceback
            logger.error(f"消费者处理数据失败: {e}")
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            time.sleep(5)  # 出现异常时等待5秒再重试
    
    # --- 核心：收工时统一结算区域表 ---
    if stop_event.is_set():
        logger.info("📊 开始结算区域价格指标")
        today = datetime.now().strftime('%Y-%m-%d')
        
        for region, data in ledger.items():
            try:
                unit_prices = data['unit_prices']
                prices = data['prices']
                areas = data['areas']
                
                # 计算并插入区域快照
                avg_unit_price, median_unit_price, weighted_avg_price = calc_district_stats(
                    data['unit_prices'], data['prices'], data['areas']
                )

                db_manager.insert_district_snapshot(
                    record_date=today,
                    region=region,
                    total_listings=len(prices),
                    avg_unit_price=avg_unit_price,
                    median_unit_price=median_unit_price,
                    weighted_avg_price=weighted_avg_price
                )
                
                logger.info(f"✅ {region} 区域快照已更新")
                
            except Exception as e:
                logger.error(f"结算 {region} 区域价格指标失败: {e}")
        
        # 区域结算：成功→下架，失败/无信号→恢复待确认
        logger.info("🔍 开始区域结算")
        for region in regions:
            outcome = region_outcomes.get(region)
            if outcome and outcome['ok']:
                try:
                    db_manager.mark_disappeared_properties(region=region)
                    logger.info(f"✅ {region}: 下架标记完成")
                except Exception as e:
                    logger.error(f"❌ {region}: 下架标记失败: {e}")
            else:
                reason = outcome.get('reason', 'no_signal') if outcome else 'no_signal'
                try:
                    db_manager.restore_pending_properties(region=region)
                    logger.info(f"↩️  {region}: 恢复待确认房源 (reason={reason})")
                except Exception as e:
                    logger.error(f"❌ {region}: 恢复待确认失败: {e}")
    
    total_time = time.time() - start_time
    logger.info(f"🏁 数据库写入线程完成，共处理 {processed_count} 条房源")
    logger.info(f"⏱️  数据库写入线程耗时: {total_time:.2f} 秒")
    if processed_count > 0:
        avg_time_per_item = total_time / processed_count * 1000
        logger.info(f"⚡ 平均每条房源处理时间: {avg_time_per_item:.2f} 毫秒")
        print(f'__STATS__{{"count": {processed_count}, "avg_ms": {avg_time_per_item:.1f}}}', flush=True)
    
    # 打印各区摘要信息
    logger.info("\n📊 各区数据摘要:")
    logger.info("=" * 60)
    for region, data in ledger.items():
        unit_prices = data['unit_prices']
        prices = data['prices']
        areas = data['areas']
        
        avg_unit_price, median_unit_price, weighted_avg_price = calc_district_stats(
            unit_prices, prices, areas
        )

        logger.info(f"🏠 {region}:")
        logger.info(f"  房源数量: {len(prices)} 条")
        logger.info(f"  平均单价: {avg_unit_price:.2f} 元/m²")
        logger.info(f"  中位数单价: {median_unit_price:.2f} 元/m²")
        logger.info(f"  资产平米价: {weighted_avg_price:.2f} 元/m²")
        if areas:
            logger.info(f"  平均面积: {sum(areas)/len(areas):.2f} 平米")
        if prices:
            logger.info(f"  平均总价: {sum(prices)/len(prices):.2f} 万元")
    logger.info("=" * 60)

def main():
    """主函数"""
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='房地产数据爬取流水线 (Playwright 版)')
    parser.add_argument('-r', '--region', type=int, nargs='+', help='选择要爬取的区域编号: 0=东城区, 1=西城区, 2=海淀区, 3=朝阳区, 4=丰台区, 5=石景山区')
    args = parser.parse_args()

    logger.info("🎯 启动房地产数据爬取流水线 (Playwright 版)")
    
    # 所有可用区域（按照用户指定的编号顺序）
    all_regions = [
        ('dongchengqu', '东城区'),
        ('xichengqu', '西城区'),
        ('haidianqu', '海淀区'),
        ('chaoyangqu', '朝阳区'),
        ('fengtaiqu', '丰台区'),
        ('shijingshanqu', '石景山区')
    ]
    
    window_size = SCRAPER_CONFIG['window_size']
    # 根据命令行参数选择区域
    selected_regions = []
    if args.region:
        # 验证区域编号
        for region_id in args.region:
            if 0 <= region_id < len(all_regions):
                selected_regions.append(all_regions[region_id])
            else:
                logger.warning(f"⚠️  无效的区域编号: {region_id}，将被忽略")
        
        if not selected_regions:
            logger.warning("⚠️  未选择有效区域，将爬取所有区域")
            selected_regions = all_regions
    else:
        # 默认爬取所有区域
        selected_regions = all_regions
    
    # 构建任务字典
    tasks = {code: name for code, name in selected_regions}
    
    # --- 资源调配看板 ---
    # 使用单个端口多个标签页的方式
    plan = [
        (CHROME_DEBUG_PORT, tasks)
    ]
    
    # 提取所有区域
    regions = [name for code, name in selected_regions]
    logger.info(f"📋 提取到区域列表: {regions}")
    
    # 1. 启动数据库写入线程
    consumer = Process(target=global_db_consumer, args=(shared_queue, stop_event, DB_CONFIG, regions))
    consumer.start()
    logger.info("✅ 数据库写入线程已启动")
    
    # 2. 启动 Playwright 工作线程
    # 直接运行 Playwright 工作线程，不使用 Process
    # 因为 Playwright 已经是异步的，不需要再用多进程
    for port, tasks in plan:
        logger.info(f"✅ 启动 Playwright 多标签页采集线程 (端口: {port})")
        try:
            asyncio.run(run_multi_tab_worker_playwright(port, tasks, shared_queue, window_size))
        except Exception as e:
            logger.error(f"❌ Playwright 工作线程失败: {e}")
        logger.info(f"✅ Playwright 多标签页采集线程已完成 (端口: {port})")
    
    # 3. 采集线程已经在主线程中执行完成，不需要等待
    logger.info("✅ 所有采集线程已完成")
    
    # 4. 下达收工指令，等待数据库处理完余货
    stop_event.set()
    consumer.join()
    logger.info("✅ 数据库写入线程已完成")

if __name__ == "__main__":
    main()
