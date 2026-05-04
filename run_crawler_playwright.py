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
from etl.db_manager import DatabaseManager
from datetime import datetime

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



async def process_page(page, region_code, region_name, page_num, shared_queue):
    """处理单个页面，返回 (success, listing_count, elapsed_ms)"""
    try:
        t0 = time.perf_counter()

        url = f"https://bj.5i5j.com/ershoufang/{region_code}/n{page_num}/"

        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        html = await page.content()

        try:
            await page.wait_for_selector('ul.pList', timeout=5000)
        except Exception:
            if await page.query_selector('.anti-crawl'):
                logger.warning(f"反爬 {region_name} P{page_num}")
                return True, 0, (time.perf_counter() - t0) * 1000
            if await page.query_selector('.login-prompt'):
                logger.warning(f"需登录 {region_name} P{page_num}")
                return True, 0, (time.perf_counter() - t0) * 1000
            for sel in ['.n_no_data', '.no-result', '.empty-tip']:
                if await page.query_selector(sel):
                    return False, 0, (time.perf_counter() - t0) * 1000
            return True, 0, (time.perf_counter() - t0) * 1000

        soup = BeautifulSoup(html, 'lxml')
        try:
            page_data = I5I5JScraperPlaywright.extract_information(soup)
        except Exception as e:
            logger.error(f"提取失败 {region_name} P{page_num}: {e}")
            return True, 0, (time.perf_counter() - t0) * 1000

        if page_data:
            for house in page_data:
                house['region'] = region_name
                try:
                    shared_queue.put(house, block=True, timeout=10)
                except Exception:
                    logger.warning(f"队列满 {house.get('house_id', 'unknown')}")
            return True, len(page_data), (time.perf_counter() - t0) * 1000
        else:
            return False, 0, (time.perf_counter() - t0) * 1000
    except Exception as e:
        logger.error(f"页面异常 {region_name} P{page_num}: {e}")
        return True, 0, (time.perf_counter() - t0) * 1000

async def run_multi_tab_worker_playwright(debug_port, district_tasks, shared_queue, window_size=3):
    """
    使用 Playwright 的多标签页并行爬取工作线程
    """
    worker_start_time = time.time()
    logger.info(f"🚀 Playwright 多标签页采集线程启动 (端口: {debug_port})，任务: {district_tasks}")
    
    # 初始化爬虫并检查登录状态
    scraper = I5I5JScraperPlaywright(debug_port=debug_port)
    await scraper.connect()
    
    # 检查登录状态（在最前面检查）
    logger.info("🔐 开始检查登录状态")
    await scraper.check_and_login()
    logger.info("✅ 登录检查完成")
    await scraper.close()

    try:
        for dist_code, dist_name in district_tasks.items():
            current_page = 1
            max_page = 2000
            restart_interval = SCRAPER_CONFIG['restart_interval']
            is_region_finished = False
            total_listings = 0
            region_start_time = time.time()
            logger.info(f"🚀 {dist_name}: 开始扫描")

            while current_page <= max_page and not is_region_finished:
                end_page = min(current_page + restart_interval - 1, max_page)

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
                    nonlocal pages_processed, last_progress_log, total_page_time
                    while not is_region_finished:
                        try:
                            page_num = await asyncio.wait_for(page_queue.get(), timeout=5)
                            success, listings_count, page_ms = await process_page(page, dist_code, dist_name, page_num, shared_queue)
                            total_listings += listings_count
                            pages_processed += 1
                            total_page_time += page_ms
                            current_page = max(current_page, page_num + 1)
                            page_queue.task_done()

                            if not success:
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
                            else:
                                batch_no_data_count = 0
                                if pages_processed - last_progress_log >= progress_interval:
                                    avg_s = total_page_time / pages_processed / 1000
                                    logger.info(f"📖 {dist_name}: {pages_processed}页 {total_listings}条 均{avg_s:.1f}s/页")
                                    last_progress_log = pages_processed
                        except asyncio.TimeoutError:
                            break
                        except Exception as e:
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
            
            logger.info(f"✅ {dist_name} 资源释放完成")
        
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
        
        # 标记消失的房源
        logger.info("🔍 开始标记消失的房源")
        for region in regions:
            try:
                db_manager.mark_disappeared_properties(region=region)
                logger.info(f"✅ {region} 区域消失房源标记完成")
            except Exception as e:
                logger.error(f"❌ 标记 {region} 区域消失房源失败: {e}")
    
    total_time = time.time() - start_time
    logger.info(f"🏁 数据库写入线程完成，共处理 {processed_count} 条房源")
    logger.info(f"⏱️  数据库写入线程耗时: {total_time:.2f} 秒")
    if processed_count > 0:
        avg_time_per_item = total_time / processed_count * 1000
        logger.info(f"⚡ 平均每条房源处理时间: {avg_time_per_item:.2f} 毫秒")
        print(f'__STATS__{{"count": {processed_count}, "avg_ms": {avg_time_per_item:.1f}}}')
    
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