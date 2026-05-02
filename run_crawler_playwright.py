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
    """处理单个页面"""
    try:
        # 随机微调，错开并发波峰
        import random
        # await asyncio.sleep(random.uniform(0.25, 0.5))
        
        url = f"https://bj.5i5j.com/ershoufang/{region_code}/n{page_num}/"
        logger.info(f"🚀 开始加载 {region_name} 第 {page_num} 页: {url}")
        
        # 1. 导航到页面，只等待 DOM 加载完成
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        
        # 3. 获取页面内容
        html = await page.content()
        
        # 5. 检查是否有房源列表
        try:
            # 等待房源列表出现
            await page.wait_for_selector('ul.pList', timeout=5000)
            logger.info(f"✅ {region_name} 第 {page_num} 页房源列表已加载")
        except Exception as e:
            # 如果等不到，检查是不是真的“无数据”空提示
            logger.warning(f"⚠️ {region_name} 第 {page_num} 页未找到房源列表: {e}")
            
            # 检查是否有反爬提示
            anti_crawl = await page.query_selector('.anti-crawl')
            if anti_crawl:
                logger.warning(f"⚠️ {region_name} 第 {page_num} 页可能被反爬")
                return True, 0  # 反爬不是真正的无数据，继续尝试
            
            # 检查是否有登录提示
            login_prompt = await page.query_selector('.login-prompt')
            if login_prompt:
                logger.warning(f"⚠️ {region_name} 第 {page_num} 页需要登录")
                return True, 0  # 登录不是真正的无数据，继续尝试
            
            # 检查是否有其他无数据提示
            no_data_selectors = ['.n_no_data', '.no-result', '.empty-tip']
            for selector in no_data_selectors:
                no_data = await page.query_selector(selector)
                if no_data:
                    logger.info(f"🛑 {region_name} 确实没数据了")
                    return False, 0  # 真正的无数据，返回 False
            
            logger.warning(f"⚠️ {region_name} 第 {page_num} 页加载异常")
            return True, 0  # 加载异常不是真正的无数据，继续尝试

        # 6. 提取数据
        soup = BeautifulSoup(html, 'lxml')
        
        # 7. 提取房源信息
        page_data = []
        try:
            # 直接使用类的 extract_information 方法，避免实例化
            page_data = I5I5JScraperPlaywright.extract_information(soup)
            logger.info(f"🔍 找到 {len(page_data)} 个房源")
        except Exception as e:
            logger.error(f"❌ 提取数据失败: {e}")
        
        if page_data:
            for house in page_data:
                house['region'] = region_name
                # 直接在协程里使用同步的put，捕获队列满的异常
                try:
                    shared_queue.put(house, block=True, timeout=10)  # 10秒超时
                except Exception as e:
                    logger.warning(f"⚠️ 队列已满，跳过房源 {house.get('house_id', 'unknown')}: {e}")
                    continue
            logger.info(f"✅ 完成 {region_name} 第 {page_num} 页，获取 {len(page_data)} 条房源")
            return True, len(page_data)
        else:
            # 没有数据，返回False，让连续无数据计数增加
            logger.info(f"⚠️ {region_name} 第 {page_num} 页无数据")
            return False, 0
    except Exception as e:
        logger.error(f"❌ 页面处理出错 {region_name} P{page_num}: {e}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        return True, 0  # 网络错误或其他异常，继续尝试下一页

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
    
    try:
        for dist_code, dist_name in district_tasks.items():
            # 为每个区域重新初始化爬虫，避免资源累积
            logger.info(f"\n{'=' * 80}")
            logger.info(f"🔄 为 {dist_name} 初始化新的爬虫实例")
            logger.info(f"{'=' * 80}")
            
            current_page = 1
            max_page = 2000  # 增加最大页码，确保能爬取所有数据
            restart_interval = 400  # 每批次处理的页面数
            is_region_finished = False
            total_listings = 0
            region_start_time = time.time()
            logger.info(f"🚀 开始并发扫描: {dist_name}")
            
            while current_page <= max_page and not is_region_finished:
                # 计算本次要爬取的页面范围
                end_page = min(current_page + restart_interval - 1, max_page)
                logger.info(f"\n{'=' * 80}")
                logger.info(f"🔄 为 {dist_name} 初始化新的爬虫实例，处理页面 {current_page}-{end_page}")
                logger.info(f"{'=' * 80}")
                
                # 重新初始化爬虫
                scraper = I5I5JScraperPlaywright(debug_port=debug_port)
                await scraper.connect()
                
                # 创建爬取页面
                await scraper.create_pages(window_size)
                
                # 使用任务队列来管理页面，避免并发问题
                page_queue = asyncio.Queue()
                batch_no_data_count = 0  # 当前批次的连续无数据页面计数
                max_no_data_pages = 3  # 连续无数据页面阈值
                
                # 预填充本次要爬取的页面队列
                for page_num in range(current_page, end_page + 1):
                    await page_queue.put(page_num)
                
                # 异步任务池模式：始终保持有 window_size 个协程在跑
                async def task_worker(page):
                    """单个页面的工作器"""
                    nonlocal is_region_finished, total_listings, batch_no_data_count, current_page
                    while not is_region_finished:
                        try:
                            # 从队列中获取页面，设置超时
                            page_num = await asyncio.wait_for(page_queue.get(), timeout=5)
                            logger.info(f"📋 {dist_name} 任务分配: 开始处理第 {page_num} 页")
                            
                            # 处理页面
                            success, listings_count = await process_page(page, dist_code, dist_name, page_num, shared_queue)
                            total_listings += listings_count
                            
                            # 更新当前页码
                            current_page = max(current_page, page_num + 1)
                            
                            # 标记任务完成
                            page_queue.task_done()
                            
                            # 只有明确的"无数据"情况才停止爬取
                            if not success:
                                batch_no_data_count += 1
                                logger.info(f"📊 {dist_name} 第 {page_num} 页无数据，连续无数据页面数: {batch_no_data_count}")
                                if batch_no_data_count >= max_no_data_pages:
                                    logger.info(f"🛑 {dist_name} 连续 {max_no_data_pages} 页无数据，停止爬取")
                                    is_region_finished = True
                                    logger.info(f"🚫 已设置 {dist_name} 爬取结束标志")
                                    # 清空队列，让其他worker尽快结束
                                    while not page_queue.empty():
                                        try:
                                            page_queue.get_nowait()
                                            page_queue.task_done()
                                        except asyncio.QueueEmpty:
                                            break
                                    break
                            else:
                                # 有数据，重置无数据计数
                                batch_no_data_count = 0
                                logger.info(f"✅ {dist_name} 第 {page_num} 页处理完成，继续下一页")
                        except asyncio.TimeoutError:
                            # 队列已空，退出
                            logger.info(f"⏰ {dist_name} 页面队列为空，worker 退出")
                            break
                        except Exception as e:
                            logger.error(f"❌ {dist_name} 处理页面时出错: {e}")
                            # 标记任务完成，避免队列阻塞
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
    
    # 初始状态更新：将所有区域的房源状态设为待确认
    logger.info("📋 开始更新所有区域的房源状态为待确认")
    for region in regions:
        try:
            db_manager.update_property_status(region=region, status=2)
            logger.info(f"✅ {region} 区域状态更新完成")
        except Exception as e:
            logger.error(f"❌ 更新 {region} 区域状态失败: {e}")
    
    # 队列深度监控
    last_queue_check_time = time.time()
    queue_check_interval = 10  # 每10秒检查一次队列深度
    
    def process_batch():
        """处理批量数据"""
        nonlocal property_batch, price_batch
        
        if property_batch:
            logger.info(f"📊 批量处理 {len(property_batch)} 条房源数据")
            db_manager.batch_insert_property_details(property_batch)
            property_batch = []
        
        if price_batch:
            logger.info(f"📊 批量处理 {len(price_batch)} 条价格历史数据")
            db_manager.batch_insert_price_history(price_batch)
            price_batch = []
    
    while True:
        try:
            # 检查队列深度
            current_time = time.time()
            if current_time - last_queue_check_time >= queue_check_interval:
                # 使用 empty() 方法替代 qsize()，避免 NotImplementedError
                if queue.empty():
                    logger.info("📊 队列深度: 0 (队列为空)")
                    # 队列为空时处理剩余数据
                    process_batch()
                else:
                    logger.info("📊 队列深度: 非空")
                last_queue_check_time = current_time
            
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
            
            # 检查是否需要插入价格历史
            exists = db_manager.get_property(house_id)
            if not exists:
                # 新房源，插入价格历史
                price_data = {
                    'house_id': house_id,
                    'price': web_price,
                    'unit_price': listing.get('unit_price', 0),
                    'record_date': datetime.now().strftime('%Y-%m-%d')
                }
                price_batch.append(price_data)
            else:
                # 老房源，检查价格是否变化
                last_price = db_manager.get_latest_price(house_id)
                if float(web_price) != float(last_price):
                    # 价格变了，插入价格历史
                    price_data = {
                        'house_id': house_id,
                        'price': web_price,
                        'unit_price': listing.get('unit_price', 0),
                        'record_date': datetime.now().strftime('%Y-%m-%d')
                    }
                    price_batch.append(price_data)
            
            # 达到批量大小，执行批量处理
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
            if processed_count % 100 == 0:
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