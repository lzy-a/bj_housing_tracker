#!/usr/bin/env python3
"""
5i5j 租房爬虫调度脚本 — 镜像二手房爬虫，复用 CDP 多标签架构
"""
import asyncio
import logging
import time
from pathlib import Path
import sys
from multiprocessing import Process, Queue, Event
from queue import Empty

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import DB_CONFIG, SCRAPER_CONFIG, CHROME_DEBUG_PORT
from bs4 import BeautifulSoup
from scrapers.i5i5j_rent_scraper_playwright import I5I5JRentScraperPlaywright
from etl.db_manager import DatabaseManager
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

shared_queue = Queue(maxsize=10000)
stop_event = Event()


def calc_district_rent_stats(rent_prices, areas):
    """根据租金和面积列表计算区域租赁指标"""
    if not rent_prices:
        return 0, 0, 0
    avg_rent = sum(rent_prices) / len(rent_prices)

    sorted_r = sorted(rent_prices)
    n = len(sorted_r)
    median_rent = (sorted_r[n // 2 - 1] + sorted_r[n // 2]) / 2 if n % 2 == 0 else sorted_r[n // 2]

    unit_rents = [r / a for r, a in zip(rent_prices, areas) if a > 0]
    avg_unit_rent = sum(unit_rents) / len(unit_rents) if unit_rents else 0

    return avg_rent, median_rent, avg_unit_rent


async def process_page(page, region_code, region_name, page_num, shared_queue):
    """处理单个租房页面"""
    try:
        url = f"https://bj.5i5j.com/zufang/{region_code}/n{page_num}/"
        logger.info(f"🚀 [租房] {region_name} 第 {page_num} 页: {url}")

        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        html = await page.content()

        try:
            await page.wait_for_selector('ul.pList', timeout=5000)
            logger.info(f"✅ [租房] {region_name} 第 {page_num} 页已加载")
        except Exception as e:
            logger.warning(f"⚠️ [租房] {region_name} 第 {page_num} 页未找到房源列表: {e}")
            no_data_selectors = ['.n_no_data', '.no-result', '.empty-tip']
            for selector in no_data_selectors:
                no_data = await page.query_selector(selector)
                if no_data:
                    logger.info(f"🛑 [租房] {region_name} 确实没数据了")
                    return False, 0
            return True, 0

        soup = BeautifulSoup(html, 'lxml')
        page_data = I5I5JRentScraperPlaywright.extract_information(soup)
        logger.info(f"🔍 [租房] 找到 {len(page_data)} 个房源")

        if page_data:
            for house in page_data:
                house['region'] = region_name
                try:
                    shared_queue.put(house, block=True, timeout=10)
                except Exception as e:
                    logger.warning(f"⚠️ 队列已满，跳过 {house.get('house_id')}: {e}")
            return True, len(page_data)
        else:
            logger.info(f"⚠️ [租房] {region_name} 第 {page_num} 页无数据")
            return False, 0
    except Exception as e:
        logger.error(f"❌ [租房] 页面处理出错 {region_name} P{page_num}: {e}")
        return True, 0


async def run_multi_tab_worker_rent(debug_port, district_tasks, shared_queue, window_size=3):
    """租房多标签页爬取"""
    worker_start_time = time.time()
    logger.info(f"🚀 [租房] Playwright 多标签页采集启动")

    scraper = I5I5JRentScraperPlaywright(debug_port=debug_port)
    await scraper.connect()
    await scraper.check_and_login()

    try:
        for dist_code, dist_name in district_tasks.items():
            logger.info(f"\n{'=' * 80}")
            logger.info(f"🔄 [租房] 开始爬取: {dist_name}")

            current_page = 1
            max_page = 2000
            restart_interval = 400
            is_region_finished = False
            total_listings = 0

            while current_page <= max_page and not is_region_finished:
                end_page = min(current_page + restart_interval - 1, max_page)

                scraper = I5I5JRentScraperPlaywright(debug_port=debug_port)
                await scraper.connect()
                await scraper.create_pages(window_size)

                page_queue = asyncio.Queue()
                batch_no_data_count = 0
                max_no_data_pages = 3

                for page_num in range(current_page, end_page + 1):
                    await page_queue.put(page_num)

                async def task_worker(page):
                    nonlocal is_region_finished, total_listings, batch_no_data_count, current_page
                    while not is_region_finished:
                        try:
                            page_num = await asyncio.wait_for(page_queue.get(), timeout=5)
                            success, listings_count = await process_page(page, dist_code, dist_name, page_num, shared_queue)
                            total_listings += listings_count
                            current_page = max(current_page, page_num + 1)
                            page_queue.task_done()

                            if not success:
                                batch_no_data_count += 1
                                if batch_no_data_count >= max_no_data_pages:
                                    logger.info(f"🛑 [租房] {dist_name} 连续 {max_no_data_pages} 页无数据")
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
                        except asyncio.TimeoutError:
                            break

                tasks = [task_worker(p) for p in scraper.pages]
                await asyncio.gather(*tasks)
                await scraper.close()

            logger.info(f"📈 [租房] {dist_name} 完成: {total_listings} 条")

        logger.info(f"✅ [租房] 采集完成，总耗时 {time.time() - worker_start_time:.0f}s")
    except Exception as e:
        logger.error(f"❌ [租房] 采集失败: {e}")


# 全局数据库写入消费者
def global_db_consumer_rent(queue, stop_event, db_config, regions):
    """租房数据库写入消费者"""
    start_time = time.time()
    logger.info("🚗 [租房] 数据库写入线程启动")
    db_manager = DatabaseManager(db_config)
    processed_count = 0

    ledger = {}
    batch_size = 500
    property_batch = []
    price_batch = []

    for region in regions:
        try:
            db_manager.conn = db_manager._get_connection()
            cursor = db_manager.conn.cursor()
            cursor.execute('UPDATE rental_details SET status = 2 WHERE region = %s', (region,))
            db_manager.conn.commit()
            cursor.close()
            db_manager._return_connection(db_manager.conn)
            logger.info(f"✅ [租房] {region} 状态重置为待确认")
        except Exception as e:
            logger.error(f"❌ [租房] 更新 {region} 状态失败: {e}")

    def process_batch():
        nonlocal property_batch, price_batch
        if property_batch:
            db_manager.batch_insert_rental_details(property_batch)
            property_batch = []
        if price_batch:
            db_manager.batch_insert_rent_history(price_batch)
            price_batch = []

    while True:
        try:
            try:
                item = queue.get(timeout=5)
            except Empty:
                if stop_event.is_set():
                    process_batch()
                    break
                continue

            if item is None:
                break

            listing = item
            house_id = listing.get('house_id')
            if not house_id:
                continue

            rent_price = listing.get('rent_price', 0)
            region = listing.get('region', '')

            property_batch.append({
                'house_id': house_id,
                'title': listing['title'],
                'region': region,
                'biz_circle': listing.get('biz_circle', ''),
                'community': listing.get('community', ''),
                'community_id': listing.get('community_id'),
                'layout': listing.get('layout', ''),
                'area': listing.get('area', 0),
                'rent_price': rent_price,
                'rent_type': listing.get('rent_type', '整租'),
                'orientation': listing.get('orientation', ''),
                'decoration': listing.get('decoration', ''),
                'floor_info': listing.get('floor_info', ''),
            })

            # 新房源 → 插入租金历史
            conn = db_manager._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT house_id FROM rental_details WHERE house_id = %s', (house_id,))
            exists = cursor.fetchone()
            cursor.close()
            db_manager._return_connection(conn)

            if not exists:
                price_batch.append({
                    'house_id': house_id,
                    'rent_price': rent_price,
                    'record_date': datetime.now().strftime('%Y-%m-%d')
                })
            else:
                # 检查租金是否变化
                conn = db_manager._get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT rent_price FROM rent_history WHERE house_id = %s ORDER BY record_date DESC LIMIT 1', (house_id,))
                result = cursor.fetchone()
                cursor.close()
                db_manager._return_connection(conn)
                if result and float(rent_price) != float(result[0]):
                    price_batch.append({
                        'house_id': house_id,
                        'rent_price': rent_price,
                        'record_date': datetime.now().strftime('%Y-%m-%d')
                    })

            if len(property_batch) >= batch_size:
                process_batch()

            # 实时账本
            if region not in ledger:
                ledger[region] = {'rent_prices': [], 'areas': []}
            if rent_price > 0 and listing.get('area', 0) > 0:
                ledger[region]['rent_prices'].append(rent_price)
                ledger[region]['areas'].append(listing['area'])

            processed_count += 1
            if processed_count % 100 == 0:
                logger.info(f"📊 [租房] 已处理 {processed_count} 条")

        except Exception as e:
            logger.error(f"❌ [租房] 消费者处理失败: {e}")
            time.sleep(5)

    # 结算区域快照
    if stop_event.is_set():
        logger.info("📊 [租房] 开始结算区域租赁指标")
        today = datetime.now().strftime('%Y-%m-%d')
        for region, data in ledger.items():
            try:
                avg_rent, median_rent, avg_unit_rent = calc_district_rent_stats(
                    data['rent_prices'], data['areas']
                )
                db_manager.insert_district_rent_snapshot(
                    record_date=today, region=region,
                    total_rentals=len(data['rent_prices']),
                    avg_rent_price=avg_rent, median_rent_price=median_rent,
                    avg_unit_rent=avg_unit_rent
                )
                logger.info(f"✅ [租房] {region} 区域租赁快照已更新")
            except Exception as e:
                logger.error(f"❌ [租房] 结算 {region} 失败: {e}")

        # 标记消失的租房房源
        for region in regions:
            try:
                conn = db_manager._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE rental_details SET status = 0 WHERE region = %s AND status = 2', (region,))
                conn.commit()
                cursor.close()
                db_manager._return_connection(conn)
            except Exception as e:
                logger.error(f"❌ [租房] 标记 {region} 消失房源失败: {e}")

        # 计算社区租售联动指标
        try:
            db_manager.compute_community_metrics(today)
            logger.info(f"✅ [租售联动] 社区指标计算完成")
        except Exception as e:
            logger.error(f"❌ [租售联动] 计算失败: {e}")

    logger.info(f"🏁 [租房] 数据库写入完成，共 {processed_count} 条")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='5i5j 租房爬虫 (Playwright)')
    parser.add_argument('-r', '--region', type=int, nargs='+',
                        help='区域编号: 0=东城 1=西城 2=海淀 3=朝阳 4=丰台 5=石景山')
    args = parser.parse_args()

    logger.info("🎯 [租房] 启动爬取流水线")

    all_regions = [
        ('dongchengqu', '东城区'), ('xichengqu', '西城区'),
        ('haidianqu', '海淀区'), ('chaoyangqu', '朝阳区'),
        ('fengtaiqu', '丰台区'), ('shijingshanqu', '石景山区')
    ]

    selected_regions = []
    if args.region:
        for rid in args.region:
            if 0 <= rid < len(all_regions):
                selected_regions.append(all_regions[rid])
        if not selected_regions:
            selected_regions = all_regions
    else:
        selected_regions = all_regions

    tasks = {code: name for code, name in selected_regions}
    regions = [name for _, name in selected_regions]
    window_size = SCRAPER_CONFIG['window_size']

    # 启动 DB 消费者
    consumer = Process(target=global_db_consumer_rent, args=(shared_queue, stop_event, DB_CONFIG, regions))
    consumer.start()

    # 启动爬虫
    asyncio.run(run_multi_tab_worker_rent(CHROME_DEBUG_PORT, tasks, shared_queue, window_size))

    # 收工
    stop_event.set()
    consumer.join()
    logger.info("✅ [租房] 全部完成")


if __name__ == "__main__":
    main()
