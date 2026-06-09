#!/usr/bin/env python3
"""
租房找房器 CLI — 完全独立运行，不依赖主爬虫。
流程：爬小区列表页 → 筛选 → 爬详情页 → 下照片 → AI 评分 → 通知

独立运行：python finder/run_finder.py
重新评分：python finder/run_finder.py --rescore
"""
import asyncio
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import CHROME_DEBUG_PORT, FINDER_CONFIG
from etl.db_manager import DatabaseManager
from finder.detail_scraper import RentalDetailScraper
from finder.image_manager import ImageManager
from finder.scorer import RentalScorer
from finder.notifier import EmailNotifier

STATUS_FILE = Path(__file__).parent.parent / 'data' / 'finder_status.json'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
CHROME_USER_DIR = "/tmp/chrome_9223"


def write_status(phase, message, progress=0, total=0, detail=''):
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    status = {
        'phase': phase, 'message': message,
        'progress': progress, 'total': total,
        'detail': detail, 'timestamp': datetime.now().isoformat(),
        'running': phase not in ('done', 'error'),
    }
    STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False), encoding='utf-8')


def ensure_chrome_with_images():
    try:
        subprocess.run(["pkill", "-f", f"remote-debugging-port={CHROME_DEBUG_PORT}"],
                        timeout=5, capture_output=True)
        time.sleep(1)
    except Exception:
        pass

    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={CHROME_DEBUG_PORT}",
        f"--user-data-dir={CHROME_USER_DIR}",
        "--no-first-run", "--no-default-browser-check",
        "--disable-session-crashed-bubble", "--disable-infobars",
        "--disable-features=ChromeWhatsNewUI",
    ]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)

    import requests
    for _ in range(10):
        try:
            r = requests.get(f"http://localhost:{CHROME_DEBUG_PORT}/json/version", timeout=3)
            if r.ok:
                logger.info("Chrome 已启动（图片模式）")
                return
        except Exception:
            pass
        time.sleep(1)
    logger.error("Chrome 启动失败")
    write_status('error', 'Chrome 启动失败')
    sys.exit(1)


def shutdown_chrome():
    try:
        subprocess.run(["pkill", "-f", f"remote-debugging-port={CHROME_DEBUG_PORT}"],
                        timeout=5, capture_output=True)
        logger.info("Chrome 已关闭")
    except Exception:
        pass


def filter_high_score(scores: dict) -> bool:
    threshold = FINDER_CONFIG.get('alert_score_threshold', 7)
    overall = scores.get('推荐指数', 0)
    if overall < threshold:
        return False
    return any(v >= threshold for k, v in scores.items() if k != '推荐指数')


def apply_filters(listings: list, filters: dict) -> list:
    """按筛选条件过滤房源（URL 已处理价格和户型，本地只过滤租型）"""
    if not filters.get('rent_type'):
        return listings
    return [l for l in listings if l.get('rent_type') == filters['rent_type']]


async def run():
    db = DatabaseManager()
    image_mgr = ImageManager()
    scorer = RentalScorer()
    notifier = EmailNotifier()

    # 1. 检查收藏小区
    write_status('init', '检查收藏小区...')
    watchlist = db.get_watchlist(active_only=True)
    if not watchlist:
        msg = '收藏小区为空，请先在前端添加收藏'
        logger.info(msg)
        write_status('done', msg)
        return

    logger.info(f"收藏小区: {len(watchlist)} 个")
    for w in watchlist:
        logger.info(f"  {w['community']} (id={w.get('community_id')}) filter={w.get('filter_criteria',{})}")

    # 2. 启动 Chrome
    write_status('chrome', '启动 Chrome...')
    ensure_chrome_with_images()

    try:
        # 3. 登录
        write_status('login', '登录 5i5j...')
        scraper = RentalDetailScraper()
        await scraper.connect()
        await scraper.check_and_login()

        # 3.5 计算通勤时间（只算收藏小区中未缓存的）
        from finder.commute import batch_calc_commute, DEST_NAME
        watched_comms = [{'community': w['community'], 'community_id': w.get('community_id', ''),
                          'region': w.get('region', ''), 'biz_circle': w.get('biz_circle', '')}
                         for w in watchlist]
        uncached = [c for c in watched_comms if not db.get_commute(c['community'], DEST_NAME)]
        if uncached:
            write_status('commute', f'计算 {len(uncached)} 个小区通勤时间...')
            batch_calc_commute(db, uncached)

        # 4. 阶段一：爬所有小区列表页，收集待评分房源
        all_scored = []
        today_str = date.today().isoformat()
        all_to_score = []  # [listing, ...]

        for wi, w in enumerate(watchlist):
            comm = w['community']
            cid = w.get('community_id')
            filters = w.get('filter_criteria', {})

            if not cid:
                logger.warning(f"  {comm}: 无 community_id，跳过")
                continue

            # 用筛选条件爬取（减少请求量）
            write_status('listing', f'爬取小区列表', wi + 1, len(watchlist), comm)
            logger.info(f"[{wi+1}/{len(watchlist)}] 爬取 {comm} (id={cid})")
            all_listings = await scraper.scrape_community(cid, filters=filters)
            logger.info(f"  列表返回: {len(all_listings)} 套")

            if not all_listings:
                continue

            for l in all_listings:
                l['region'] = w.get('region', '')

            # 只保留当前小区的房源（5i5j 页面会返回同板块其他小区的）
            same_comm = [l for l in all_listings if l.get('community') == comm]
            logger.info(f"  同小区: {len(same_comm)} 套（去掉其他小区 {len(all_listings) - len(same_comm)} 套）")

            # 标记下架：当前小区列表里没有的 → status=0
            found_ids = {l['house_id'] for l in same_comm}
            conn = db._get_connection()
            c = conn.cursor()
            c.execute("""
                UPDATE rental_details SET status = 0, updated_at = CURRENT_TIMESTAMP
                WHERE community = %s AND status = 1
                AND house_id IN (SELECT house_id FROM rental_scores)
                AND house_id != ALL(%s)
            """, (comm, list(found_ids) if found_ids else ['__none__']))
            delisted = c.rowcount
            conn.commit()
            c.close()
            db._return_connection(conn)
            if delisted:
                logger.info(f"  标记下架: {delisted} 套")

            # 应用筛选
            filtered = apply_filters(same_comm, filters)
            logger.info(f"  筛选后: {len(filtered)} 套")

            if not filtered:
                continue

            # 写入 DB
            db.batch_insert_rental_details([{
                'house_id': l['house_id'], 'title': l['title'],
                'region': l.get('region', ''), 'biz_circle': l.get('biz_circle', ''),
                'community': l.get('community', ''), 'community_id': l.get('community_id', ''),
                'layout': l.get('layout', ''), 'area': l.get('area'),
                'rent_price': l.get('rent_price'), 'rent_type': l.get('rent_type', ''),
                'orientation': l.get('orientation', ''), 'decoration': l.get('decoration', ''),
                'floor_info': l.get('floor_info', ''), 'update_time': today_str,
            } for l in filtered])

            # 过滤：已有有效评分的跳过，无评分或全0的要评
            for l in filtered:
                conn = db._get_connection()
                c = conn.cursor()
                c.execute("""
                    SELECT scores FROM rental_scores
                    WHERE house_id=%s ORDER BY created_at DESC LIMIT 1
                """, (l['house_id'],))
                row = c.fetchone()
                c.close()
                db._return_connection(conn)

                if row:
                    scores = row[0]
                    has_valid = isinstance(scores, dict) and any(v > 0 for v in scores.values())
                    if has_valid:
                        continue

                all_to_score.append(l)

        logger.info(f"总计待评分: {len(all_to_score)} 套")
        if not all_to_score:
            await scraper.close()
            write_status('done', '没有需要评分的房源')
            return

        # 5. 阶段二：统一爬详情页 + 下载 + 评分（全局进度）
        grand_total = len(all_to_score)
        concurrency = FINDER_CONFIG.get('concurrency', 3)
        sem = asyncio.Semaphore(concurrency)
        counter = {'done': 0, 'scored': 0}

        async def _process_one(listing):
            async with sem:
                hid = listing['house_id']
                comm = listing.get('community', '')
                label = f"{comm} {listing.get('layout', '')}"
                counter['done'] += 1
                n = counter['done']

                # 检查是否已有下载好的照片
                existing_photos = db.get_photos_for_house(hid)
                downloaded = [p for p in existing_photos if p.get('downloaded') and p.get('local_path')]

                if downloaded:
                    # 已有照片，直接用
                    data_dir = Path(__file__).parent.parent / 'data'
                    local_paths = [str(data_dir / p['local_path']) for p in downloaded]
                    write_status('scoring', f'[{comm}] AI 评分（已有照片）', n, grand_total, label)
                else:
                    # 没有照片，爬详情页 + 下载
                    write_status('scraping', f'[{comm}] 爬详情页', n, grand_total, label)
                    detail = await scraper.scrape_detail(hid)
                    photos = detail.get('photos', [])

                    local_paths = []
                    if photos:
                        write_status('downloading', f'[{comm}] 下载 {len(photos)} 张照片', n, grand_total, label)
                        local_paths = image_mgr.download_photos(db, hid, photos)

                    write_status('scoring', f'[{comm}] AI 评分', n, grand_total, label)

                # 查市场均价 + 通勤数据
                layout_prefix = listing.get('layout', '')[:2] if listing.get('layout') else None
                market = db.get_market_data(comm, layout_prefix)
                commute = db.get_commute(comm)

                write_status('scoring', f'[{comm}] AI 评分', n, grand_total, label)
                result = scorer.score_listing(listing, local_paths,
                                              market_data=market, commute_data=commute)

                db.insert_rental_score(hid, result['scores'],
                                       result.get('summary', ''), scorer.model,
                                       raw_input=result.get('raw_input', ''),
                                       raw_output=result.get('raw_output', ''))
                db.mark_detail_scraped(hid)

                scores_str = ' '.join(f'{k}:{v}' for k, v in result['scores'].items())
                if filter_high_score(result['scores']):
                    counter['scored'] += 1
                    logger.info(f"  [{n}/{grand_total}] ★ {hid} ({label}): {scores_str}")
                    write_status('scoring', f'[{comm}] ★ 高分 {counter["scored"]} 套', n, grand_total, label)
                    return {**listing, **result}
                else:
                    logger.info(f"  [{n}/{grand_total}] {hid} ({label}): {scores_str}")
                return None

        tasks = [_process_one(l) for l in all_to_score]
        results = await asyncio.gather(*tasks)
        all_scored = [r for r in results if r]

        await scraper.close()

        await scraper.close()

        # 5. 邮件通知
        if all_scored and notifier.is_configured():
            write_status('notify', f'发送邮件通知 ({len(all_scored)} 套高分)...')
            if notifier.send_alert(all_scored):
                db.mark_notified([s['house_id'] for s in all_scored])

        msg = f'完成: {len(all_scored)} 套高分房源'
        logger.info(msg)
        write_status('done', msg)

    except Exception as e:
        logger.error(f"扫描异常: {e}", exc_info=True)
        write_status('error', f'扫描异常: {e}')
    finally:
        shutdown_chrome()


async def run_rescore():
    """重新评分：用已有照片重新打分，不爬详情页。"""
    db = DatabaseManager()
    scorer = RentalScorer()
    notifier = EmailNotifier()

    write_status('init', '重新评分模式...')

    conn = db._get_connection()
    c = conn.cursor()
    c.execute('DELETE FROM rental_scores WHERE score_date = CURRENT_DATE')
    logger.info(f"已清除 {c.rowcount} 条旧评分")
    conn.commit()
    c.close()
    db._return_connection(conn)

    watchlist = db.get_watchlist(active_only=True)
    if not watchlist:
        write_status('done', '收藏小区为空')
        return

    # 收集有照片的房源
    to_score = []
    for w in watchlist:
        conn = db._get_connection()
        c = conn.cursor()
        from psycopg2 import extras
        c.execute = extras.RealDictCursor
        c2 = conn.cursor(cursor_factory=extras.RealDictCursor)
        c2.execute('''
            SELECT r.house_id, r.title, r.region, r.biz_circle, r.community,
                   r.layout, r.area, r.rent_price, r.rent_type, r.orientation,
                   r.decoration, r.floor_info, r.community_id
            FROM rental_details r
            WHERE r.community = %s AND r.status = 1
        ''', (w['community'],))
        listings = c2.fetchall()
        c2.close()
        c.close()
        db._return_connection(conn)

        for l in listings:
            photos = db.get_photos_for_house(l['house_id'])
            downloaded = [p for p in photos if p.get('downloaded') and p.get('local_path')]
            if downloaded:
                data_dir = Path(__file__).parent.parent / 'data'
                to_score.append((dict(l), [str(data_dir / p['local_path']) for p in downloaded]))

    if not to_score:
        write_status('done', '没有已下载照片的房源')
        return

    total = len(to_score)
    logger.info(f"重新评分: {total} 套")
    scored_listings = []

    for i, (listing_info, image_paths) in enumerate(to_score):
        hid = listing_info['house_id']
        comm = listing_info.get('community', '')
        label = f"{comm} {listing_info.get('layout', '')}"
        write_status('scoring', f'AI 评分', i + 1, total, label)

        layout_prefix = listing_info.get('layout', '')[:2] if listing_info.get('layout') else None
        market = db.get_market_data(comm, layout_prefix)
        commute = db.get_commute(comm)
        result = scorer.score_listing(listing_info, image_paths,
                                      market_data=market, commute_data=commute)
        db.insert_rental_score(hid, result['scores'], result.get('summary', ''), scorer.model,
                               raw_input=result.get('raw_input', ''),
                               raw_output=result.get('raw_output', ''))

        logger.info(f"  {hid} ({label}): {result['scores']}")
        if filter_high_score(result['scores']):
            scored_listings.append({**listing_info, **result})

    if scored_listings and notifier.is_configured():
        write_status('notify', f'发送邮件通知 ({len(scored_listings)} 套高分)...')
        if notifier.send_alert(scored_listings):
            db.mark_notified([s['house_id'] for s in scored_listings])

    msg = f'完成: {total} 套已评分, {len(scored_listings)} 套高分'
    logger.info(msg)
    write_status('done', msg, total, total)


async def run_no_scrape():
    """跳过列表页爬取，直接对 DB 中已有房源评分。"""
    db = DatabaseManager()
    image_mgr = ImageManager()
    scorer = RentalScorer()
    notifier = EmailNotifier()

    write_status('init', '跳过列表页，直接评分...')

    # 1. 查未评分的房源（收藏小区中，status=1，没有有效评分）
    watchlist = db.get_watchlist(active_only=True)
    if not watchlist:
        write_status('done', '收藏小区为空')
        return

    listings = db.get_unscored_in_watchlist()
    if not listings:
        write_status('done', '没有需要评分的房源')
        return

    logger.info(f"待评分: {len(listings)} 套")

    # 2. 启动 Chrome
    write_status('chrome', '启动 Chrome...')
    ensure_chrome_with_images()

    try:
        write_status('login', '登录 5i5j...')
        scraper = RentalDetailScraper()
        await scraper.connect()
        await scraper.check_and_login()

        # 3. 计算未缓存的通勤
        from finder.commute import batch_calc_commute, DEST_NAME
        watched_comms = [{'community': w['community'], 'community_id': w.get('community_id', ''),
                          'region': w.get('region', ''), 'biz_circle': w.get('biz_circle', '')}
                         for w in watchlist]
        uncached = [c for c in watched_comms if not db.get_commute(c['community'], DEST_NAME)]
        if uncached:
            write_status('commute', f'计算 {len(uncached)} 个小区通勤...')
            batch_calc_commute(db, uncached)

        # 4. 处理房源（爬详情页 + 下载 + 评分）
        grand_total = len(listings)
        concurrency = FINDER_CONFIG.get('concurrency', 5)
        sem = asyncio.Semaphore(concurrency)
        counter = {'done': 0, 'scored': 0}

        async def _process_one(listing):
            async with sem:
                hid = listing['house_id']
                comm = listing.get('community', '')
                label = f"{comm} {listing.get('layout', '')}"
                counter['done'] += 1
                n = counter['done']

                # 检查已有照片
                existing_photos = db.get_photos_for_house(hid)
                downloaded = [p for p in existing_photos if p.get('downloaded') and p.get('local_path')]

                if downloaded:
                    data_dir = Path(__file__).parent.parent / 'data'
                    local_paths = [str(data_dir / p['local_path']) for p in downloaded]
                    write_status('scoring', f'[{comm}] AI 评分', n, grand_total, label)
                else:
                    write_status('scraping', f'[{comm}] 爬详情页', n, grand_total, label)
                    detail = await scraper.scrape_detail(hid)
                    photos = detail.get('photos', [])
                    local_paths = []
                    if photos:
                        write_status('downloading', f'[{comm}] 下载 {len(photos)} 张', n, grand_total, label)
                        local_paths = image_mgr.download_photos(db, hid, photos)
                    write_status('scoring', f'[{comm}] AI 评分', n, grand_total, label)

                layout_prefix = listing.get('layout', '')[:2] if listing.get('layout') else None
                market = db.get_market_data(comm, layout_prefix)
                commute = db.get_commute(comm)

                result = scorer.score_listing(listing, local_paths,
                                              market_data=market, commute_data=commute)
                db.insert_rental_score(hid, result['scores'],
                                       result.get('summary', ''), scorer.model,
                                       raw_input=result.get('raw_input', ''),
                                       raw_output=result.get('raw_output', ''))
                db.mark_detail_scraped(hid)

                scores_str = ' '.join(f'{k}:{v}' for k, v in result['scores'].items())
                if filter_high_score(result['scores']):
                    counter['scored'] += 1
                    logger.info(f"  [{n}/{grand_total}] ★ {hid} ({label}): {scores_str}")
                    return {**listing, **result}
                else:
                    logger.info(f"  [{n}/{grand_total}] {hid} ({label}): {scores_str}")
                return None

        tasks = [_process_one(l) for l in listings]
        results = await asyncio.gather(*tasks)
        all_scored = [r for r in results if r]

        await scraper.close()

        # 5. 邮件通知
        if all_scored and notifier.is_configured():
            write_status('notify', f'发送邮件通知 ({len(all_scored)} 套高分)...')
            if notifier.send_alert(all_scored):
                db.mark_notified([s['house_id'] for s in all_scored])

        msg = f'完成: {grand_total} 套已评分, {len(all_scored)} 套高分'
        logger.info(msg)
        write_status('done', msg, grand_total, grand_total)

    except Exception as e:
        logger.error(f"扫描异常: {e}", exc_info=True)
        write_status('error', f'扫描异常: {e}')
    finally:
        shutdown_chrome()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="租房找房器")
    parser.add_argument("--rescore", action="store_true",
                        help="重新评分（跳过爬取，用已有照片）")
    parser.add_argument("--no-scrape", action="store_true",
                        help="跳过列表页爬取，直接对已有房源评分")
    args = parser.parse_args()

    if args.rescore:
        asyncio.run(run_rescore())
    elif args.no_scrape:
        asyncio.run(run_no_scrape())
    else:
        asyncio.run(run())


if __name__ == "__main__":
    main()
