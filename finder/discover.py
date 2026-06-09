#!/usr/bin/env python3
"""
发现潜在小区 — 扫描所有小区通勤时间，找出符合要求但未收藏的。
用法：python finder/discover.py [--max-commute 45]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.db_manager import DatabaseManager
from finder.commute import batch_calc_commute, DEST_NAME


def main():
    parser = argparse.ArgumentParser(description="发现通勤达标的潜在小区")
    parser.add_argument('--max-commute', type=int, default=45, help='最大通勤时间（分钟）')
    parser.add_argument('--min-price', type=float, default=3000, help='最低价格')
    parser.add_argument('--max-price', type=float, default=4500, help='最高价格')
    parser.add_argument('--min-count', type=int, default=2, help='小区最少房源数')
    args = parser.parse_args()

    db = DatabaseManager()

    # 1. 计算未缓存的小区通勤（只算有符合条件房源的小区）
    uncached = db.get_uncached_communities(DEST_NAME, max_price=args.max_price,
                                           layout_prefix='1室')
    if uncached:
        print(f'计算 {len(uncached)} 个小区的通勤时间...')
        batch_calc_commute(db, uncached, DEST_NAME)

    # 2. 获取符合条件的小区
    all_commutes = db.get_all_commutes(DEST_NAME, args.max_commute,
                                       min_price=args.min_price, max_price=args.max_price,
                                       layout_prefix='1室', min_count=args.min_count)

    # 3. 过滤掉已收藏的
    watchlist = db.get_watchlist()
    watched_names = {w['community'] for w in watchlist}

    results = [c for c in all_commutes if c['community'] not in watched_names]

    # 4. 输出
    print(f'\n通勤≤{args.max_commute}分钟的未收藏小区（{len(results)} 个）：\n')
    print(f'{"小区":<12} {"区域":<8} {"板块":<10} {"通勤":>6} {"最低价":>8} {"在租":>4}')
    print('-' * 60)
    for r in results:
        min_rent = f"¥{r['min_rent']:.0f}" if r.get('min_rent') else '-'
        print(f'{r["community"]:<12} {r.get("region",""):<8} {r.get("biz_circle",""):<10} '
              f'{r["transit_minutes"]:>4}min {min_rent:>8} {r.get("listing_count",0):>4}套')


if __name__ == '__main__':
    main()
