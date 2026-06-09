#!/usr/bin/env python3
"""
北京租房通勤分析脚本
目标：筛选到太平桥站（19号线）通勤 ≤ 45分钟的整租一居室（≤4500元）
输出：综合排名报告（HTML + CSV）
"""

import psycopg2
import requests
import time
import json
import csv
from datetime import datetime

# ========== 配置 ==========
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'house_data',
    'user': 'mb_admin',
    'password': 'zy2206124'
}
AMAP_KEY = '4a11929c8af1f7b84bd33bbdffb61b2a'

# 太平桥站坐标
DEST_LNG, DEST_LAT = 116.363297, 39.910065

# 通勤上限（分钟）
MAX_COMMUTE_MINUTES = 45

# 请求间隔（高德免费API QPS限制）
REQUEST_INTERVAL = 0.2


# ========== 高德API调用 ==========
def geocode(address, city='北京'):
    """地址 → 经纬度"""
    url = 'https://restapi.amap.com/v3/geocode/geo'
    params = {
        'key': AMAP_KEY,
        'address': address,
        'city': city
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data['status'] == '1' and data['geocodes']:
            loc = data['geocodes'][0]['location']
            lng, lat = loc.split(',')
            return float(lng), float(lat), data['geocodes'][0].get('formatted_address', address)
    except Exception as e:
        print(f'  [地理编码失败] {address}: {e}')
    return None, None, None


def transit_commute(from_lng, from_lat, to_lng, to_lat):
    """公交/地铁通勤时间计算"""
    url = 'https://restapi.amap.com/v3/direction/transit/integrated'
    params = {
        'key': AMAP_KEY,
        'origin': f'{from_lng},{from_lat}',
        'destination': f'{to_lng},{to_lat}',
        'city': '北京',
        'strategy': 0,  # 最快捷模式
        'nightflag': 0  # 日间
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data['status'] == '1' and data.get('route', {}).get('transits'):
            # 取第一条路线
            transit = data['route']['transits'][0]
            duration_min = int(transit['duration']) // 60
            distance_m = int(transit['distance'])
            # 步行距离
            walking_m = int(transit.get('walking_distance', 0))
            return duration_min, distance_m, walking_m, 'transit'
    except Exception as e:
        pass
    return None, None, None, None


def walking_commute(from_lng, from_lat, to_lng, to_lat):
    """步行通勤时间计算"""
    url = 'https://restapi.amap.com/v3/direction/walking'
    params = {
        'key': AMAP_KEY,
        'origin': f'{from_lng},{from_lat}',
        'destination': f'{to_lng},{to_lat}'
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data['status'] == '1' and data.get('route', {}).get('paths'):
            path = data['route']['paths'][0]
            duration_min = int(path['duration']) // 60
            distance_m = int(path['distance'])
            return duration_min, distance_m, 'walk'
    except Exception as e:
        pass
    return None, None, None


def driving_commute(from_lng, from_lat, to_lng, to_lat):
    """驾车通勤时间计算（仅供参考）"""
    url = 'https://restapi.amap.com/v3/direction/driving'
    params = {
        'key': AMAP_KEY,
        'origin': f'{from_lng},{from_lat}',
        'destination': f'{to_lng},{to_lat}',
        'strategy': 0
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data['status'] == '1' and data.get('route', {}).get('paths'):
            path = data['route']['paths'][0]
            duration_min = int(path['duration']) // 60
            distance_m = int(path['distance'])
            return duration_min, distance_m, 'drive'
    except Exception as e:
        pass
    return None, None, None


# ========== 主流程 ==========
def main():
    print('=' * 60)
    print('北京租房通勤分析 — 太平桥站（19号线）')
    print(f'启动时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)

    # 1. 连接数据库，读取候选房源
    print('\n📡 连接数据库...')
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 只取丰台和朝阳（这两个区到太平桥通勤最合理）
    cur.execute("""
        SELECT 
            house_id, community_id, community, region, biz_circle,
            layout, area, rent_price, decoration, floor_info,
            orientation, title
        FROM rental_details
        WHERE rent_type = '整租'
          AND layout LIKE '%1室%'
          AND rent_price <= 4500
          AND status = 1
          AND region IN ('丰台区', '朝阳区', '海淀区', '石景山区', '西城区', '东城区')
        ORDER BY region, rent_price
    """)
    rows = cur.fetchall()
    print(f'✅ 候选房源：{len(rows)} 套')

    # 2. 按小区去重（同一小区多个房源，通勤时间一样）
    communities = {}
    for r in rows:
        comm_id = r[1]  # community_id
        if comm_id not in communities:
            # 构建地址用于地理编码
            addr = f'北京市{r[3]}{r[4] if r[4] else ""}{r[2]}'
            communities[comm_id] = {
                'community': r[2],
                'region': r[3],
                'biz_circle': r[4],
                'address': addr,
                'houses': []
            }
        communities[comm_id]['houses'].append(r)

    print(f'🏘️  涉及小区：{len(communities)} 个')

    # 3. 给每个小区计算通勤时间
    print('\n🚇 计算通勤时间...')
    commute_results = {}

    for i, (comm_id, info) in enumerate(communities.items()):
        if (i + 1) % 10 == 0:
            print(f'  进度: {i+1}/{len(communities)}')

        # 地理编码
        time.sleep(REQUEST_INTERVAL)
        lng, lat, formatted_addr = geocode(info['address'])
        if lng is None:
            continue

        # 公交/地铁通勤（主要指标）
        time.sleep(REQUEST_INTERVAL)
        transit_min, transit_dist, walking_dist, mode = transit_commute(lng, lat, DEST_LNG, DEST_LAT)

        commute_results[comm_id] = {
            'lng': lng,
            'lat': lat,
            'formatted_addr': formatted_addr,
            'transit_min': transit_min,
            'transit_dist': transit_dist,
            'walking_dist': walking_dist
        }

    print(f'✅ 完成 {len(commute_results)} 个小区的通勤计算')

    # 4. 筛选通勤达标的房源，按租金+通勤+面积综合排序
    print('\n📊 筛选 & 排序...')
    scored = []

    for comm_id, commute_info in commute_results.items():
        transit_min = commute_info['transit_min']
        if transit_min is None or transit_min > MAX_COMMUTE_MINUTES:
            continue

        info = communities[comm_id]
        for house in info['houses']:
            house_id = house[0]
            area = house[6]
            rent = house[7]
            decoration = house[8] or '未知'

            # 综合评分 (0-100)
            # 租金评分 (0-40): 越便宜越好
            price_score = max(0, 40 - (rent - 2000) / 2500 * 40)

            # 通勤评分 (0-30): 越短越好
            commute_score = max(0, 30 - transit_min / 45 * 30)

            # 面积评分 (0-15): 越大越好
            area_score = min(15, area / 60 * 15) if area else 8

            # 装修评分 (0-15): 精装>简装>其他
            deco_score = {'精装': 15, '简装': 10, '毛坯': 3}.get(decoration, 8)

            total_score = price_score + commute_score + area_score + deco_score

            scored.append({
                'house_id': house_id,
                'community': info['community'],
                'region': info['region'],
                'biz_circle': info['biz_circle'],
                'layout': house[5],
                'area': area,
                'rent_price': rent,
                'decoration': decoration,
                'floor_info': house[9],
                'orientation': house[10],
                'title': house[11],
                'transit_min': transit_min,
                'transit_dist': commute_info['transit_dist'],
                'walking_dist': commute_info['walking_dist'],
                'price_score': round(price_score, 1),
                'commute_score': round(commute_score, 1),
                'area_score': round(area_score, 1),
                'deco_score': deco_score,
                'total_score': round(total_score, 1)
            })

    # 按总分降序
    scored.sort(key=lambda x: x['total_score'], reverse=True)

    print(f'✅ 通勤达标房源：{len(scored)} 套')

    # 5. 输出 CSV
    csv_path = '/Users/liuziyang/WorkBuddy/2026-06-05-22-09-00/commute_results.csv'
    fields = [
        'rank', 'total_score', 'community', 'region', 'biz_circle',
        'rent_price', 'area', 'layout', 'decoration', 'floor_info',
        'orientation', 'transit_min', 'price_score', 'commute_score',
        'area_score', 'deco_score', 'house_id', 'title'
    ]
    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for idx, item in enumerate(scored[:200]):  # Top 200
            item['rank'] = idx + 1
            writer.writerow(item)
    print(f'📄 CSV 已保存: {csv_path}')

    # 6. 生成 HTML 报告
    html = generate_html(scored[:50])  # Top 50
    html_path = '/Users/liuziyang/WorkBuddy/2026-06-05-22-09-00/commute_report.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'📄 HTML 报告已保存: {html_path}')

    # 7. 统计摘要
    print('\n' + '=' * 60)
    print('📊 分析摘要')
    print('=' * 60)

    regions = {}
    for item in scored:
        regions[item['region']] = regions.get(item['region'], 0) + 1
    print(f'✅ 通勤达标总房源: {len(scored)} 套')
    print(f'📍 区域分布:')
    for reg, cnt in sorted(regions.items(), key=lambda x: -x[1]):
        print(f'   {reg}: {cnt} 套')

    if scored:
        print(f'\n🏆 Top 5 推荐:')
        for i, item in enumerate(scored[:5]):
            print(f"   #{i+1} {item['community']}({item['region']}) "
                  f"¥{item['rent_price']:.0f} {item['area']:.0f}㎡ "
                  f"通勤{item['transit_min']}分钟 "
                  f"总分{item['total_score']}")

    print(f'\n⏰ 完成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    cur.close()
    conn.close()


def generate_html(scored):
    """生成可交互HTML报告"""
    rows_html = ''
    for i, item in enumerate(scored):
        score_color = '#27ae60' if item['total_score'] >= 70 else '#f39c12' if item['total_score'] >= 50 else '#e74c3c'
        commute_color = '#27ae60' if item['transit_min'] <= 30 else '#f39c12' if item['transit_min'] <= 40 else '#e74c3c'

        rows_html += f'''
        <tr>
            <td class="rank">{i+1}</td>
            <td>
                <div class="comm-name">{item['community']}</div>
                <div class="comm-sub">{item['region']} {item['biz_circle']}</div>
            </td>
            <td class="price">¥{item['rent_price']:.0f}</td>
            <td>{item['area']:.0f}㎡</td>
            <td>{item['layout']}</td>
            <td>{item['decoration']}</td>
            <td style="color:{commute_color};font-weight:bold">{item['transit_min']}分钟</td>
            <td>
                <span class="score-badge" style="background:{score_color}">{item['total_score']}</span>
            </td>
            <td>
                <div class="mini-bar">
                    <span class="bar-label">价格</span><div class="bar"><div class="fill" style="width:{item['price_score']/40*100}%"></div></div><span>{item['price_score']}</span>
                </div>
                <div class="mini-bar">
                    <span class="bar-label">通勤</span><div class="bar"><div class="fill" style="width:{item['commute_score']/30*100}%"></div></div><span>{item['commute_score']}</span>
                </div>
                <div class="mini-bar">
                    <span class="bar-label">面积</span><div class="bar"><div class="fill" style="width:{item['area_score']/15*100}%"></div></div><span>{item['area_score']}</span>
                </div>
                <div class="mini-bar">
                    <span class="bar-label">装修</span><div class="bar"><div class="fill" style="width:{item['deco_score']/15*100}%"></div></div><span>{item['deco_score']}</span>
                </div>
            </td>
        </tr>'''

    return f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>北京租房通勤分析 — 太平桥站</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f6fa; padding: 20px; }}
.container {{ max-width: 1400px; margin: 0 auto; }}
h1 {{ font-size: 24px; color: #2c3e50; margin-bottom: 8px; }}
.subtitle {{ color: #7f8c8d; font-size: 14px; margin-bottom: 24px; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 12px rgba(0,0,0,0.08); }}
th {{ background: #2c3e50; color: white; padding: 14px 12px; text-align: left; font-weight: 600; font-size: 13px; }}
td {{ padding: 12px; border-bottom: 1px solid #ecf0f1; font-size: 13px; }}
tr:hover {{ background: #f8f9ff; }}
.rank {{ font-size: 20px; font-weight: 700; color: #3498db; width: 40px; }}
.comm-name {{ font-weight: 600; color: #2c3e50; }}
.comm-sub {{ font-size: 12px; color: #95a5a6; margin-top: 2px; }}
.price {{ font-weight: 700; color: #e74c3c; font-size: 15px; }}
.score-badge {{ display: inline-block; color: white; padding: 4px 10px; border-radius: 20px; font-weight: 700; font-size: 14px; min-width: 40px; text-align: center; }}
.mini-bar {{ display: flex; align-items: center; gap: 6px; margin: 2px 0; font-size: 11px; }}
.bar-label {{ width: 28px; color: #7f8c8d; text-align: right; }}
.bar {{ width: 80px; height: 6px; background: #ecf0f1; border-radius: 3px; overflow: hidden; }}
.fill {{ height: 100%; background: linear-gradient(90deg, #3498db, #2ecc71); border-radius: 3px; }}
.footer {{ margin-top: 20px; color: #95a5a6; font-size: 12px; text-align: center; }}
</style>
</head>
<body>
<div class="container">
    <h1>🏠 北京租房通勤分析</h1>
    <p class="subtitle">
        目的地：19号线太平桥站 | 条件：整租一居 ≤4500元 | 通勤 ≤45分钟 | 
        数据来源：我爱我家（{datetime.now().strftime('%Y-%m-%d')}）
    </p>
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>小区</th>
                <th>月租</th>
                <th>面积</th>
                <th>户型</th>
                <th>装修</th>
                <th>通勤</th>
                <th>总分</th>
                <th>分项评分</th>
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
    <p class="footer">评分规则：租金(40%) + 通勤(30%) + 面积(15%) + 装修(15%) | 仅展示 Top 50</p>
</div>
</body>
</html>'''


if __name__ == '__main__':
    main()
