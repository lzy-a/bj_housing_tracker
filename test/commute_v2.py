#!/usr/bin/env python3
"""
北京租房通勤分析脚本 v2 — 按小区去重优化
只对每个小区调用一次地理编码+通勤API，大幅减少调用次数
"""

import psycopg2
import requests
import time
import csv
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost', 'port': 5432, 'dbname': 'house_data',
    'user': 'mb_admin', 'password': 'zy2206124'
}
AMAP_KEY = '4a11929c8af1f7b84bd33bbdffb61b2a'
DEST_LNG, DEST_LAT = 116.363297, 39.910065
MAX_COMMUTE = 45

def geocode(addr):
    url = 'https://restapi.amap.com/v3/geocode/geo'
    r = requests.get(url, params={'key': AMAP_KEY, 'address': addr, 'city': '北京'}, timeout=10)
    d = r.json()
    if d['status'] == '1' and d['geocodes']:
        lng, lat = d['geocodes'][0]['location'].split(',')
        return float(lng), float(lat)
    return None, None

def transit_time(flng, flat):
    url = 'https://restapi.amap.com/v3/direction/transit/integrated'
    r = requests.get(url, params={
        'key': AMAP_KEY, 'origin': f'{flng},{flat}',
        'destination': f'{DEST_LNG},{DEST_LAT}',
        'city': '北京', 'strategy': 0, 'nightflag': 0
    }, timeout=10)
    d = r.json()
    if d['status'] == '1' and d.get('route', {}).get('transits'):
        t = d['route']['transits'][0]
        return int(t['duration']) // 60, int(t['distance']), int(t.get('walking_distance', 0))
    return None, None, None

def main():
    print('=' * 50)
    print('北京租房通勤分析 v2')
    print(f'启动: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 50)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 只取丰台+石景山（离太平桥最近）
    cur.execute("""
        SELECT DISTINCT community_id, community, region, biz_circle
        FROM rental_details
        WHERE rent_type='整租' AND layout LIKE '%1室%'
          AND rent_price <= 4500 AND status = 1
          AND region IN ('丰台区', '石景山区')
    """)
    communities = cur.fetchall()
    print(f'\n🏘️  去重后小区: {len(communities)} 个 (丰台+石景山)')

    # 通勤计算
    print('🚇 计算通勤时间...')
    commute = {}
    for i, c in enumerate(communities):
        comm_id, comm_name, region, biz = c
        if (i + 1) % 20 == 0:
            print(f'  进度: {i+1}/{len(communities)}')

        addr = f'北京市{region}{biz or ""}{comm_name}'
        time.sleep(0.15)
        lng, lat = geocode(addr)
        if lng is None:
            continue
        time.sleep(0.15)
        tmin, tdist, walk = transit_time(lng, lat)
        if tmin is None:
            continue
        commute[comm_id] = (tmin, tdist, walk, lng, lat)

    print(f'✅ 通勤计算完成: {len(commute)} 个小区')

    # 拿回房源详情
    cur.execute("""
        SELECT house_id, community_id, community, region, biz_circle,
               layout, area, rent_price, decoration, floor_info, orientation, title
        FROM rental_details
        WHERE rent_type='整租' AND layout LIKE '%1室%'
          AND rent_price <= 4500 AND status = 1
          AND region IN ('丰台区', '石景山区')
    """)
    houses = cur.fetchall()

    # 筛选+评分
    scored = []
    for h in houses:
        hid, cid, comm, reg, biz, layout, area, rent, deco, floor, orient, title = h
        if cid not in commute:
            continue
        tmin, tdist, walk, lng, lat = commute[cid]
        if tmin > MAX_COMMUTE:
            continue

        # 综合评分
        ps = max(0, 40 - (rent - 2000) / 2500 * 40)
        cs = max(0, 30 - tmin / 45 * 30)
        ars = min(15, (area or 30) / 60 * 15)
        ds = {'精装': 15, '简装': 10, '毛坯': 3}.get(deco or '', 8)
        total = ps + cs + ars + ds

        scored.append({
            'house_id': hid, 'community': comm, 'region': reg, 'biz_circle': biz,
            'rent': rent, 'area': area, 'layout': layout, 'deco': deco or '',
            'floor': floor or '', 'orient': orient or '', 'title': title or '',
            'transit_min': tmin, 'transit_dist': tdist, 'walk_dist': walk,
            'price_s': round(ps, 1), 'commute_s': round(cs, 1),
            'area_s': round(ars, 1), 'deco_s': ds, 'total_s': round(total, 1)
        })

    scored.sort(key=lambda x: x['total_s'], reverse=True)

    # 按小区去重取最优（同一小区只留评分最高的）
    seen_comm = set()
    deduped = []
    for s in scored:
        if s['community'] not in seen_comm:
            deduped.append(s)
            seen_comm.add(s['community'])

    print(f'✅ 通勤达标: {len(scored)} 套 / {len(deduped)} 个小区')

    # 输出 CSV
    csv_path = '/Users/liuziyang/WorkBuddy/2026-06-05-22-09-00/commute_results.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=deduped[0].keys())
        w.writeheader()
        for i, item in enumerate(deduped[:100]):
            w.writerow(item)
    print(f'📄 CSV: {csv_path}')

    # 生成 HTML
    html = gen_html(deduped[:50])
    html_path = '/Users/liuziyang/WorkBuddy/2026-06-05-22-09-00/commute_report.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'📄 HTML: {html_path}')

    # 摘要
    regions = {}
    for s in scored:
        regions[s['region']] = regions.get(s['region'], 0) + 1
    print(f'\n📍 通勤达标区域分布:')
    for reg, cnt in sorted(regions.items(), key=lambda x: -x[1]):
        print(f'   {reg}: {cnt} 套')

    print(f'\n🏆 Top 10 小区:')
    for i, s in enumerate(deduped[:10]):
        print(f'   #{i+1} {s["community"]}({s["region"]}) '
              f'¥{s["rent"]:.0f} {s["area"]:.0f}㎡ '
              f'通勤{s["transit_min"]}分钟 总分{s["total_s"]}')

    print(f'\n⏰ 完成: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    cur.close()
    conn.close()


def gen_html(items):
    rows = ''
    for i, s in enumerate(items):
        c = '#27ae60' if s['total_s'] >= 70 else '#f39c12' if s['total_s'] >= 50 else '#e74c3c'
        tc = '#27ae60' if s['transit_min'] <= 30 else '#f39c12' if s['transit_min'] <= 40 else '#e74c3c'
        rows += f'''
        <tr>
            <td class="r">{i+1}</td>
            <td><div class="cn">{s['community']}</div><div class="cs">{s['region']} {s['biz_circle']}</div></td>
            <td class="pr">¥{s['rent']:.0f}</td>
            <td>{s['area']:.0f}㎡</td>
            <td>{s['layout']}</td>
            <td>{s['deco']}</td>
            <td style="color:{tc};font-weight:bold">{s['transit_min']}分钟</td>
            <td><span class="sb" style="background:{c}">{s['total_s']}</span></td>
            <td>
                <div class="mb"><span class="bl">价格</span><div class="bb"><div class="bf" style="width:{s['price_s']/40*100}%"></div></div><span>{s['price_s']}</span></div>
                <div class="mb"><span class="bl">通勤</span><div class="bb"><div class="bf" style="width:{s['commute_s']/30*100}%"></div></div><span>{s['commute_s']}</span></div>
                <div class="mb"><span class="bl">面积</span><div class="bb"><div class="bf" style="width:{s['area_s']/15*100}%"></div></div><span>{s['area_s']}</span></div>
                <div class="mb"><span class="bl">装修</span><div class="bb"><div class="bf" style="width:{s['deco_s']/15*100}%"></div></div><span>{s['deco_s']}</span></div>
            </td>
        </tr>'''

    return f'''<!DOCTYPE html><html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>北京租房通勤分析 — 太平桥站</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#f5f6fa;padding:20px}}
.c{{max-width:1400px;margin:0 auto}}
h1{{font-size:24px;color:#2c3e50;margin-bottom:4px}}
.su{{color:#7f8c8d;font-size:14px;margin-bottom:20px}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)}}
th{{background:#2c3e50;color:#fff;padding:12px 10px;text-align:left;font-weight:600;font-size:13px}}
td{{padding:10px;border-bottom:1px solid #ecf0f1;font-size:13px}}
tr:hover{{background:#f8f9ff}}
.r{{font-size:18px;font-weight:700;color:#3498db;width:35px}}
.cn{{font-weight:600;color:#2c3e50}}
.cs{{font-size:11px;color:#95a5a6;margin-top:2px}}
.pr{{font-weight:700;color:#e74c3c;font-size:15px}}
.sb{{display:inline-block;color:#fff;padding:4px 10px;border-radius:20px;font-weight:700;font-size:14px;min-width:36px;text-align:center}}
.mb{{display:flex;align-items:center;gap:5px;margin:2px 0;font-size:11px}}
.bl{{width:26px;color:#7f8c8d;text-align:right}}
.bb{{width:70px;height:6px;background:#ecf0f1;border-radius:3px;overflow:hidden}}
.bf{{height:100%;background:linear-gradient(90deg,#3498db,#2ecc71);border-radius:3px}}
.ft{{margin-top:20px;color:#95a5a6;font-size:12px;text-align:center}}
</style></head>
<body><div class="c">
<h1>🏠 北京租房通勤分析</h1>
<p class="su">目的地: 19号线太平桥站 | 整租一居 ≤4500元 | 通勤 ≤45分钟 | 丰台+石景山 | 同小区取最优 | {datetime.now().strftime('%Y-%m-%d')}</p>
<table><thead><tr><th>#</th><th>小区</th><th>月租</th><th>面积</th><th>户型</th><th>装修</th><th>通勤</th><th>总分</th><th>分项</th></tr></thead>
<tbody>{rows}</tbody></table>
<p class="ft">评分: 租金40% + 通勤30% + 面积15% + 装修15% | Top 50小区 (每个小区仅展示最优房源)</p>
</div></body></html>'''


if __name__ == '__main__':
    main()
