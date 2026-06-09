"""
通勤计算模块 — 高德API计算小区到目的地的公交时间，按小区缓存。
"""
import logging
import time
import requests

logger = logging.getLogger(__name__)

AMAP_KEY = '4a11929c8af1f7b84bd33bbdffb61b2a'
DEST_NAME = '太平桥站'
DEST_LNG, DEST_LAT = 116.363297, 39.910065
# API 返回的 duration 已包含步行，不再额外加时间


def geocode(address: str) -> tuple:
    """地理编码：地址 → 经纬度"""
    url = 'https://restapi.amap.com/v3/geocode/geo'
    try:
        r = requests.get(url, params={
            'key': AMAP_KEY, 'address': address, 'city': '北京'
        }, timeout=10)
        d = r.json()
        if d.get('status') == '1' and d.get('geocodes'):
            lng, lat = d['geocodes'][0]['location'].split(',')
            return float(lng), float(lat)
    except Exception as e:
        logger.warning(f"地理编码失败: {address} - {e}")
    return None, None


def transit_time(lng: float, lat: float) -> tuple:
    """公交导航：起点经纬度 → (分钟, 距离米, 步行距离米)"""
    url = 'https://restapi.amap.com/v3/direction/transit/integrated'
    try:
        r = requests.get(url, params={
            'key': AMAP_KEY,
            'origin': f'{lng},{lat}',
            'destination': f'{DEST_LNG},{DEST_LAT}',
            'city': '北京',
            'strategy': 0,
            'nightflag': 0
        }, timeout=10)
        d = r.json()
        if d.get('status') == '1' and d.get('route', {}).get('transits'):
            t = d['route']['transits'][0]
            minutes = int(t['duration']) // 60
            distance = int(t['distance'])
            walking = int(t.get('walking_distance', 0))
            return minutes, distance, walking
    except Exception as e:
        logger.warning(f"公交导航失败: {lng},{lat} - {e}")
    return None, None, None


def calc_commute(community: str, region: str = '', biz_circle: str = '') -> dict:
    """计算单个小区的通勤时间（含步行5分钟）。返回 {'minutes': int, ...} 或 None"""
    addr = f'北京市{region}{biz_circle or ""}{community}'
    lng, lat = geocode(addr)
    if lng is None:
        return None

    time.sleep(0.15)  # 高德限速
    minutes, distance, walking = transit_time(lng, lat)
    if minutes is None:
        return None

    return {
        'minutes': minutes,
        'transit_minutes': minutes,
        'distance': distance,
        'walking': walking,
        'lng': lng,
        'lat': lat,
    }


def batch_calc_commute(db, communities: list, dest_name: str = DEST_NAME) -> int:
    """批量计算通勤时间并缓存到DB。communities: list of dict with community, community_id, region, biz_circle"""
    cached = 0
    for i, c in enumerate(communities):
        comm = c['community']
        comm_id = c.get('community_id', '')

        # 检查缓存
        existing = db.get_commute(comm, dest_name)
        if existing:
            cached += 1
            continue

        result = calc_commute(comm, c.get('region', ''), c.get('biz_circle', ''))
        if result:
            db.save_commute(
                comm, comm_id, dest_name,
                result['minutes'], result['distance'], result['walking'],
                result['lng'], result['lat']
            )
            cached += 1
            logger.info(f"  通勤: {comm} → {result['minutes']}min")

        if (i + 1) % 20 == 0:
            logger.info(f"  通勤计算进度: {i+1}/{len(communities)}")

    return cached
