"""
FastAPI 后端 — 租房找房器交互式前端 + API。
启动：uvicorn finder.app:app --host 0.0.0.0 --port 8080
"""
import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import FINDER_CONFIG
from etl.db_manager import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="租房找房器")

# 静态文件和模板
static_dir = Path(__file__).parent / 'static'
template_dir = Path(__file__).parent / 'templates'
image_dir = Path(__file__).parent.parent / 'data' / 'images'

static_dir.mkdir(parents=True, exist_ok=True)
template_dir.mkdir(parents=True, exist_ok=True)
image_dir.mkdir(parents=True, exist_ok=True)

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
app.mount("/images", StaticFiles(directory=str(image_dir)), name="images")
templates = Jinja2Templates(directory=str(template_dir))

# DB 连接
db = DatabaseManager()


# ---- 页面 ----

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ---- Watchlist API ----

@app.get("/api/watchlist")
async def get_watchlist():
    rows = db.get_watchlist()
    return {"watchlist": rows}


@app.post("/api/watchlist")
async def add_watchlist(request: Request):
    data = await request.json()
    community = data.get('community', '').strip()
    if not community:
        raise HTTPException(400, "小区名不能为空")
    db.add_to_watchlist(
        community=community,
        region=data.get('region'),
        biz_circle=data.get('biz_circle'),
        community_id=data.get('community_id'),
        notes=data.get('notes'),
        filter_criteria=data.get('filter_criteria'),
    )
    return {"ok": True}


@app.put("/api/watchlist/{watchlist_id}")
async def update_watchlist(watchlist_id: int, request: Request):
    data = await request.json()
    db.update_watchlist(watchlist_id, **data)
    return {"ok": True}


@app.delete("/api/watchlist/{watchlist_id}")
async def delete_watchlist(watchlist_id: int):
    db.remove_from_watchlist(watchlist_id)
    return {"ok": True}


# ---- Listings API ----

@app.get("/api/listings")
async def get_listings(
    min_score: int = Query(None, ge=0, le=10),
    max_price: float = Query(None),
    min_area: float = Query(None),
    max_area: float = Query(None),
    region: str = Query(None),
    community: str = Query(None),
    rent_type: str = Query(None),
    layout: str = Query(None),
    sort_by: str = Query('score_date'),
    sort_dir: str = Query('DESC'),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    result = db.get_listings_with_scores(
        min_score=min_score, max_price=max_price,
        min_area=min_area, max_area=max_area,
        region=region, community=community, rent_type=rent_type,
        layout=layout,
        sort_by=sort_by, sort_dir=sort_dir,
        page=page, page_size=page_size,
    )
    return result


@app.get("/api/listings/{house_id}")
async def get_listing_detail(house_id: str):
    detail = db.get_listing_detail(house_id)
    if not detail:
        raise HTTPException(404, "房源不存在")
    # 序列化 datetime/date 对象
    return _serialize(detail)


# ---- Photos API ----

@app.get("/api/photos/{house_id}")
async def get_photos(house_id: str):
    photos = db.get_photos_for_house(house_id)
    return {"photos": photos}


# ---- Communities search ----

@app.get("/api/communities")
async def search_communities(q: str = Query('', min_length=1)):
    results = db.search_communities(q)
    return {"communities": results}


# ---- Stats ----

@app.get("/api/stats")
async def get_stats():
    conn = db._get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM watchlist_communities WHERE is_active = TRUE")
        watched = cursor.fetchone()[0]
        cursor.execute("""
            SELECT COUNT(DISTINCT s.house_id) FROM rental_scores s
            JOIN rental_details r ON s.house_id = r.house_id WHERE r.status = 1
        """)
        scored = cursor.fetchone()[0]
        cursor.execute("""
            SELECT COUNT(*) FROM rental_scores
            WHERE score_date = CURRENT_DATE AND notified = FALSE
        """)
        unnotified = cursor.fetchone()[0]
        return {"watched_communities": watched, "scored_listings": scored, "unnotified": unnotified}
    finally:
        cursor.close()
        db._return_connection(conn)


# ---- Scan ----

STATUS_FILE = Path(__file__).parent.parent / 'data' / 'finder_status.json'


@app.post("/api/trigger-scan")
async def trigger_scan():
    """手动触发一次扫描（后台运行）"""
    # 检查是否已在运行
    if STATUS_FILE.exists():
        try:
            status = json.loads(STATUS_FILE.read_text(encoding='utf-8'))
            if status.get('running'):
                return {"ok": False, "message": "扫描正在运行中"}
        except Exception:
            pass
    try:
        # 先写一个 "启动中" 状态，防止轮询读到旧的 running:false
        STATUS_FILE.write_text(json.dumps({
            'phase': 'starting', 'message': '扫描启动中...',
            'progress': 0, 'total': 0, 'detail': '',
            'timestamp': datetime.now().isoformat(), 'running': True,
        }, ensure_ascii=False), encoding='utf-8')

        log_file = Path(__file__).parent.parent / 'data' / 'finder_scan.log'
        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__).parent / 'run_finder.py')],
            stdout=open(log_file, 'w'), stderr=subprocess.STDOUT,
        )
        return {"ok": True, "pid": proc.pid, "message": "扫描已启动"}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/stop-scan")
async def stop_scan():
    """停止扫描"""
    import signal
    # 写入停止状态
    STATUS_FILE.write_text(json.dumps({
        'phase': 'stopped', 'message': '用户手动停止',
        'progress': 0, 'total': 0, 'detail': '',
        'timestamp': datetime.now().isoformat(), 'running': False,
    }, ensure_ascii=False), encoding='utf-8')
    # 杀掉 run_finder.py 进程
    try:
        subprocess.run(["pkill", "-f", "run_finder.py"], timeout=3, capture_output=True)
    except Exception:
        pass
    return {"ok": True, "message": "已停止"}


@app.get("/api/scan-status")
async def scan_status():
    """查询扫描进度"""
    if not STATUS_FILE.exists():
        return {"running": False, "phase": "idle", "message": "未运行"}
    try:
        status = json.loads(STATUS_FILE.read_text(encoding='utf-8'))
        return status
    except Exception:
        return {"running": False, "phase": "unknown", "message": "状态读取失败"}


@app.post("/api/rescore/{house_id}")
async def rescore_listing(house_id: str):
    """重新评分单个房源"""
    from pathlib import Path as P
    from finder.scorer import RentalScorer
    from finder.image_manager import ImageManager

    # 获取房源信息
    detail = db.get_listing_detail(house_id)
    if not detail:
        raise HTTPException(404, "房源不存在")

    listing_info = {
        'house_id': house_id,
        'title': detail.get('title', ''),
        'region': detail.get('region', ''),
        'biz_circle': detail.get('biz_circle', ''),
        'community': detail.get('community', ''),
        'layout': detail.get('layout', ''),
        'area': detail.get('area'),
        'rent_price': detail.get('rent_price'),
        'rent_type': detail.get('rent_type', ''),
        'orientation': detail.get('orientation', ''),
        'decoration': detail.get('decoration', ''),
        'floor_info': detail.get('floor_info', ''),
    }

    # 获取已有照片路径
    photos = detail.get('photos', [])
    downloaded = [p for p in photos if p.get('downloaded') and p.get('local_path')]
    data_dir = P(__file__).parent.parent / 'data'
    image_paths = [str(data_dir / p['local_path']) for p in downloaded]

    # 查市场均价 + 通勤
    comm = listing_info.get('community', '')
    layout_prefix = listing_info.get('layout', '')[:2] if listing_info.get('layout') else ''
    market = db.get_market_data(comm, layout_prefix)
    commute = db.get_commute(comm)

    # 评分
    scorer = RentalScorer()
    result = scorer.score_listing(listing_info, image_paths,
                                  market_data=market, commute_data=commute)

    # 保存
    db.insert_rental_score(house_id, result['scores'],
                           result.get('summary', ''), scorer.model,
                           raw_input=result.get('raw_input', ''),
                           raw_output=result.get('raw_output', ''))

    return {"ok": True, "scores": result['scores'], "summary": result.get('summary', '')}


@app.get("/api/scan-log")
async def scan_log(lines: int = Query(50)):
    """查看扫描日志尾部"""
    log_file = Path(__file__).parent.parent / 'data' / 'finder_scan.log'
    if not log_file.exists():
        return {"log": ""}
    try:
        all_lines = log_file.read_text(encoding='utf-8').splitlines()
        return {"log": '\n'.join(all_lines[-lines:])}
    except Exception:
        return {"log": ""}


def _serialize(obj):
    """递归序列化 datetime/date 对象"""
    from datetime import datetime, date
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj
