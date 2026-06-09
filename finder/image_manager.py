"""
图片管理 — 下载房源照片到本地，存入 rental_photos 表。
"""
import logging
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

IMAGE_BASE_DIR = Path(__file__).parent.parent / 'data' / 'images'


class ImageManager:
    def __init__(self, base_dir=None):
        self.base_dir = Path(base_dir) if base_dir else IMAGE_BASE_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def download_photos(self, db_manager, house_id: str, photos: list) -> list:
        """下载照片并更新数据库。
        photos: list of dict with keys: photo_url, room_type
        Returns: list of local paths
        """
        # 先写入 DB
        db_manager.insert_rental_photos(house_id, photos)

        # 获取刚插入的记录
        db_photos = db_manager.get_photos_for_house(house_id)
        local_paths = []

        house_dir = self.base_dir / house_id
        house_dir.mkdir(parents=True, exist_ok=True)

        for i, photo in enumerate(db_photos):
            if photo.get('downloaded') and photo.get('local_path'):
                local_paths.append(photo['local_path'])
                continue

            url = photo['photo_url']
            ext = _guess_ext(url)
            filename = f"{i + 1:03d}{ext}"
            local_path = house_dir / filename

            try:
                resp = requests.get(url, timeout=15, headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
                    'Referer': 'https://bj.5i5j.com/',
                })
                resp.raise_for_status()
                local_path.write_bytes(resp.content)
                rel_path = str(local_path.relative_to(self.base_dir.parent))
                db_manager.update_photo_local_path(photo['id'], rel_path)
                local_paths.append(rel_path)
                logger.debug(f"下载: {rel_path}")
            except Exception as e:
                logger.warning(f"下载失败 {url}: {e}")

        return local_paths

    def get_image_path(self, rel_path: str) -> Path:
        """获取图片的绝对路径"""
        return self.base_dir.parent / rel_path


def _guess_ext(url: str) -> str:
    """从 URL 猜测文件扩展名"""
    lower = url.lower()
    for ext in ('.png', '.webp', '.gif'):
        if ext in lower:
            return ext
    return '.jpg'
