import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据路径
DATA_DIR = PROJECT_ROOT / 'data'
DB_PATH = str(DATA_DIR / 'db' / 'beijing_realestate.db')
RAW_DATA_DIR = DATA_DIR / 'raw'

# 创建目录
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DATA_DIR / 'db', exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# 爬虫配置
SCRAPERS_CONFIG = {
    'lianjia': {
        'enabled': True,
        'delay': 1.0,
        'timeout': 10,
    }
}

# 日志配置
LOGGING = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}