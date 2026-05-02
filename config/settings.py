import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据路径
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'

for d in [DATA_DIR, DATA_DIR / 'db', RAW_DATA_DIR]:
    os.makedirs(d, exist_ok=True)

# ---- 数据库连接（Python 应用侧）----
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'house_data'),
    'user': os.getenv('DB_USER', 'mb_admin'),
    'password': os.getenv('DB_PASSWORD', 'zy2206124'),
}

# ---- Chrome 远程调试 ----
CHROME_DEBUG_PORT = int(os.getenv('CHROME_DEBUG_PORT', '9223'))

# ---- 我爱我家登录 ----
I5I5J_PHONE = os.getenv('I5I5J_PHONE', '13436960685')
I5I5J_PASSWORD = os.getenv('I5I5J_PASSWORD', 'mysj113598')

# ---- 爬虫配置 ----
SCRAPER_CONFIG = {
    'delay': 0.0,
    'timeout': 30,
    'window_size': 5,
    'max_page': 2000,
    'batch_size': 500,
}

# ---- 日志 ----
LOGGING = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}
