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
I5I5J_PHONE_2 = os.getenv('I5I5J_PHONE_2', '18501287718')
I5I5J_PASSWORD = os.getenv('I5I5J_PASSWORD', 'mysj113598')

# ---- 爬虫配置 ----
SCRAPER_CONFIG = {
    'delay': 0.0,
    'delay_range': [0.2, 0.4],
    'timeout': 30,
    'window_size': 5,
    'max_page': 2000,
    'restart_interval': 400,
    'batch_size': 500,
}

# ---- LLM API（共用 Key 和端点，模型各自配置）----
LLM_API_KEY = os.getenv('LLM_API_KEY', os.getenv('MIMO_KEY', os.getenv('ANTHROPIC_API_KEY', '')))
LLM_BASE_URL = os.getenv('LLM_BASE_URL', 'https://token-plan-cn.xiaomimimo.com/anthropic')

# 分析师模型
CLAUDE_API_KEY = LLM_API_KEY
CLAUDE_BASE_URL = LLM_BASE_URL
CLAUDE_MODEL = os.getenv('ANALYST_MODEL', os.getenv('CLAUDE_MODEL', 'mimo-v2.5-pro'))

# 找房器模型
FINDER_MODEL = os.getenv('FINDER_MODEL', 'mimo-v2.5-pro')

# ---- 分析师配置 ----
ANALYST_CONFIG = {
    'daily_max_tokens': 8192,
    'weekly_max_tokens': 16384,
    'thinking_enabled': True,   # 免费 token plan，开到最大
    'temperature': 1.0,
}

# ---- Finder (租房找房器) ----
FINDER_CONFIG = {
    'concurrency': 5,
    'scrape_delay_range': [0.3, 0.8],
    'alert_score_threshold': 7,
    'max_images_per_listing': 10,
}

FINDER_API_KEY = os.getenv('FINDER_API_KEY', LLM_API_KEY)
FINDER_BASE_URL = os.getenv('FINDER_BASE_URL', LLM_BASE_URL)

# 视觉模型（支持多模态图片输入，留空则用 FINDER_MODEL）
FINDER_VISION_API_KEY = os.getenv('FINDER_VISION_API_KEY', '')
FINDER_VISION_BASE_URL = os.getenv('FINDER_VISION_BASE_URL', '')
FINDER_VISION_MODEL = os.getenv('FINDER_VISION_MODEL', '')

# ---- Email (Gmail SMTP) ----
GMAIL_SMTP_SERVER = os.getenv('GMAIL_SMTP_SERVER', 'smtp.gmail.com')
GMAIL_SMTP_PORT = int(os.getenv('GMAIL_SMTP_PORT', '587'))
GMAIL_SENDER = os.getenv('GMAIL_SENDER', '')
GMAIL_PASSWORD = os.getenv('GMAIL_PASSWORD', '')
GMAIL_RECIPIENT = os.getenv('GMAIL_RECIPIENT', '')

# ---- 日志 ----
LOGGING = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}
