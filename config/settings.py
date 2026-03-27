"""
项目配置文件
"""
import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据路径
DATA_DIR = PROJECT_ROOT / 'data'
DB_PATH = DATA_DIR / 'db' / 'beijing_realestate.db'
RAW_DATA_DIR = DATA_DIR / 'raw'

# 爬虫配置
SCRAPERS_CONFIG = {
    'lianjia': {
        'enabled': True,
        'delay': 1.0,  # 请求延迟（秒）
        'timeout': 10,
    },
    'zjw': {
        'enabled': True,
        'delay': 2.0,
    }
}

# 数据库配置
DATABASE = {
    'type': 'sqlite',  # 可改为 'postgresql'
    'path': str(DB_PATH),
}

# 看板配置
DASHBOARD_CONFIG = {
    'host': '127.0.0.1',
    'port': 8080,
    'debug': True,
}

# 日志配置
LOGGING = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
}

# 爬取计划（cron表达式）
CRAWLER_SCHEDULE = {
    'lianjia': '0 */6 * * *',  # 每6小时
    'zjw': '0 0 * * *',         # 每天00:00
}