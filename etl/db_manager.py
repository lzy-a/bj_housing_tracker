"""
数据库管理模块
使用SQLite存储数据（可升级为PostgreSQL）
"""
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self, db_path: str = 'data/db/beijing_realestate.db'):
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """初始化数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 房源信息表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                price REAL NOT NULL,
                area REAL NOT NULL,
                unit_price REAL NOT NULL,
                district TEXT NOT NULL,
                source TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(title, district, timestamp)
            )
        ''')
        
        # 区级统计表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS district_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                district TEXT NOT NULL,
                avg_price REAL,
                median_price REAL,
                avg_unit_price REAL,
                median_unit_price REAL,
                listing_count INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(district, timestamp)
            )
        ''')
        
        # 政策新闻表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS policies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                content TEXT,
                source TEXT,
                publish_date DATETIME,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"数据库初始化完成: {self.db_path}")
    
    def insert_listings(self, df: pd.DataFrame):
        """插入房源数据"""
        conn = sqlite3.connect(self.db_path)
        try:
            df.to_sql('listings', conn, if_exists='append', index=False)
            logger.info(f"成功插入 {len(df)} 条房源数据")
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
        finally:
            conn.close()
    
    def get_district_stats(self) -> pd.DataFrame:
        """获取最新区级统计数据"""
        conn = sqlite3.connect(self.db_path)
        query = '''
            SELECT * FROM district_stats 
            WHERE timestamp = (SELECT MAX(timestamp) FROM district_stats)
        '''
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_price_trend(self, district: str = None, days: int = 30) -> pd.DataFrame:
        """获取价格趋势数据"""
        conn = sqlite3.connect(self.db_path)
        
        where_clause = f"WHERE district = '{district}'" if district else ""
        query = f'''
            SELECT 
                DATE(timestamp) as date,
                district,
                AVG(unit_price) as avg_unit_price,
                COUNT(*) as listing_count
            FROM listings
            {where_clause}
            AND timestamp >= datetime('now', '-{days} days')
            GROUP BY DATE(timestamp), district
            ORDER BY date DESC
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df