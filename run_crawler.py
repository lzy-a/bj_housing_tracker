"""
爬虫执行脚本
"""
import sys
from pathlib import Path
from datetime import datetime
import schedule
import time
import logging

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from scrapers.lianjia_scraper import LianjiaScraperBase
from etl.cleaner import DataCleaner
from etl.db_manager import DatabaseManager
from config.settings import SCRAPERS_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CrawlerJob:
    """爬虫任务管理"""
    
    def __init__(self):
        self.lianjia_scraper = LianjiaScraperBase()
        self.cleaner = DataCleaner()
        self.db = DatabaseManager()
    
    def run_lianjia_crawler(self):
        """运行链家爬虫"""
        logger.info("🚀 开始爬取链家数据...")
        try:
            # 获取所有区的数据
            all_listings = self.lianjia_scraper.fetch_all_districts()
            
            # 合并所有数据
            all_data = []
            for district, listings in all_listings.items():
                for listing in listings:
                    listing['source'] = 'lianjia'
                all_data.extend(listings)
            
            if all_data:
                # 清洗数据
                df = self.cleaner.clean_listings(all_data)
                logger.info(f"✅ 清洗后得到 {len(df)} 条数据")
                
                # 计算区级统计
                district_stats = self.cleaner.aggregate_by_district(df)
                
                # 插入数据库
                self.db.insert_listings(df)
                logger.info(f"✅ 数据已保存到数据库")
            else:
                logger.warning("⚠️ 未获取到数据")
        
        except Exception as e:
            logger.error(f"❌ 爬虫执行失败: {e}")
    
    def run_scheduler(self):
        """运行定时任务"""
        # 设置定时任务
        schedule.every(6).hours.do(self.run_lianjia_crawler)
        
        logger.info("📅 爬虫定时任务已启动")
        
        # 首次运行
        self.run_lianjia_crawler()
        
        # 保持运行
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='北京楼市数据爬虫')
    parser.add_argument('--mode', choices=['once', 'schedule'], 
                       default='once', help='运行模式')
    
    args = parser.parse_args()
    
    job = CrawlerJob()
    
    if args.mode == 'once':
        logger.info("🔄 单次运行模式")
        job.run_lianjia_crawler()
    else:
        logger.info("📅 定时运行模式")
        job.run_scheduler()