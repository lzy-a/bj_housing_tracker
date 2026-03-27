"""
爬虫执行脚本
"""
import sys
from pathlib import Path
import logging

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from scrapers.lianjia_scraper import LianjiaScraperBase
from etl.cleaner import DataCleaner
from etl.db_manager import DatabaseManager

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
                all_data.extend(listings)
            
            if all_data:
                # 清洗数据
                df = self.cleaner.clean_listings(all_data)
                
                if not df.empty:
                    # 计算区级统计
                    district_stats = self.cleaner.aggregate_by_district(df)
                    
                    # 插入数据库
                    self.db.insert_listings(df)
                    self.db.insert_district_stats(district_stats)
                    logger.info(f"✅ 数据已保存到数据库")
                else:
                    logger.warning("⚠️ 清洗后无有效数据")
            else:
                logger.warning("⚠️ 未获取到数据")
        
        except Exception as e:
            logger.error(f"❌ 爬虫执行失败: {e}", exc_info=True)

if __name__ == '__main__':
    job = CrawlerJob()
    logger.info("🔄 开始运行爬虫")
    job.run_lianjia_crawler()
    logger.info("✅ 爬虫运行完成")
