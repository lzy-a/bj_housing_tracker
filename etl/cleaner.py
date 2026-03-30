import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    """数据清洗类"""
    
    def clean_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗房源数据"""
        if df.empty:
            logger.warning("房源数据为空")
            return pd.DataFrame()
        
        logger.info(f"原始数据数量: {len(df)}")
        
        # 1. 移除重复
        df = df.drop_duplicates(subset=['house_code'])
        
        # 2. 数据类型转换
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['area'] = pd.to_numeric(df['area'], errors='coerce')
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        
        # 3. 移除空值
        df = df.dropna(subset=['price', 'area', 'unit_price'])
        
        # 4. 数据有效性检查
        df = df[(df['price'] > 50) & (df['price'] < 10000)]
        df = df[(df['unit_price'] > 1000) & (df['unit_price'] < 100000)]
        df = df[(df['area'] > 20) & (df['area'] < 500)]
        
        logger.info(f"清洗后数据数量: {len(df)}")
        return df
    
    def aggregate_by_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """按区进行统计"""
        if df.empty:
            logger.warning("数据为空")
            return pd.DataFrame()
        
        stats = df.groupby('position').agg({
            'price': ['mean', 'median'],
            'unit_price': ['mean', 'median'],
            'title': 'count'
        }).reset_index()
        
        stats.columns = ['position', 'avg_price', 'median_price', 
                        'avg_unit_price', 'median_unit_price', 'listing_count']
        
        for col in ['avg_price', 'median_price', 'avg_unit_price', 'median_unit_price']:
            stats[col] = stats[col].round(2)
        
        stats['listing_count'] = stats['listing_count'].astype(int)
        
        logger.info(f"统计完成，共 {len(stats)} 个位置")
        return stats