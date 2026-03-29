import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaner:
    """数据清洗类"""
    
    def clean_listings(self, listings: list) -> pd.DataFrame:
        """
        清洗房源数据
        """
        if not listings:
            logger.warning("房源数据为空")
            return pd.DataFrame()
        
        df = pd.DataFrame(listings)
        logger.info(f"原始数据数量: {len(df)}")
        
        # 数据清洗
        # 1. 移除重复数据
        df = df.drop_duplicates(subset=['title', 'district'])
        
        # 2. 数据类型转换和验证
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['area'] = pd.to_numeric(df['area'], errors='coerce')
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        
        # 3. 移除空值和无效数据
        df = df.dropna(subset=['price', 'area', 'unit_price'])
        
        # 4. 确保必需列存在
        required_columns = ['title', 'price', 'area', 'unit_price', 'district', 'source']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"缺少必需列: {col}")
                df[col] = ''
        
        logger.info(f"清洗后数据数量: {len(df)}")
        return df
    
    def aggregate_by_district(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        按区进行统计聚合
        """
        if df.empty:
            logger.warning("数据为空，无法统计")
            return pd.DataFrame()
        
        stats = df.groupby('district').agg({
            'price': ['mean', 'median'],
            'unit_price': ['mean', 'median'],
            'title': 'count'
        }).reset_index()
        
        # 重命名列以匹配数据库表字段
        stats.columns = ['district', 'avg_price', 'median_price', 
                        'avg_unit_price', 'median_unit_price', 'listing_count']
        
        # 四舍五入
        for col in ['avg_price', 'median_price', 'avg_unit_price', 'median_unit_price']:
            stats[col] = stats[col].round(2)
        
        stats['listing_count'] = stats['listing_count'].astype(int)
        
        logger.info(f"统计完成，共 {len(stats)} 个区")
        return stats