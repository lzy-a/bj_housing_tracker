"""
房产中介数据爬虫
获取链家、安居客等中介平台的数据
"""
import requests
from bs4 import BeautifulSoup
import json
from typing import List, Dict
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LianjiaScraperBase:
    """链家数据爬虫基类"""
    
    BASE_URL = "https://bj.lianjia.com"
    
    def __init__(self, delay: float = 1.0):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.delay = delay  # 请求延迟，避免被反爬
    
    def fetch_listings(self, district: str, page: int = 1) -> List[Dict]:
        """获取二手房列表"""
        try:
            url = f"{self.BASE_URL}/ershoufang/{district}/pg{page}"
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.content, 'html.parser')
            listings = []
            
            # 解析房源信息（具体选择器需根据实际页面调整）
            for item in soup.find_all('li', class_='clear'):
                try:
                    listing = {
                        'title': item.find('a', class_='MuiLink-root').text,
                        'price': self._extract_price(item),
                        'district': district,
                        'area': self._extract_area(item),
                        'unit_price': self._extract_unit_price(item),
                        'timestamp': datetime.now().isoformat()
                    }
                    listings.append(listing)
                except Exception as e:
                    logger.warning(f"解析单个房源失败: {e}")
                    continue
            
            time.sleep(self.delay)  # 礼貌爬虫
            return listings
            
        except Exception as e:
            logger.error(f"获取二手房列表失败: {e}")
            return []
    
    def _extract_price(self, item) -> float:
        """提取价格"""
        try:
            price_text = item.find('span', class_='red').text
            return float(price_text)
        except:
            return 0.0
    
    def _extract_area(self, item) -> float:
        """提取面积"""
        try:
            area_text = item.find('span', class_='area').text
            return float(area_text.replace('㎡', ''))
        except:
            return 0.0
    
    def _extract_unit_price(self, item) -> float:
        """提取单价"""
        try:
            unit_price_text = item.find('span', class_='unitPriceValue').text
            return float(unit_price_text)
        except:
            return 0.0
    
    def fetch_all_districts(self) -> Dict[str, List[Dict]]:
        """获取所有区的房源数据"""
        districts = [
            'chaoyang', 'dongcheng', 'xicheng', 'haidian', 'fengtai',
            'shijingshan', 'daxing', 'tongzhou', 'changping', 'shunyi'
        ]
        
        results = {}
        for district in districts:
            logger.info(f"爬取 {district} 区数据...")
            results[district] = self.fetch_listings(district)
        
        return results

from datetime import datetime

