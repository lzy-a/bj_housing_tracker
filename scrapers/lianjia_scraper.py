import requests
from bs4 import BeautifulSoup
import logging
import time
import json
from typing import List, Dict
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LianjiaScraperBase:
    """链家数据爬虫 - 改进版"""
    
    def __init__(self, delay: float = 2.0, max_retries: int = 3):
        """
        初始化爬虫
        delay: 请求间隔（秒）
        max_retries: 最大重试次数
        """
        self.base_url = 'https://bj.lianjia.com'
        self.ershoufang_url = self.base_url + '/ershoufang'
        self.delay = delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://bj.lianjia.com/ershoufang/'
        })
        
        # 区代码映射
        self.district_map = {
            'chaoyang': '朝阳区',
            'dongcheng': '东城区',
            'xicheng': '西城区',
            'haidian': '海淀区',
            'fengtai': '丰台区',
            'shijingshan': '石景山区',
            'daxing': '大兴区',
            'tongzhou': '通州区',
        }
    
    def request_get(self, url: str) -> str:
        """发送请求，带重试机制"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"[尝试 {attempt+1}/{self.max_retries}] 请求: {url}")
                response = self.session.get(url, timeout=15)
                response.encoding = 'utf-8'
                
                if response.status_code == 200:
                    time.sleep(self.delay)
                    return response.text
                else:
                    logger.warning(f"状态码 {response.status_code}，等待后重试...")
                    time.sleep(self.delay * 2)
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"请求异常: {e}，等待后重试...")
                time.sleep(self.delay * 2)
        
        logger.error(f"请求失败: {url}")
        return ""
    
    def extract_information(self, soup: BeautifulSoup) -> List[Dict]:
        """从 HTML 提取房源信息"""
        data = []
        
        try:
            # 查找房源列表容器
            house_list = soup.find('ul', {'class': 'sellListContent'})
            if not house_list:
                logger.warning("未找到房源列表")
                return data
            
            # 遍历每个房源
            for house_item in house_list.find_all('li', recursive=False):
                try:
                    info_div = house_item.find('div', {'class': 'info'}, recursive=False)
                    if not info_div:
                        continue
                    
                    # ========== 标题和ID ==========
                    title_div = info_div.find('div', {'class': 'title'}, recursive=False)
                    if not title_div:
                        continue
                    title_link = title_div.find('a')
                    if not title_link:
                        continue
                    
                    house_title = title_link.get_text(strip=True)
                    house_id = title_link.get('href', '').split('/')[-1].replace('.html', '')
                    
                    # ========== 位置信息 ==========
                    flood_div = info_div.find('div', {'class': 'flood'}, recursive=False)
                    house_location = '未知'
                    if flood_div:
                        location_link = flood_div.find('a')
                        if location_link:
                            house_location = location_link.get_text(strip=True)
                    
                    # ========== 详细信息 ==========
                    address_div = info_div.find('div', {'class': 'address'}, recursive=False)
                    if not address_div:
                        continue
                    
                    address_parts = [p.strip() for p in address_div.get_text(strip=True).split('|')]
                    
                    house_type = address_parts[0] if len(address_parts) > 0 else '未知'
                    house_size = address_parts[1] if len(address_parts) > 1 else '未知'
                    house_towards = address_parts[2] if len(address_parts) > 2 else '未知'
                    house_flood = address_parts[3] if len(address_parts) > 3 else '未知'
                    house_year = address_parts[4] if len(address_parts) > 4 else '未知'
                    house_building = address_parts[5] if len(address_parts) > 5 else '未知'
                    
                    # ========== 价格信息 ==========
                    price_info = info_div.find('div', {'class': 'priceInfo'}, recursive=False)
                    house_total_price = '未知'
                    house_unit_price = '未知'
                    
                    if price_info:
                        total_price_div = price_info.find('div', {'class': 'totalPrice'}, recursive=False)
                        if total_price_div:
                            total_price_span = total_price_div.find('span')
                            if total_price_span:
                                house_total_price = total_price_span.get_text(strip=True)
                        
                        unit_price_div = price_info.find('div', {'class': 'unitPrice'}, recursive=False)
                        if unit_price_div:
                            unit_price_span = unit_price_div.find('span')
                            if unit_price_span:
                                unit_price_text = unit_price_span.get_text(strip=True)
                                # 提取数字，格式: "12345元/m²"
                                house_unit_price = unit_price_text.replace('元/m²', '').strip()
                    
                    # ========== 数据格式化 ==========
                    # 提取面积数字
                    try:
                        area = float(house_size.replace('m²', '').strip())
                    except:
                        area = 0
                    
                    # 提取总价数字
                    try:
                        price = float(house_total_price.replace('万', '').strip())
                    except:
                        price = 0
                    
                    # 提取单价数字
                    try:
                        unit_price = float(house_unit_price)
                    except:
                        unit_price = 0
                    
                    # 只保存有效数据
                    if area > 0 and price > 0 and unit_price > 0:
                        data.append({
                            'title': house_title,
                            'price': price,
                            'area': area,
                            'unit_price': unit_price,
                            'house_type': house_type,
                            'towards': house_towards,
                            'floor': house_flood,
                            'year': house_year,
                            'building': house_building,
                            'location': house_location,
                            'house_id': house_id,
                            'source': 'lianjia'
                        })
                    
                except Exception as e:
                    logger.debug(f"解析单个房源失败: {e}")
                    continue
            
            logger.info(f"成功提取 {len(data)} 条房源")
            return data
            
        except Exception as e:
            logger.error(f"提取信息失败: {e}")
            return data
    
    def fetch_listings(self, district: str, page: int = 1) -> List[Dict]:
        """获取房源列表"""
        url = f'{self.ershoufang_url}/pg{page}l2rs{district}/'
        
        html = self.request_get(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        return self.extract_information(soup)
    
    def get_total_pages(self, district: str) -> int:
        """获取总页数"""
        url = f'{self.ershoufang_url}/l2rs{district}/'
        
        html = self.request_get(url)
        if not html:
            return 0
        
        soup = BeautifulSoup(html, 'html.parser')
        
        try:
            page_box = soup.find('div', {'class': 'page-box'})
            if page_box and page_box.find('div'):
                page_data = page_box.find('div').get('page-data')
                if page_data:
                    page_json = json.loads(page_data)
                    return page_json.get('totalPage', 1)
        except Exception as e:
            logger.warning(f"获取总页数失败: {e}")
        
        return 1
    
    def fetch_all_districts(self, pages_per_district: int = 3) -> Dict[str, List[Dict]]:
        """获取所有区的房源"""
        results = {}
        
        for district_code, district_name in self.district_map.items():
            logger.info(f"\n{'='*60}")
            logger.info(f"开始爬取 {district_name}")
            logger.info(f"{'='*60}")
            
            all_listings = []
            total_pages = self.get_total_pages(district_code)
            max_pages = min(pages_per_district, total_pages)
            
            logger.info(f"总页数: {total_pages}, 计划爬取: {max_pages} 页")
            
            for page in range(1, max_pages + 1):
                logger.info(f"爬取第 {page}/{max_pages} 页...")
                listings = self.fetch_listings(district_code, page)
                all_listings.extend(listings)
                
                if not listings:
                    logger.warning(f"第 {page} 页无数据，停止爬取")
                    break
            
            results[district_code] = all_listings
            logger.info(f"{district_name} 共爬取 {len(all_listings)} 条房源\n")
        
        return results