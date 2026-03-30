import logging
import time
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LianjiaScraperBase:
    """链家数据爬虫 - 使用远程 Chrome 调试端口"""
    
    def __init__(self, debug_port: int = 9222, delay: float = 2.0):
        """
        初始化爬虫
        debug_port: Chrome 远程调试端口
        delay: 请求间隔（秒）
        """
        self.base_url = 'https://bj.lianjia.com'
        self.delay = delay
        
        # 连接到远程调试端口
        options = webdriver.ChromeOptions()
        options.add_experimental_option('debuggerAddress', f'127.0.0.1:{debug_port}')
        
        try:
            self.driver = webdriver.Chrome(options=options)
            logger.info(f"✅ 已连接到远程 Chrome (端口: {debug_port})")
        except Exception as e:
            logger.error(f"❌ 连接失败: {e}")
            logger.info("请先启动: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222")
            raise
    
    def fetch_listings(self, district: str, page: int = 1) -> List[Dict]:
        """获取房源列表"""
        url = f'{self.base_url}/ershoufang/pg{page}l2rs{district}/'
        
        try:
            logger.info(f"🌐 加载: {url}")
            self.driver.get(url)
            
            # 等待房源列表加载
            wait = WebDriverWait(self.driver, 15)
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'sellListContent')))
            
            logger.info(f"✅ 页面加载完成")
            time.sleep(self.delay)
            
            # 获取 HTML
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # 提取数据
            data = self.extract_information(soup)
            return data
            
        except Exception as e:
            logger.error(f"❌ 加载失败: {e}")
            return []
    
    def extract_information(self, soup: BeautifulSoup) -> List[Dict]:
        """提取房源信息"""
        data = []
        
        try:
            house_list = soup.find('ul', {'class': 'sellListContent'})
            if not house_list:
                logger.warning("��� 未找到房源列表")
                return data
            
            items = house_list.find_all('li', recursive=False)
            logger.info(f"📦 找到 {len(items)} 个房源")
            
            for house_item in items:
                try:
                    info_div = house_item.find('div', {'class': 'info'}, recursive=False)
                    if not info_div:
                        continue
                    
                    # ===== 标题 + 房源ID =====
                    title_div = info_div.find('div', {'class': 'title'}, recursive=False)
                    if not title_div:
                        continue
                    title_link = title_div.find('a')
                    if not title_link:
                        continue
                    
                    title = title_link.get_text(strip=True)
                    house_code = title_link.get('data-housecode', '')
                    
                    # ===== 位置信息：info > flood > positionInfo > a =====
                    position_text = '未知'
                    flood_div = info_div.find('div', {'class': 'flood'}, recursive=False)
                    if flood_div:
                        position_info_div = flood_div.find('div', {'class': 'positionInfo'}, recursive=False)
                        if position_info_div:
                            position_links = position_info_div.find_all('a')
                            if position_links:
                                position_text = ' - '.join([a.get_text(strip=True) for a in position_links])
                    
                    # ===== 地址信息（原始字符串）=====
                    address_div = info_div.find('div', {'class': 'address'}, recursive=False)
                    if not address_div:
                        continue
                    
                    address_raw = address_div.get_text(strip=True)
                    
                    # ===== 价格信息 =====
                    price_info = info_div.find('div', {'class': 'priceInfo'}, recursive=False)
                    if not price_info:
                        continue
                    
                    total_price_div = price_info.find('div', {'class': 'totalPrice'}, recursive=False)
                    unit_price_div = price_info.find('div', {'class': 'unitPrice'}, recursive=False)
                    
                    if not (total_price_div and unit_price_div):
                        continue
                    
                    total_price_span = total_price_div.find('span')
                    unit_price_span = unit_price_div.find('span')
                    
                    if not (total_price_span and unit_price_span):
                        continue
                    
                    total_price_text = total_price_span.get_text(strip=True)
                    unit_price_text = unit_price_span.get_text(strip=True)
                    
                    # 提取数字
                    try:
                        price = float(total_price_text.replace('万', '').strip())
                    except:
                        price = 0
                    
                    try:
                        unit_price = float(unit_price_text.replace('元/m²', '').strip())
                    except:
                        unit_price = 0
                    
                    # 提取面积（从地址原始字符串中）
                    import re
                    area = 0
                    area_match = re.search(r'(\d+\.?\d*)m²', address_raw)
                    if area_match:
                        area = float(area_match.group(1))
                    
                    # 只保存有效数据
                    if area > 0 and price > 0 and unit_price > 0:
                        data.append({
                            'house_code': house_code,          # 房源ID
                            'title': title,                    # 房源标题
                            'position': position_text,         # 位置信息
                            'price': price,                    # 总价（万）
                            'area': area,                      # 面积（平米）
                            'unit_price': unit_price,          # 单价（元/m²）
                            'address_raw': address_raw,        # 原始地址字符串
                            'source': 'lianjia'
                        })
                
                except Exception as e:
                    logger.debug(f"解析房源失败: {e}")
                    continue
            
            logger.info(f"✅ 提取 {len(data)} 条有效房源")
            return data
            
        except Exception as e:
            logger.error(f"提取失败: {e}")
            return data
    
    def fetch_all_districts(self, pages_per_district: int = 2) -> Dict[str, List[Dict]]:
        """获取所有区的房源"""
        districts_cn = {
            '朝阳': '朝阳区',
            '东城': '东城区',
            '西城': '西城区',
            '海淀': '海淀区',
            '丰台': '丰台区',
            '石景山': '石景山区',
            '大兴': '大兴区',
            '通州': '通州区',
        }
        
        results = {}
        
        try:
            for district_code, district_name in districts_cn.items():
                logger.info(f"\n{'='*60}")
                logger.info(f"爬取 {district_name}")
                logger.info(f"{'='*60}")
                
                all_listings = []
                
                for page in range(1, pages_per_district + 1):
                    logger.info(f"第 {page}/{pages_per_district} 页...")
                    listings = self.fetch_listings(district_code, page)
                    all_listings.extend(listings)
                    
                    if not listings:
                        logger.warning(f"第 {page} 页无数据，停止")
                        break
                
                results[district_code] = all_listings
                logger.info(f"{district_name} 共 {len(all_listings)} 条房源\n")
        
        except Exception as e:
            logger.error(f"爬取失败: {e}")
        
        return results
    
    def __del__(self):
        """析构函数"""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
        except:
            pass