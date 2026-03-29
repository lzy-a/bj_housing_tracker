from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 连接到远程调试端口
options = webdriver.ChromeOptions()
options.add_experimental_option('debuggerAddress', '127.0.0.1:9222')

try:
    driver = webdriver.Chrome(options=options)
    logger.info("✅ 已连接到远程 Chrome (端口 9222)")
except Exception as e:
    logger.error(f"❌ 连接失败: {e}")
    logger.info("请先启动: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222")
    exit(1)

url = 'https://bj.lianjia.com/ershoufang/l2rs朝阳/'
logger.info(f"🌐 访问: {url}")

try:
    driver.get(url)
    logger.info("⏳ 等待页面加载（15秒）...")
    
    wait = WebDriverWait(driver, 15)
    wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'sellListContent')))
    logger.info("✅ 房源列表已加载")
    
    time.sleep(2)
    
    html = driver.page_source
    
    with open('debug_remote.html', 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info("✅ HTML 已保存到 debug_remote.html\n")
    
    soup = BeautifulSoup(html, 'html.parser')
    
    print("=" * 100)
    print("房源列表（按爬取顺序）")
    print("=" * 100)
    
    house_list = soup.find('ul', {'class': 'sellListContent'})
    if house_list:
        items = house_list.find_all('li', recursive=False)
        print(f"\n✅ 找到 {len(items)} 个房源\n")
        
        # 按顺序遍历每个房源
        for idx, house_item in enumerate(items, 1):
            try:
                info_div = house_item.find('div', {'class': 'info'}, recursive=False)
                if not info_div:
                    continue
                
                # 标题 + 房源ID
                title_div = info_div.find('div', {'class': 'title'}, recursive=False)
                title_link = title_div.find('a') if title_div else None
                title = title_link.get_text(strip=True) if title_link else "未知"
                house_code = title_link.get('data-housecode', '未知') if title_link else '未知'
                
                # 位置信息：info > flood > positionInfo > a
                position_text = "未知"
                flood_div = info_div.find('div', {'class': 'flood'}, recursive=False)
                if flood_div:
                    position_info_div = flood_div.find('div', {'class': 'positionInfo'}, recursive=False)
                    if position_info_div:
                        position_links = position_info_div.find_all('a')
                        if position_links:
                            position_text = " - ".join([a.get_text(strip=True) for a in position_links])
                
                # 地址（原始字符串）
                address_div = info_div.find('div', {'class': 'address'}, recursive=False)
                address_raw = address_div.get_text(strip=True) if address_div else "未知"
                
                # 价格
                price_info = info_div.find('div', {'class': 'priceInfo'}, recursive=False)
                total_price_div = price_info.find('div', {'class': 'totalPrice'}, recursive=False) if price_info else None
                unit_price_div = price_info.find('div', {'class': 'unitPrice'}, recursive=False) if price_info else None
                
                total_price_text = total_price_div.find('span').get_text(strip=True) if total_price_div and total_price_div.find('span') else "未知"
                unit_price_text = unit_price_div.find('span').get_text(strip=True) if unit_price_div and unit_price_div.find('span') else "未知"
                
                # 打印房源信息
                print(f"[{idx:2d}] ID:{house_code} | {title}")
                print(f"     位置: {position_text}")
                print(f"     价格: {total_price_text}  |  单价: {unit_price_text}")
                print(f"     详情: {address_raw}")
                print()
                
            except Exception as e:
                logger.debug(f"解析房源失败: {e}")
                continue
    else:
        print("❌ 未找到房源列表")

except Exception as e:
    logger.error(f"❌ 加载失败: {e}")
    import traceback
    traceback.print_exc()

finally:
    input("\n按 Enter 关闭浏览器...")
    driver.quit()
    logger.info("✅ 浏览器已关闭")