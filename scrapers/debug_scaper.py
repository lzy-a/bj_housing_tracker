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
    logger.info("✅ HTML 已保存到 debug_remote.html")
    
    soup = BeautifulSoup(html, 'html.parser')
    
    print("\n" + "=" * 60)
    print("1️⃣  检查房源列表")
    print("=" * 60)
    house_list = soup.find('ul', {'class': 'sellListContent'})
    if house_list:
        items = house_list.find_all('li', recursive=False)
        print(f"✅ 找到 {len(items)} 个房源\n")
    else:
        print("❌ 未找到房源列表\n")
    
    print("=" * 60)
    print("2️⃣  检查标题")
    print("=" * 60)
    titles = soup.find_all('div', {'class': 'title'}, limit=5)
    if titles:
        print(f"✅ 找到 {len(titles)} 个标题")
        for i, t in enumerate(titles[:3]):
            print(f"  {i}: {t.get_text(strip=True)[:50]}")
        print()
    else:
        print("❌ ���找到标题\n")
    
    print("=" * 60)
    print("3️⃣  检查价格")
    print("=" * 60)
    prices = soup.find_all('div', {'class': 'totalPrice'}, limit=5)
    if prices:
        print(f"✅ 找到 {len(prices)} 个价格")
        for i, p in enumerate(prices[:3]):
            print(f"  {i}: {p.get_text(strip=True)}")
        print()
    else:
        print("❌ 未找到价格\n")
    
    print("=" * 60)
    print("4️⃣  检查地址")
    print("=" * 60)
    addresses = soup.find_all('div', {'class': 'address'}, limit=5)
    if addresses:
        print(f"✅ 找到 {len(addresses)} 个地址")
        for i, a in enumerate(addresses[:3]):
            print(f"  {i}: {a.get_text(strip=True)[:60]}")
        print()
    else:
        print("❌ 未找到地址\n")

except Exception as e:
    logger.error(f"❌ 加载失败: {e}")
    import traceback
    traceback.print_exc()