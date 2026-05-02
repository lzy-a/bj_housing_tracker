import os

folders = [
    'data/raw', 'data/db', 'config', 'scrapers', 'etl', 'dashboard'
]

files = [
    'config/__init__.py', 'scrapers/__init__.py', 'etl/__init__.py',
    'requirements.txt', 'README.md'
]

for folder in folders:
    os.makedirs(folder, exist_ok=True)

for file in files:
    with open(file, 'a'):
        os.utime(file, None)

print("✅ 项目目录结构初始化完成！")