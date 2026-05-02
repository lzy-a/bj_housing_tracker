#!/usr/bin/env python3
"""
一键全量爬取：二手房 → 租房 → 社区指标
自动检测 Chrome CDP，未启动则自动拉起
"""
import subprocess
import sys
import time
import requests
import argparse


CHROME_PORT = 9223
CHROME_USER_DIR = "/tmp/chrome_9223"
CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def ensure_chrome():
    """确保 Chrome CDP 已启动，没有则自动拉起"""
    try:
        r = requests.get(f"http://localhost:{CHROME_PORT}/json/version", timeout=3)
        r.raise_for_status()
        ws = r.json().get("webSocketDebuggerUrl", "")
        print(f"✅ Chrome CDP 已在运行: {ws[:60]}...")
        return
    except Exception:
        print("⏳ Chrome CDP 未启动，正在拉起...")

    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={CHROME_PORT}",
        f"--user-data-dir={CHROME_USER_DIR}",
        "--blink-settings=imagesEnabled=false",
    ]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)

    # 确认启动成功
    for i in range(10):
        try:
            r = requests.get(f"http://localhost:{CHROME_PORT}/json/version", timeout=3)
            if r.ok:
                print("✅ Chrome CDP 已启动")
                return
        except Exception:
            pass
        time.sleep(1)

    print("❌ Chrome CDP 启动失败，请手动检查")
    sys.exit(1)


def run_step(name, script, *args):
    """运行一个爬虫步骤"""
    print(f"\n{'=' * 60}")
    print(f"🚀 {name}")
    print(f"{'=' * 60}")
    cmd = [sys.executable, script] + list(args)
    result = subprocess.run(cmd, cwd=sys.path[0] or ".")
    if result.returncode != 0:
        print(f"❌ {name} 失败 (exit={result.returncode})")
        return False
    print(f"✅ {name} 完成")
    return True


def main():
    parser = argparse.ArgumentParser(description="一键全量爬取：二手房 + 租房")
    parser.add_argument("-r", "--region", type=int, nargs="+",
                        help="区域编号: 0=东城 1=西城 2=海淀 3=朝阳 4=丰台 5=石景山")
    parser.add_argument("--sale-only", action="store_true", help="只跑二手房")
    parser.add_argument("--rent-only", action="store_true", help="只跑租房")
    args = parser.parse_args()

    region_args = []
    if args.region:
        region_args = ["-r"] + [str(r) for r in args.region]

    ensure_chrome()

    scripts = []
    if not args.rent_only:
        scripts.append(("二手房", "run_crawler_playwright.py"))
    if not args.sale_only:
        scripts.append(("租房", "run_crawler_rent.py"))

    for name, script in scripts:
        if not run_step(name, script, *region_args):
            sys.exit(1)

    print(f"\n{'=' * 60}")
    print("🎉 全部完成！二手房 + 租房数据已入库，社区指标已更新")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
