#!/usr/bin/env python3
"""
一键全量爬取：二手房 → 租房 → 社区指标
自动检测 Chrome CDP，未启动则自动拉起
"""
import subprocess
import sys
import time
import json
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


def shutdown_chrome():
    """关闭 Chrome CDP 实例，释放浏览器资源"""
    try:
        subprocess.run(
            ["pkill", "-f", f"remote-debugging-port={CHROME_PORT}"],
            timeout=5
        )
        print("✅ Chrome 浏览器已关闭，下次启动将为全新实例")
    except Exception:
        pass


def run_step(name, script, *args):
    """运行一个爬虫步骤，返回 (success, stats_dict)"""
    print(f"\n{'=' * 60}")
    print(f"🚀 {name}")
    print(f"{'=' * 60}")
    cmd = [sys.executable, script] + list(args)

    stats = {}
    with subprocess.Popen(cmd, cwd=sys.path[0] or ".", stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
        for line in p.stdout:
            # 结构化数据行不打印，留给 run_all 整合
            if line.startswith('__STATS__'):
                try:
                    stats = json.loads(line[len('__STATS__'):])
                except json.JSONDecodeError:
                    pass
            else:
                sys.stdout.write(line)
        p.wait()

    if p.returncode != 0:
        print(f"❌ {name} 失败 (exit={p.returncode})")
        return False, {}

    print(f"✅ {name} 完成")
    return True, stats


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

    total_start = time.time()
    for name, script in scripts:
        t0 = time.time()
        ok, stats = run_step(name, script, *region_args)
        if not ok:
            shutdown_chrome()
            sys.exit(1)
        elapsed = time.time() - t0
        count = stats.get('count', '?')
        avg_ms = stats.get('avg_ms', None)
        speed_info = f"{count}条"
        if avg_ms:
            speed_info += f", 均{avg_ms:.0f}ms/条"
        print(f"⏱️  {name}: {elapsed:.0f}s | {speed_info}")

    shutdown_chrome()

    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 60}")
    print(f"🎉 全部完成！总耗时 {total_elapsed:.0f}s（{total_elapsed/60:.1f}min）")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
