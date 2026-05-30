#!/usr/bin/env python3
"""
一键全量爬取：二手房 → 租房 → 社区指标
自动检测 Chrome CDP，未启动则自动拉起
"""
import os
import shutil
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
    """确保 Chrome CDP 已启动，每次强制重启干净实例"""
    # 先杀旧进程，并等待其真正退出
    try:
        subprocess.run(
            ["pkill", "-f", f"remote-debugging-port={CHROME_PORT}"],
            timeout=5
        )
        # 等待进程真正退出（最多等 5 秒），否则后续文件清理可能被覆盖
        for _ in range(10):
            result = subprocess.run(
                ["pgrep", "-f", f"remote-debugging-port={CHROME_PORT}"],
                capture_output=True, timeout=3
            )
            if result.returncode != 0:
                break
            time.sleep(0.5)
        print("🔄 已关闭旧 Chrome 实例")
    except Exception:
        pass

    # 清理残留锁文件，避免 "Chrome 未正确关闭" 提示
    for lock in ["SingletonLock", "SingletonSocket", "SingletonCookie"]:
        lp = os.path.join(CHROME_USER_DIR, lock)
        if os.path.exists(lp):
            try:
                os.remove(lp)
            except OSError:
                pass

    # 清理会话恢复文件，防止上次异常退出的 tab 被恢复
    default_dir = os.path.join(CHROME_USER_DIR, "Default")
    for sf in ["Current Session", "Current Tabs", "Last Session", "Last Tabs"]:
        fp = os.path.join(default_dir, sf)
        if os.path.exists(fp):
            try:
                os.remove(fp)
            except OSError:
                pass
    # 也清理旧会话目录
    sessions_dir = os.path.join(default_dir, "Sessions")
    if os.path.exists(sessions_dir):
        try:
            shutil.rmtree(sessions_dir, ignore_errors=True)
        except Exception:
            pass

    # 清理缓存目录，控制 profile 体积（保留 Cookies 不动）
    for cd in ["Cache", "Code Cache", "GPUCache", "Service Worker",
               "DawnCache", "ShaderCache", "GrShaderCache"]:
        cp = os.path.join(default_dir, cd)
        if os.path.exists(cp):
            try:
                shutil.rmtree(cp, ignore_errors=True)
            except Exception:
                pass

    # 写入/创建 Preferences：抑制恢复提示 + 关闭会话恢复
    os.makedirs(default_dir, exist_ok=True)
    prefs_path = os.path.join(default_dir, "Preferences")
    if os.path.exists(prefs_path):
        try:
            with open(prefs_path, "r") as f:
                prefs = json.load(f)
        except Exception:
            prefs = {}
    else:
        prefs = {}
    try:
        prefs["profile"] = prefs.get("profile", {})
        prefs["profile"]["exit_type"] = "Normal"
        prefs["profile"]["exited_cleanly"] = True
        # 启动时打开新标签页，不要恢复上次会话
        prefs["session"] = prefs.get("session", {})
        prefs["session"]["restore_on_startup"] = 0
        with open(prefs_path, "w") as f:
            json.dump(prefs, f, indent=2)
    except Exception:
        pass

    print("⏳ 正在拉起 Chrome...")
    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={CHROME_PORT}",
        f"--user-data-dir={CHROME_USER_DIR}",
        "--blink-settings=imagesEnabled=false",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-session-crashed-bubble",
        "--disable-infobars",
        "--disable-features=ChromeWhatsNewUI",
        "--disk-cache-size=5242880",
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
    parser.add_argument("--no-analyst", action="store_true", help="跳过 AI 分析师")
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
    summaries = []
    for name, script in scripts:
        t0 = time.time()
        ok, stats = run_step(name, script, *region_args)
        if not ok:
            shutdown_chrome()
            sys.exit(1)
        elapsed = time.time() - t0
        count = stats.get('count', '?')
        avg_ms = stats.get('avg_ms')
        w = sum(2 if ord(c) > 127 else 1 for c in name)
        name_pad = name + ' ' * (6 - w)  # CJK 占 2 列，补到等宽
        if avg_ms:
            summaries.append(f"  {name_pad}: {elapsed:.0f}s | {count}条 | 均{avg_ms:.1f}ms/条")
        else:
            summaries.append(f"  {name_pad}: {elapsed:.0f}s | {count}条")

    shutdown_chrome()

    # AI 分析师
    if not args.no_analyst:
        print(f"\n{'=' * 60}")
        print(f"🤖 启动 AI 分析师...")
        print(f"{'=' * 60}")
        result = subprocess.run(
            [sys.executable, "run_analyst.py"],
            cwd=sys.path[0] or ".",
        )
        if result.returncode != 0:
            print(f"⚠️ AI 分析师异常退出 (exit={result.returncode})，数据爬取正常")

    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 60}")
    for s in summaries:
        print(s)
    print(f"🎉 总耗时 {total_elapsed:.0f}s（{total_elapsed/60:.1f}min）")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
