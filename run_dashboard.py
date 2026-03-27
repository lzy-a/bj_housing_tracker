"""
看板启动脚本
"""
import subprocess
import sys
from pathlib import Path

def main():
    """启动Streamlit看板"""
    dashboard_path = Path(__file__).parent / 'dashboard' / 'app.py'
    
    cmd = [
        sys.executable,
        '-m',
        'streamlit',
        'run',
        str(dashboard_path),
        '--logger.level=info',
        '--client.showErrorDetails=true'
    ]
    
    subprocess.run(cmd)

if __name__ == '__main__':
    main()