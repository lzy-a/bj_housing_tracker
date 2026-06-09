"""
邮件通知 — 发现高分房源时发送 Gmail 通知。
"""
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    GMAIL_SMTP_SERVER, GMAIL_SMTP_PORT,
    GMAIL_SENDER, GMAIL_PASSWORD, GMAIL_RECIPIENT,
    FINDER_CONFIG,
)

logger = logging.getLogger(__name__)


class EmailNotifier:
    def __init__(self, smtp_server=None, smtp_port=None,
                 sender=None, password=None, recipient=None):
        self.smtp_server = smtp_server or GMAIL_SMTP_SERVER
        self.smtp_port = smtp_port or GMAIL_SMTP_PORT
        self.sender = sender or GMAIL_SENDER
        self.password = password or GMAIL_PASSWORD
        self.recipient = recipient or GMAIL_RECIPIENT

    def is_configured(self) -> bool:
        return all([self.sender, self.password, self.recipient])

    def send_alert(self, listings: list) -> bool:
        """发送高分房源通知邮件。"""
        if not self.is_configured():
            logger.warning("邮件未配置，跳过发送")
            return False
        if not listings:
            return False

        subject = f"🏠 发现 {len(listings)} 套高分租房源"
        html_body = self._build_html(listings)

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.sender
        msg['To'] = self.recipient
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.recipient, msg.as_string())
            logger.info(f"邮件已发送: {len(listings)} 套房源")
            return True
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False

    def _build_html(self, listings: list) -> str:
        threshold = FINDER_CONFIG.get('alert_score_threshold', 7)
        rows = ''
        for item in listings:
            scores = item.get('scores', {})
            score_lines = ''.join(
                f'<span style="margin-right:12px">{k}: <b>{v}</b>/10</span>'
                for k, v in scores.items()
            )
            rows += f'''
            <tr>
                <td style="padding:8px;border-bottom:1px solid #eee">
                    <b>{item.get('community', '')}</b><br>
                    <span style="color:#666">{item.get('title', '')}</span>
                </td>
                <td style="padding:8px;border-bottom:1px solid #eee">
                    {item.get('layout', '')}<br>
                    {item.get('area', '')}㎡ | {item.get('rent_type', '')}
                </td>
                <td style="padding:8px;border-bottom:1px solid #eee;font-size:16px;color:#e74c3c">
                    ¥{item.get('rent_price', '')}/月
                </td>
                <td style="padding:8px;border-bottom:1px solid #eee;font-size:12px">
                    {score_lines}<br>
                    <span style="color:#666">{item.get('llm_summary', '')[:100]}</span>
                </td>
                <td style="padding:8px;border-bottom:1px solid #eee">
                    <a href="https://bj.5i5j.com/zufang/{item.get('house_id', '')}.html">查看</a>
                </td>
            </tr>'''

        return f'''
        <html><body style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto;padding:20px">
        <h2 style="color:#2c3e50">🏠 租房找房器 — 高分房源通知</h2>
        <p>发现以下房源评分达到 {threshold}+ 分：</p>
        <table style="width:100%;border-collapse:collapse">
        <thead><tr style="background:#f8f9fa">
            <th style="padding:8px;text-align:left">小区</th>
            <th style="padding:8px;text-align:left">户型</th>
            <th style="padding:8px;text-align:left">租金</th>
            <th style="padding:8px;text-align:left">评分</th>
            <th style="padding:8px;text-align:left">链接</th>
        </tr></thead>
        <tbody>{rows}</tbody>
        </table>
        <p style="color:#999;font-size:12px;margin-top:20px">
            此邮件由租房找房器自动发送
        </p>
        </body></html>'''
