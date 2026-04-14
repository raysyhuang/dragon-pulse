"""
Real-Time Alerts Module

Send notifications when high-conviction signals are detected.
Supports Slack, Discord, Email, and Telegram.
"""

from __future__ import annotations
import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional, Callable
from dataclasses import dataclass
import time as _time
import re
import requests

# Load environment variables from .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


# ─── Formatting Helpers ────────────────────────────

def _bar(value: float, max_val: float = 100, width: int = 10) -> str:
    """Unicode progress bar."""
    ratio = max(0, min(1, value / max_val))
    filled = round(ratio * width)
    return "\u2588" * filled + "\u2591" * (width - filled)


def _regime_emoji(regime: str) -> str:
    return {"bull": "\U0001f7e2", "bear": "\U0001f534", "choppy": "\U0001f7e1", "caution": "\U0001f7e1"}.get(
        (regime or "").lower(), "\u26aa"
    )


def _regime_cn(regime: str) -> str:
    """Translate regime key to Chinese display label."""
    return {"bull": "牛市", "bear": "熊市", "choppy": "震荡", "caution": "震荡"}.get(
        (regime or "").lower(), "未知"
    )


def _section_line() -> str:
    return "\u2500" * 28


def _starts_with_title_block(message: str, title: str) -> bool:
    """Detect a leading title even when it is wrapped in simple HTML tags."""
    first_line = (message or "").lstrip().split("\n", 1)[0].strip()
    if not first_line:
        return False
    normalized = re.sub(r"</?[^>]+>", "", first_line).strip()
    return normalized == title.strip()

from src.utils.time import utc_now


def _forward_to_mas_log(source: str, message: str, chat_id: str | None = None) -> None:
    """Best-effort forward of a sent Telegram message to MAS central log."""
    mas_url = os.environ.get("MAS_TELEGRAM_LOG_URL")
    mas_key = os.environ.get("MAS_API_SECRET_KEY")
    if not mas_url or not mas_key:
        return
    try:
        requests.post(
            mas_url,
            json={"source": source, "message": message, "chat_id": chat_id},
            headers={"Authorization": f"Bearer {mas_key}"},
            timeout=5,
        )
    except Exception:
        pass  # best-effort, never fail the main flow


@dataclass
class AlertConfig:
    """Configuration for alerts."""
    enabled: bool = False
    channels: list[str] = None  # ["email", "telegram", "desktop", "file", "slack", "discord"]
    
    # Slack
    slack_webhook: Optional[str] = None
    
    # Discord
    discord_webhook: Optional[str] = None
    
    # Email
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    from_address: Optional[str] = None
    to_addresses: list[str] = None
    
    # Telegram
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    
    # Desktop notifications (macOS/Windows/Linux)
    desktop_sound: bool = True
    
    # File-based alerts (local log)
    alert_log_path: str = "outputs/alerts.log"
    
    # Triggers
    trigger_all_three_overlap: bool = True
    trigger_weekly_pro30_overlap: bool = True
    trigger_high_composite_score: float = 7.0
    
    def __post_init__(self):
        if self.channels is None:
            self.channels = []
        if self.to_addresses is None:
            self.to_addresses = []
        
        # Load from environment variables
        self.slack_webhook = self.slack_webhook or os.environ.get("SLACK_WEBHOOK_URL")
        self.discord_webhook = self.discord_webhook or os.environ.get("DISCORD_WEBHOOK_URL")
        self.smtp_user = self.smtp_user or os.environ.get("SMTP_USER")
        self.smtp_password = self.smtp_password or os.environ.get("SMTP_PASSWORD")
        self.telegram_bot_token = self.telegram_bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = self.telegram_chat_id or os.environ.get("TELEGRAM_CHAT_ID")


class AlertManager:
    """Manages sending alerts across multiple channels."""
    
    def __init__(self, config: Optional[AlertConfig] = None):
        self.config = config or AlertConfig()
        self._handlers: dict[str, Callable] = {
            "slack": self._send_slack,
            "discord": self._send_discord,
            "email": self._send_email,
            "telegram": self._send_telegram,
            "desktop": self._send_desktop,
            "file": self._send_file,
        }
    
    def send_alert(
        self,
        title: str,
        message: str,
        data: Optional[dict] = None,
        channels: Optional[list[str]] = None,
        priority: str = "normal"
    ) -> dict[str, bool]:
        """
        Send alert to configured channels.
        
        Args:
            title: Alert title
            message: Alert message
            data: Additional structured data
            channels: Override default channels
            priority: "low", "normal", "high"
        
        Returns:
            Dict mapping channel -> success status
        """
        if not self.config.enabled:
            logger.debug("Alerts disabled, skipping")
            return {}
        
        channels = channels or self.config.channels
        results = {}
        
        for channel in channels:
            handler = self._handlers.get(channel)
            if handler:
                try:
                    success = handler(title, message, data, priority)
                    results[channel] = success
                except Exception as e:
                    logger.error(f"Failed to send {channel} alert: {e}")
                    results[channel] = False
            else:
                logger.warning(f"Unknown alert channel: {channel}")
                results[channel] = False
        
        return results
    
    def _send_slack(
        self,
        title: str,
        message: str,
        data: Optional[dict],
        priority: str
    ) -> bool:
        """Send alert to Slack webhook."""
        webhook_url = self.config.slack_webhook
        if not webhook_url:
            logger.warning("Slack webhook not configured")
            return False
        
        # Build Slack blocks
        emoji = {"low": "📊", "normal": "📈", "high": "🚨"}.get(priority, "📈")
        
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} {title}"}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": message}
            }
        ]
        
        if data:
            # Add data fields
            fields = []
            for key, value in list(data.items())[:10]:  # Limit fields
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*{key}:*\n{value}"
                })
            
            if fields:
                blocks.append({
                    "type": "section",
                    "fields": fields[:10]  # Slack limit
                })
        
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"Sent at {utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC"
            }]
        })
        
        payload = {"blocks": blocks}
        
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        return response.status_code == 200
    
    def _send_discord(
        self,
        title: str,
        message: str,
        data: Optional[dict],
        priority: str
    ) -> bool:
        """Send alert to Discord webhook."""
        webhook_url = self.config.discord_webhook
        if not webhook_url:
            logger.warning("Discord webhook not configured")
            return False
        
        # Build Discord embed
        color = {"low": 0x808080, "normal": 0x00FF00, "high": 0xFF0000}.get(priority, 0x00FF00)
        
        embed = {
            "title": title,
            "description": message,
            "color": color,
            "timestamp": utc_now().isoformat().replace("+00:00", ""),
            "footer": {"text": "Momentum Scanner"}
        }
        
        if data:
            embed["fields"] = [
                {"name": k, "value": str(v)[:1024], "inline": True}
                for k, v in list(data.items())[:25]  # Discord limit
            ]
        
        payload = {"embeds": [embed]}
        
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        return response.status_code == 204
    
    def _send_email(
        self,
        title: str,
        message: str,
        data: Optional[dict],
        priority: str
    ) -> bool:
        """Send alert via email."""
        if not all([
            self.config.smtp_user,
            self.config.smtp_password,
            self.config.from_address,
            self.config.to_addresses
        ]):
            logger.warning("Email not fully configured")
            return False
        
        # Build email
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[{priority.upper()}] {title}"
        msg["From"] = self.config.from_address
        msg["To"] = ", ".join(self.config.to_addresses)
        
        # Plain text version
        text_content = f"{title}\n\n{message}"
        if data:
            text_content += "\n\nDetails:\n"
            for k, v in data.items():
                text_content += f"  {k}: {v}\n"
        
        # HTML version
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <h2 style="color: {'#FF0000' if priority == 'high' else '#333'};">{title}</h2>
            <p style="font-size: 14px; line-height: 1.6;">{message.replace(chr(10), '<br>')}</p>
        """
        
        if data:
            html_content += """
            <table style="border-collapse: collapse; margin-top: 20px;">
                <tr><th style="text-align: left; padding: 8px; background: #f0f0f0;">Field</th>
                    <th style="text-align: left; padding: 8px; background: #f0f0f0;">Value</th></tr>
            """
            for k, v in data.items():
                html_content += f"""
                <tr><td style="padding: 8px; border-bottom: 1px solid #ddd;">{k}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{v}</td></tr>
                """
            html_content += "</table>"
        
        html_content += f"""
            <p style="font-size: 12px; color: #888; margin-top: 30px;">
                Sent at {utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC
            </p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))
        
        try:
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls()
                server.login(self.config.smtp_user, self.config.smtp_password)
                server.send_message(msg)
            return True
        except Exception as e:
            logger.error(f"Email send failed: {e}")
            return False
    
    def _send_telegram_message(self, token: str, chat_id: str, text: str) -> bool:
        """Send a single Telegram message with HTML parse mode and exponential backoff retry."""
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        for attempt in range(3):
            try:
                response = requests.post(url, json=payload, timeout=10)
                if response.status_code == 200:
                    return True

                # If HTML parse fails, retry without parse_mode on first attempt
                if attempt == 0 and response.status_code == 400:
                    payload.pop("parse_mode", None)
                    response = requests.post(url, json=payload, timeout=10)
                    if response.status_code == 200:
                        return True

                try:
                    err = response.json().get("description", "Unknown")
                except Exception:
                    err = response.text[:200]
                logger.warning(f"Telegram attempt {attempt + 1} failed ({response.status_code}): {err}")

            except requests.exceptions.RequestException as e:
                logger.warning(f"Telegram attempt {attempt + 1} error: {e}")

            if attempt < 2:
                _time.sleep(2 ** attempt)  # 1s, 2s

        logger.error("Telegram send failed after 3 attempts")
        return False

    def _send_telegram(
        self,
        title: str,
        message: str,
        data: Optional[dict],
        priority: str
    ) -> bool:
        """
        Send alert to Telegram with HTML formatting.

        Note: Retries are compute-only - no side effects emitted on retry attempts.
        """
        from pathlib import Path
        from src.core.retry_guard import is_retry_attempt, log_retry_suppression

        token = self.config.telegram_bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = self.config.telegram_chat_id or os.environ.get("TELEGRAM_CHAT_ID")

        missing = []
        if not token:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not chat_id:
            missing.append("TELEGRAM_CHAT_ID")
        if missing:
            logger.warning(f"Telegram not configured (missing {', '.join(missing)})")
            return False

        run_id = os.environ.get("GITHUB_RUN_ID", "N/A")
        run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")

        # Retries re-run computation but MUST NOT emit side effects
        if is_retry_attempt():
            log_retry_suppression("Telegram alert", run_id=run_id, title=title)
            return True

        # Extract asof date from data or title if available
        asof_date = data.get("asof") if data and "asof" in data else None
        if not asof_date and data and "Date" in data:
            asof_date = data["Date"]

        # Check for duplicate send marker file
        if asof_date and run_id != "N/A":
            outputs_dir = Path("outputs") / asof_date
            marker_file = outputs_dir / f".telegram_sent_{run_id}_{run_attempt}.txt"
            if marker_file.exists():
                logger.info(f"Telegram alert already sent for run_id={run_id}, attempt={run_attempt}. Skipping.")
                return True

        # Avoid repeating the same header when the formatted body already includes it.
        text = message if _starts_with_title_block(message, title) else f"{title}\n\n{message}"

        logger.info(
            "Telegram config: token_present=%s chat_id=%s run_id=%s attempt=%s",
            bool(token), chat_id, run_id, run_attempt,
        )

        # Split long messages (Telegram limit: 4096 chars)
        MAX_LEN = 4000
        chunks = []
        if len(text) <= MAX_LEN:
            chunks = [text]
        else:
            lines = text.split("\n")
            chunk = ""
            for line in lines:
                if len(chunk) + len(line) + 1 > MAX_LEN:
                    chunks.append(chunk)
                    chunk = line
                else:
                    chunk = f"{chunk}\n{line}" if chunk else line
            if chunk:
                chunks.append(chunk)

        success = True
        for chunk in chunks:
            if not self._send_telegram_message(token, chat_id, chunk):
                success = False

        if success:
            logger.info(f"Telegram alert sent: {title} (run_id={run_id})")
            # Forward to MAS telegram log (best-effort)
            _forward_to_mas_log("dragon_pulse", text, chat_id)
            # Write marker file
            if asof_date and run_id != "N/A":
                outputs_dir = Path("outputs") / asof_date
                outputs_dir.mkdir(parents=True, exist_ok=True)
                marker_file = outputs_dir / f".telegram_sent_{run_id}_{run_attempt}.txt"
                try:
                    with open(marker_file, "w") as f:
                        f.write(f"Title: {title}\nRun ID: {run_id}\nAttempt: {run_attempt}\n")
                    logger.info(f"Created marker file: {marker_file}")
                except Exception as e:
                    logger.warning(f"Failed to create marker file: {e}")

        return success
    
    def _send_desktop(
        self,
        title: str,
        message: str,
        data: Optional[dict],
        priority: str
    ) -> bool:
        """Send desktop notification (macOS/Windows/Linux)."""
        import platform
        system = platform.system()
        
        try:
            if system == "Darwin":  # macOS
                # Use osascript for native macOS notifications
                import subprocess
                
                # Escape quotes in message
                safe_title = title.replace('"', '\\"')
                safe_message = message.replace('"', '\\"').replace('\n', ' ')
                
                script = f'display notification "{safe_message}" with title "{safe_title}"'
                if self.config.desktop_sound:
                    script += ' sound name "Glass"'
                
                subprocess.run(
                    ["osascript", "-e", script],
                    capture_output=True,
                    timeout=5
                )
                return True
                
            elif system == "Windows":
                # Try Windows toast notifications
                try:
                    from win10toast import ToastNotifier
                    toaster = ToastNotifier()
                    toaster.show_toast(title, message, duration=10, threaded=True)
                    return True
                except ImportError:
                    # Fallback to basic Windows notification
                    import ctypes
                    ctypes.windll.user32.MessageBoxW(0, message, title, 0x40)
                    return True
                    
            elif system == "Linux":
                # Use notify-send on Linux
                import subprocess
                subprocess.run(
                    ["notify-send", title, message],
                    capture_output=True,
                    timeout=5
                )
                return True
                
        except Exception as e:
            logger.error(f"Desktop notification failed: {e}")
            return False
        
        return False
    
    def _send_file(
        self,
        title: str,
        message: str,
        data: Optional[dict],
        priority: str
    ) -> bool:
        """Write alert to local log file."""
        try:
            from pathlib import Path
            
            log_path = Path(self.config.alert_log_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
            
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"[{timestamp}] [{priority.upper()}] {title}\n")
                f.write(f"{'='*60}\n")
                f.write(f"{message}\n")
                
                if data:
                    f.write("\nDetails:\n")
                    for k, v in data.items():
                        f.write(f"  {k}: {v}\n")
                
                f.write("\n")
            
            return True
            
        except Exception as e:
            logger.error(f"File alert failed: {e}")
            return False


# Convenience functions

def send_overlap_alert(
    overlap_type: str,
    tickers: list[str],
    date_str: str,
    config: Optional[AlertConfig] = None
) -> dict[str, bool]:
    """Send alert for overlap detection."""
    manager = AlertManager(config)
    
    if overlap_type == "all_three" and not manager.config.trigger_all_three_overlap:
        return {}
    if overlap_type == "weekly_pro30" and not manager.config.trigger_weekly_pro30_overlap:
        return {}
    
    emoji_map = {
        "all_three": "⭐",
        "weekly_pro30": "🔥",
        "weekly_movers": "📈",
        "pro30_movers": "💎"
    }
    
    title_map = {
        "all_three": "ALL THREE Overlap Detected!",
        "weekly_pro30": "Weekly + Pro30 Overlap",
        "weekly_movers": "Weekly + Movers Overlap",
        "pro30_movers": "Pro30 + Movers Overlap"
    }
    
    emoji = emoji_map.get(overlap_type, "📊")
    title = title_map.get(overlap_type, f"{overlap_type} Overlap")
    
    message = f"{emoji} {len(tickers)} ticker(s) found in {overlap_type.replace('_', ' ')} overlap:\n"
    message += ", ".join(tickers[:10])
    if len(tickers) > 10:
        message += f" (+{len(tickers) - 10} more)"
    
    priority = "high" if overlap_type == "all_three" else "normal"
    
    return manager.send_alert(
        title=title,
        message=message,
        data={
            "asof": date_str,
            "Date": date_str,
            "Overlap Type": overlap_type,
            "Ticker Count": len(tickers),
            "Tickers": ", ".join(tickers)
        },
        priority=priority
    )


def send_high_score_alert(
    ticker: str,
    score: float,
    rank: int,
    date_str: str,
    config: Optional[AlertConfig] = None
) -> dict[str, bool]:
    """Send alert for high composite score."""
    manager = AlertManager(config)
    
    if score < manager.config.trigger_high_composite_score:
        return {}
    
    return manager.send_alert(
        title=f"High Score Alert: {ticker}",
        message=f"🏆 {ticker} ranked #{rank} with composite score {score:.2f}",
        data={
            "asof": date_str,
            "Date": date_str,
            "Ticker": ticker,
            "Rank": rank,
            "Composite Score": f"{score:.2f}"
        },
        priority="high" if score >= 8.0 else "normal"
    )


def _ticker_display(ticker: str, name: str | None = None) -> str:
    """Format ticker with company name for readability: '贵州茅台 600519.SH'."""
    if name:
        short_name = name[:12]
        return f"{short_name} {ticker}"
    return ticker


def send_run_summary_alert(
    date_str: str,
    weekly_count: int,
    pro30_count: int,
    movers_count: int,
    overlaps: dict,
    config: Optional[AlertConfig] = None,
    weekly_tickers: Optional[list] = None,
    pro30_tickers: Optional[list] = None,
    movers_tickers: Optional[list] = None,
    model_health: Optional[dict] = None,
    weekly_top5_data: Optional[list] = None,
    hybrid_top3: Optional[list] = None,
    primary_label: str = "Weekly",
    primary_candidates_count: Optional[int] = None,
    position_alerts: Optional[dict] = None,
    regime: Optional[str] = None,
) -> dict[str, bool]:
    """Send comprehensive daily scan summary with HTML formatting."""
    manager = AlertManager(config)

    all_three = overlaps.get("all_three", [])
    primary_pro30 = overlaps.get("primary_pro30", overlaps.get("weekly_pro30", []))
    primary_movers = overlaps.get("primary_movers", overlaps.get("weekly_movers", []))
    pro30_movers = overlaps.get("pro30_movers", [])

    weekly_tickers = list(weekly_tickers) if weekly_tickers else []
    pro30_tickers = list(pro30_tickers) if pro30_tickers else []
    movers_tickers = list(movers_tickers) if movers_tickers else []
    weekly_top5_data = weekly_top5_data or []
    hybrid_top3 = hybrid_top3 or []

    # Build a name lookup from available data
    name_map: dict[str, str] = {}
    for item in hybrid_top3:
        t = item.get("ticker", "")
        n = item.get("name", "")
        if t and n:
            name_map[t] = n
    for item in weekly_top5_data:
        t = item.get("ticker", "")
        n = item.get("name", "")
        if t and n:
            name_map.setdefault(t, n)

    regime_em = _regime_emoji(regime) if regime else ""
    pick_count = len(hybrid_top3) if hybrid_top3 else weekly_count

    lines = [
        f"<b>\U0001f409 Dragon Pulse \u2014 {date_str}</b>",
        "",
    ]

    if regime:
        lines.append(f"{regime_em} Regime: <b>{regime.upper()}</b> | Picks: <b>{pick_count}</b>")
    else:
        lines.append(f"Picks: <b>{pick_count}</b>")
    lines.append("")

    # Hybrid Top 3
    if hybrid_top3:
        lines.append(_section_line())
        lines.append("\U0001f3c6 <b>Hybrid Top 3</b>")
        lines.append("")
        for item in hybrid_top3[:3]:
            ticker = item.get("ticker", "?")
            name = item.get("name", "") or name_map.get(ticker, "")
            display = _ticker_display(ticker, name)
            hybrid_score = item.get("hybrid_score", 0)
            sources = ", ".join(item.get("sources", []))
            score_bar = _bar(hybrid_score, 100, 10)

            lines.append(f"<b>{item.get('rank', '?')}. {display}</b>  {sources}")
            lines.append(f"   {score_bar} {hybrid_score:.0f}/100")
            lines.append("")

    # Weekly Top 5
    if weekly_top5_data:
        lines.append(_section_line())
        lines.append(f"\u2b50 <b>{primary_label} Top 5</b>")
        lines.append("")
        for item in weekly_top5_data[:5]:
            ticker = item.get("ticker", "?")
            name = item.get("name", "") or name_map.get(ticker, "")
            display = _ticker_display(ticker, name)
            score = item.get("composite_score") or item.get("swing_score", 0) or 0
            try:
                score = float(score)
            except Exception:
                score = 0.0
            verdict = item.get("verdict") or item.get("confidence", "")
            score_bar = _bar(score, 10, 10)
            lines.append(f"  {display}: {score_bar} {score:.1f} {verdict}".rstrip())
        lines.append("")

    # Overlaps
    if all_three or primary_pro30 or primary_movers or pro30_movers:
        lines.append(_section_line())
        lines.append("\U0001f3af <b>Overlaps</b>")
        if all_three:
            display_list = [_ticker_display(t, name_map.get(t)) for t in all_three]
            lines.append(f"   \u2b50 ALL THREE: {', '.join(display_list)}")
        if primary_pro30:
            non_triple = [t for t in primary_pro30 if t not in all_three]
            if non_triple:
                display_list = [_ticker_display(t, name_map.get(t)) for t in non_triple]
                lines.append(f"   \U0001f525 {primary_label}+Pro30: {', '.join(display_list)}")
        if primary_movers:
            non_other = [t for t in primary_movers if t not in all_three]
            if non_other:
                display_list = [_ticker_display(t, name_map.get(t)) for t in non_other]
                lines.append(f"   \U0001f4c8 {primary_label}+Movers: {', '.join(display_list)}")
        if pro30_movers:
            non_other = [t for t in pro30_movers if t not in all_three]
            if non_other:
                display_list = [_ticker_display(t, name_map.get(t)) for t in non_other]
                lines.append(f"   \U0001f48e Pro30+Movers: {', '.join(display_list)}")
        lines.append("")

    # Model Health
    if model_health:
        health_status = model_health.get("status", "Unknown")
        hit_rate = model_health.get("hit_rate")
        win_rate = model_health.get("win_rate")
        parts = [f"\U0001f4c8 Model: {health_status}"]
        if hit_rate is not None and win_rate is not None:
            parts.append(f"Hit: {hit_rate * 100:.0f}% | Win: {win_rate * 100:.0f}%")
        lines.append("  ".join(parts))

        strategies = model_health.get("strategies", [])
        if strategies:
            strat_parts = [f"{s.get('name', '?')}:{s.get('hit_rate', 0) * 100:.0f}%" for s in strategies[:3]]
            lines.append(f"  {' | '.join(strat_parts)}")

    # Determine priority
    if all_three:
        priority = "high"
    elif hybrid_top3 and any("Pro30" in item.get("sources", []) for item in hybrid_top3):
        priority = "high"
    elif primary_pro30:
        priority = "normal"
    else:
        priority = "low"

    emoji = {"\U0001f4ca": "low", "\U0001f4c8": "normal", "\U0001f6a8": "high"}.get(priority, "\U0001f4c8")

    message = "\n".join(lines)
    title = f"\U0001f409 Dragon Pulse \u2014 {date_str}"

    return manager.send_alert(
        title=title,
        message=message,
        data={"asof": date_str},
        priority=priority,
    )
