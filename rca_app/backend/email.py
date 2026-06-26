"""
Email utility — sends styled HTML emails via Mailgun.
Loads credentials from Databricks Secrets when deployed, env vars for local dev.
"""
import base64
import os
import re
import logging
import requests

logger = logging.getLogger(__name__)

SECRET_SCOPE = "dbrks-eo-analytics-demo"
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

# Lazy-loaded config (populated on first send)
_config: dict = {}


def _load_config() -> dict:
    """Load Mailgun config from Databricks Secrets (deployed) or env vars (local)."""
    global _config
    if _config:
        return _config

    # Non-sensitive values from env vars (set by app.yaml / databricks.yml)
    _config = {
        "MAILGUN_API_URL": os.environ.get("MAILGUN_API_URL", ""),
        "MAILGUN_API_KEY": os.environ.get("MAILGUN_API_KEY", ""),
        "SENDER": os.environ.get("SENDER", ""),
        "RECIPIENT": os.environ.get("RECIPIENT", ""),
    }

    # When deployed, load the API key from Databricks Secrets (not stored in plain env vars)
    if IS_DATABRICKS_APP and not _config["MAILGUN_API_KEY"]:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            resp = w.secrets.get_secret(scope=SECRET_SCOPE, key="MAILGUN_API_KEY")
            raw = resp.value if resp.value else ""
            # SDK returns base64-encoded secret values
            _config["MAILGUN_API_KEY"] = base64.b64decode(raw).decode() if raw else ""
            logger.info(f"Loaded MAILGUN_API_KEY from Databricks Secrets (scope={SECRET_SCOPE})")
        except Exception as e:
            logger.warning(f"Could not load MAILGUN_API_KEY from secrets: {e}")

    return _config

# ── App-matched palette (dark navy operational dashboard) ────────
BG_OUTER = "#070e1a"
BG_CARD = "#111f33"
BG_ELEVATED = "#1a2d45"
BORDER = "#1e3a5f"
TEXT_PRIMARY = "#e8edf4"
TEXT_SECONDARY = "#8ba3c1"
TEXT_MUTED = "#5a7a9e"
CYAN = "#00d4ff"
ACCENT = "#00b4d8"
CRITICAL = "#ff4757"
HIGH = "#ff8c42"
MEDIUM = "#ffc107"
GREEN = "#00e676"
PURPLE = "#bc8cff"
FONT = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
MONO = "SF Mono, Cascadia Code, Consolas, monospace"


def _markdown_to_html(md: str) -> str:
    """Convert markdown → styled HTML matching the app's dark-navy theme."""
    html = md

    # Bold — cyan accent for emphasis
    html = re.sub(r'\*\*(.+?)\*\*', rf'<strong style="color:{CYAN};">\1</strong>', html)

    # Inline code — dark pill
    html = re.sub(
        r'`([^`]+)`',
        rf'<code style="background:{BG_ELEVATED};color:{CYAN};padding:2px 7px;'
        rf'border-radius:4px;font-family:{MONO};font-size:0.85em;">\1</code>',
        html,
    )

    # H1 — section divider with accent bar
    html = re.sub(
        r'^#\s+(.+)$',
        rf'<div style="margin:24px 0 12px;padding:10px 14px;background:{BG_ELEVATED};'
        rf'border-left:3px solid {CYAN};border-radius:0 8px 8px 0;">'
        rf'<span style="font-size:18px;font-weight:700;color:{TEXT_PRIMARY};">\1</span></div>',
        html, flags=re.MULTILINE,
    )

    # H2 — section headers with cyan accent
    html = re.sub(
        r'^##\s+(.+)$',
        rf'<div style="margin:22px 0 10px;padding:8px 12px;background:{BG_ELEVATED};'
        rf'border-left:3px solid {ACCENT};border-radius:0 6px 6px 0;">'
        rf'<span style="font-size:15px;font-weight:700;color:{TEXT_PRIMARY};">\1</span></div>',
        html, flags=re.MULTILINE,
    )

    # H3–H5 — subtle labels
    for lvl in (5, 4, 3):
        html = re.sub(
            rf'^#{{{lvl}}}\s+(.+)$',
            rf'<div style="margin:14px 0 6px;font-size:13px;font-weight:600;'
            rf'color:{TEXT_SECONDARY};text-transform:uppercase;letter-spacing:0.04em;">\1</div>',
            html, flags=re.MULTILINE,
        )

    # Numbered list items — styled cards
    html = re.sub(
        r'^(\d+)\.\s+(.+)$',
        rf'<div style="display:flex;align-items:flex-start;gap:10px;margin:6px 0;'
        rf'padding:8px 12px;background:{BG_ELEVATED};border-radius:6px;">'
        rf'<span style="color:{ACCENT};font-weight:700;font-size:14px;min-width:18px;">\1.</span>'
        rf'<span style="color:{TEXT_PRIMARY};font-size:14px;line-height:1.5;">\2</span></div>',
        html, flags=re.MULTILINE,
    )

    # Bullet list items
    html = re.sub(
        r'^[-*]\s+(.+)$',
        rf'<div style="display:flex;align-items:flex-start;gap:8px;margin:4px 0 4px 8px;">'
        rf'<span style="color:{ACCENT};font-size:10px;margin-top:5px;">&#9679;</span>'
        rf'<span style="color:{TEXT_PRIMARY};font-size:14px;line-height:1.5;">\1</span></div>',
        html, flags=re.MULTILINE,
    )

    # Paragraphs (double newline)
    html = re.sub(r'\n\n', rf'</div><div style="margin:8px 0;color:{TEXT_SECONDARY};font-size:14px;line-height:1.6;">', html)
    # Single newlines → <br>
    html = html.replace('\n', '<br/>')

    return f'<div style="margin:8px 0;color:{TEXT_SECONDARY};font-size:14px;line-height:1.6;">{html}</div>'


def send_analysis_email(analysis_markdown: str, model: str, pattern_name: str = None) -> dict:
    """Send the AI analysis as a formatted email. Returns status dict."""
    cfg = _load_config()

    missing = [k for k, v in cfg.items() if not v]
    if missing:
        logger.warning(f"Email not configured — missing: {', '.join(missing)}")
        return {"email_sent": False, "reason": f"Missing config: {', '.join(missing)}"}

    sender = cfg["SENDER"]
    recipient = cfg["RECIPIENT"]

    subject = "Root Cause Intelligence — AI Analysis"
    if pattern_name:
        subject = f"Root Cause Intelligence — {pattern_name}"

    analysis_html = _markdown_to_html(analysis_markdown)

    html_body = f"""
<html><body style="margin:0;padding:0;background:{BG_OUTER};font-family:{FONT};">
<table width="100%" style="background:{BG_OUTER};"><tr><td align="center" style="padding:24px 12px;">
<table width="680" style="max-width:680px;background:{BG_CARD};border-radius:12px;border:1px solid {BORDER};box-shadow:0 8px 24px rgba(0,0,0,.5);">

<!-- Header bar -->
<tr><td style="padding:20px 24px 16px;border-bottom:1px solid {BORDER};">
  <table width="100%"><tr>
    <td>
      <div style="font-size:20px;font-weight:700;color:{TEXT_PRIMARY};letter-spacing:-0.01em;">Root Cause Intelligence</div>
      <div style="font-size:12px;color:{TEXT_MUTED};margin-top:4px;">AI-Powered Analysis Report</div>
    </td>
    <td align="right" valign="top">
      <div style="display:inline-block;background:{ACCENT};color:#fff;padding:4px 10px;border-radius:999px;font-size:10px;font-weight:700;letter-spacing:0.05em;">AI ANALYSIS</div>
    </td>
  </tr></table>
</td></tr>

<!-- Body -->
<tr><td style="padding:20px 24px;">
{analysis_html}
</td></tr>

<!-- Footer -->
<tr><td style="padding:14px 24px;border-top:1px solid {BORDER};">
  <table width="100%"><tr>
    <td style="font-size:11px;color:{TEXT_MUTED};">
      Model: <span style="color:{TEXT_SECONDARY};">{model}</span>
    </td>
    <td align="right" style="font-size:11px;color:{TEXT_MUTED};">
      Enterprise Observability &bull;
      <a href="mailto:{sender}" style="color:{ACCENT};text-decoration:none;">{sender}</a>
    </td>
  </tr></table>
</td></tr>

</table>
</td></tr></table>
</body></html>""".strip()

    # Plain text fallback
    plain_text = re.sub(r'<[^>]+>', '', analysis_markdown.replace('\n\n', '\n'))

    try:
        to_list = [r.strip() for r in recipient.split(",") if r.strip()]
        resp = requests.post(
            cfg["MAILGUN_API_URL"],
            auth=("api", cfg["MAILGUN_API_KEY"]),
            data={
                "from": f"EO Analytics <{sender}>",
                "to": to_list,
                "subject": subject,
                "text": plain_text,
                "html": html_body,
                "h:Reply-To": sender,
            },
            timeout=20,
        )
        if resp.ok:
            msg_id = resp.json().get("id", "unknown")
            logger.info(f"Analysis email sent to {to_list} (id={msg_id})")
            return {"email_sent": True, "recipients": to_list, "message_id": msg_id}
        else:
            logger.error(f"Mailgun error {resp.status_code}: {resp.text[:400]}")
            return {"email_sent": False, "reason": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    except Exception as e:
        logger.error(f"Email send error: {e}")
        return {"email_sent": False, "reason": str(e)}
