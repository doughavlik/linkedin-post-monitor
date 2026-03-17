import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from db import get_setting

ACTIVITY_LABELS = {
    'post': 'Post',
    'share': 'Share',
    'article': 'Article',
    'comment': 'Comment',
    'reaction': 'Reaction',
}


def send_monitor_email(new_company_activity, checked_companies,
                       new_people_activity=None, checked_people=0, error=None):
    """
    Send a summary email via SMTP.
    Returns (success: bool, error_message: str | None).
    """
    smtp_host = get_setting('smtp_host', 'smtp.gmail.com')
    smtp_port = int(get_setting('smtp_port', '587') or '587')
    smtp_user = (get_setting('smtp_user', '') or '').strip()
    smtp_password = (get_setting('smtp_app_password', '') or '').strip()
    email_to = (get_setting('email_to', '') or '').strip()

    if not smtp_user or not smtp_password or not email_to:
        return False, "Email not configured — fill in SMTP settings."

    new_people_activity = new_people_activity or []
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M CT')
    subject = f"LinkedIn Monitor — {now_str}"

    if error and 'expired' in error.lower():
        body = (
            f"⚠️  LinkedIn session has expired.\n\n"
            f"Please open the app and update your LinkedIn cookies in Settings.\n\n"
            f"Time: {now_str}"
        )
    elif not new_company_activity and not new_people_activity:
        body = (
            f"No new activity detected in the last ~30 minutes.\n\n"
            f"Checked {checked_companies} companies"
            + (f" and {checked_people} people" if checked_people else "")
            + f" at {now_str}."
        )
    else:
        lines = [f"New LinkedIn activity detected ({now_str}):\n"]

        if new_company_activity:
            lines.append("── COMPANIES ──")
            for item in new_company_activity:
                atype = ACTIVITY_LABELS.get(item.get('activity_type', ''), 'Activity')
                ts = item.get('activity_time') or item.get('timestamp') or ''
                lines.append(f"• {item['name']}  [{atype}]  {ts}")
                lines.append(f"  {item['url']}\n")

        if new_people_activity:
            lines.append("── PEOPLE ──")
            for item in new_people_activity:
                atype = ACTIVITY_LABELS.get(item.get('activity_type', ''), 'Activity')
                ts = item.get('activity_time') or item.get('timestamp') or ''
                company_note = f"  ({item['company_name']})" if item.get('company_name') else ''
                lines.append(f"• {item['name']}{company_note}  [{atype}]  {ts}")
                lines.append(f"  {item['url']}\n")

        body = '\n'.join(lines)
        body += (
            f"\n\nChecked {checked_companies} companies"
            + (f" and {checked_people} people" if checked_people else "")
            + " total."
        )

    if error:
        body += f"\n\n⚠️  Note: {error}"

    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = email_to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, [email_to], msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)
