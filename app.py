import os
import re
import atexit
from datetime import datetime

from flask import Flask, jsonify, request, render_template
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

import db as database
from monitor import run_monitor
from mailer import send_monitor_email

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'linkedin-monitor-secret-dev-key')

scheduler = BackgroundScheduler(daemon=True)


# ---------------------------------------------------------------------------
# Scheduler / monitor job
# ---------------------------------------------------------------------------

def _run_and_log():
    new_co, checked_co, new_ppl, checked_ppl, error = run_monitor()
    email_sent, email_error = send_monitor_email(
        new_co, checked_co, new_ppl, checked_ppl, error
    )

    notes_parts = []
    if error:
        notes_parts.append(error)
    if email_error:
        notes_parts.append(f"Email error: {email_error}")

    database.log_run(
        companies_checked=checked_co,
        new_posts_count=len(new_co),
        people_checked=checked_ppl,
        new_people_activity_count=len(new_ppl),
        email_sent=1 if email_sent else 0,
        status='error' if error else 'ok',
        notes=' | '.join(notes_parts),
    )
    return new_co, checked_co, new_ppl, checked_ppl, error, email_sent, email_error


def monitor_job():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running LinkedIn monitor…")
    new_co, checked_co, new_ppl, checked_ppl, error, email_sent, _ = _run_and_log()
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Done. "
        f"{len(new_co)} company + {len(new_ppl)} people new activity. "
        f"email_sent={email_sent}"
    )


def start_scheduler():
    interval = int(database.get_setting('monitor_interval_minutes', '30') or '30')
    scheduler.add_job(
        monitor_job,
        trigger=IntervalTrigger(minutes=interval),
        id='monitor_job',
        replace_existing=True,
    )
    if not scheduler.running:
        scheduler.start()


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    return render_template('index.html')


# ---------------------------------------------------------------------------
# Companies API
# ---------------------------------------------------------------------------

@app.route('/api/companies', methods=['GET'])
def list_companies():
    return jsonify(database.get_companies())


@app.route('/api/companies', methods=['POST'])
def add_company():
    data = request.get_json(force=True)
    name = (data.get('name') or '').strip()
    slug = (data.get('slug') or '').strip()
    if not name or not slug:
        return jsonify({'error': 'name and slug are required'}), 400
    try:
        database.add_company(name, slug)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/companies/<int:company_id>', methods=['PUT'])
def update_company(company_id):
    data = request.get_json(force=True)
    name = (data.get('name') or '').strip()
    slug = (data.get('slug') or '').strip()
    active = int(data.get('active', 1))
    if not name or not slug:
        return jsonify({'error': 'name and slug are required'}), 400
    database.update_company(company_id, name, slug, active)
    return jsonify({'ok': True})


@app.route('/api/companies/<int:company_id>', methods=['DELETE'])
def delete_company(company_id):
    database.delete_company(company_id)
    return jsonify({'ok': True})


# ---------------------------------------------------------------------------
# People API
# ---------------------------------------------------------------------------

def _slug_from_url(url):
    """Extract LinkedIn profile slug from a /in/ URL."""
    url = url.strip().rstrip('/')
    m = re.search(r'/in/([^/?#]+)', url)
    return m.group(1) if m else None


@app.route('/api/people', methods=['GET'])
def list_people():
    return jsonify(database.get_people())


@app.route('/api/people', methods=['POST'])
def add_person():
    data = request.get_json(force=True)
    name = (data.get('name') or '').strip()
    profile_url = (data.get('profile_url') or '').strip()
    company_id = data.get('company_id') or None

    profile_slug = _slug_from_url(profile_url)
    if not name or not profile_slug:
        return jsonify({'error': 'name and a valid LinkedIn /in/ URL are required'}), 400

    # Normalise URL to canonical form
    canonical_url = f"https://www.linkedin.com/in/{profile_slug}/"

    try:
        database.add_person(name, profile_slug, canonical_url, company_id)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/people/<int:person_id>', methods=['PUT'])
def update_person(person_id):
    data = request.get_json(force=True)
    name = (data.get('name') or '').strip()
    profile_url = (data.get('profile_url') or '').strip()
    company_id = data.get('company_id') or None
    active = int(data.get('active', 1))

    profile_slug = _slug_from_url(profile_url)
    if not name or not profile_slug:
        return jsonify({'error': 'name and a valid LinkedIn /in/ URL are required'}), 400

    canonical_url = f"https://www.linkedin.com/in/{profile_slug}/"
    database.update_person(person_id, name, profile_slug, canonical_url, company_id, active)
    return jsonify({'ok': True})


@app.route('/api/people/<int:person_id>', methods=['DELETE'])
def delete_person(person_id):
    database.delete_person(person_id)
    return jsonify({'ok': True})


# ---------------------------------------------------------------------------
# Settings API
# ---------------------------------------------------------------------------

SENSITIVE_KEYS = {'smtp_app_password', 'linkedin_li_at', 'linkedin_jsessionid'}
ALLOWED_KEYS = {
    'linkedin_li_at', 'linkedin_jsessionid',
    'smtp_host', 'smtp_port', 'smtp_user', 'smtp_app_password',
    'email_to', 'monitor_interval_minutes',
}


@app.route('/api/settings', methods=['GET'])
def get_settings():
    settings = database.get_all_settings()
    masked = {}
    for key in ALLOWED_KEYS:
        val = settings.get(key, '')
        masked[key] = '__set__' if (key in SENSITIVE_KEYS and val) else (val or '')
    return jsonify(masked)


@app.route('/api/settings', methods=['POST'])
def save_settings():
    data = request.get_json(force=True)
    for key in ALLOWED_KEYS:
        if key in data:
            val = data[key]
            if val == '__set__':
                continue
            database.set_setting(key, str(val).strip())
    start_scheduler()
    return jsonify({'ok': True})


# ---------------------------------------------------------------------------
# Monitor API
# ---------------------------------------------------------------------------

@app.route('/api/monitor/run', methods=['POST'])
def run_now():
    new_co, checked_co, new_ppl, checked_ppl, error, email_sent, email_error = _run_and_log()

    return jsonify({
        'ok': True,
        'new_company_activity': new_co,
        'new_people_activity': new_ppl,
        'checked_companies': checked_co,
        'checked_people': checked_ppl,
        'email_sent': email_sent,
        'error': error,
        'email_error': email_error,
    })


@app.route('/api/monitor/runs', methods=['GET'])
def get_runs():
    return jsonify(database.get_recent_runs())


@app.route('/api/monitor/status', methods=['GET'])
def monitor_status():
    job = scheduler.get_job('monitor_job')
    next_run = None
    if job and job.next_run_time:
        next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S')

    runs = database.get_recent_runs(1)
    last_run = runs[0] if runs else None

    return jsonify({
        'scheduler_running': scheduler.running,
        'next_run': next_run,
        'last_run': last_run,
        'interval_minutes': int(database.get_setting('monitor_interval_minutes', '30') or '30'),
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    database.init_db()
    database.seed_companies()
    start_scheduler()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    print("LinkedIn Monitor running at http://localhost:5000")
    app.run(debug=False, port=5000, use_reloader=False)
