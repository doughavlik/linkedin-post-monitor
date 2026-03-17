import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'linkedin_monitor.db')

COMPANY_SLUGS = [
    "priceeasy-ai", "converge-point", "voyagerportal", "smartvault-corporation",
    "aha-labs-inc-", "cyberrplatform", "ehs-insight", "frontline-data-solutions",
    "imubit", "targetrecruit", "repeatmd", "servient-inc", "shrgroup-hospitality",
    "convergent-science-inc", "cloudnine-discovery", "cobblestone-systems-corp-",
    "blumetra-solutions", "htx-labs", "thread-service", "pixforce", "liqteq",
    "aicure", "combocurve", "management-controls-inc", "maximl",
    "cybersoft-north-america-inc-", "twiseroperatingsystem", "regdesk",
    "wherescape", "data-dynamics", "graylog", "optculture", "codefinity-solutions",
    "streebo-inc", "int", "calibermind", "vortexa", "xperti", "engflow",
    "capitalize-analytics", "officialbitswits", "cumulus-quality-systems",
    "lensec-llc", "p97networks", "voovio", "continuousautomation", "optisigns",
    "bluwarecorp", "onware", "bursys", "ngenue", "mlc-cad-systems",
    "identity-automation", "tachyus", "mechademy", "macorva", "innowatts-energy",
    "decimetrix", "envana-software-solutions", "sk-global-software", "vikua",
    "the-zig", "spark-business-works", "kahunaworkforcesolutions", "langomobile",
    "cloudlogicallyinc", "dealerbuilt", "automation-solutions-inc",
    "pcssoftwareusa", "evisions-inc", "multisensorai", "smartac-com-inc",
    "snapstream-media", "thirdai-corp", "mapertech", "sensys-llc", "unvired-inc",
    "alliance-solutions-group", "office-gemini", "opptly", "matrix-requirements",
    "securitygate", "anbsystems", "neuralixai", "umbrage", "webyog-inc-",
    "black-in-gaming", "cerebre", "oildex", "omniscience-bio", "wearepions",
    "trident-1", "jonas-fitness-inc-", "wesoai", "kiuwan", "januaryadvisors",
    "protransport", "aize", "dearman-systems-inc", "truefit",
]


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            active INTEGER DEFAULT 1,
            last_post_text TEXT,
            last_post_url TEXT,
            last_post_time TEXT,
            last_activity_type TEXT,
            last_checked TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    ''')

    # Migrations for databases created before these columns were added
    for col_def in ['last_post_time TEXT', 'last_activity_type TEXT', 'notes TEXT']:
        try:
            c.execute(f"ALTER TABLE companies ADD COLUMN {col_def}")
        except Exception:
            pass  # Already exists

    c.execute('''
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            profile_slug TEXT UNIQUE NOT NULL,
            profile_url TEXT NOT NULL,
            company_id INTEGER,
            active INTEGER DEFAULT 1,
            last_activity_text TEXT,
            last_activity_time TEXT,
            last_activity_url TEXT,
            last_activity_type TEXT,
            last_checked TEXT,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL
        )
    ''')

    for col_def in ['notes TEXT']:
        try:
            c.execute(f"ALTER TABLE people ADD COLUMN {col_def}")
        except Exception:
            pass

    c.execute('''
        CREATE TABLE IF NOT EXISTS monitor_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT NOT NULL,
            companies_checked INTEGER DEFAULT 0,
            new_posts_count INTEGER DEFAULT 0,
            people_checked INTEGER DEFAULT 0,
            new_people_activity_count INTEGER DEFAULT 0,
            email_sent INTEGER DEFAULT 0,
            status TEXT,
            notes TEXT
        )
    ''')

    # Migration for monitor_runs if columns don't exist yet
    for col_def in ['people_checked INTEGER DEFAULT 0',
                    'new_people_activity_count INTEGER DEFAULT 0']:
        try:
            c.execute(f"ALTER TABLE monitor_runs ADD COLUMN {col_def}")
        except Exception:
            pass

    c.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    conn.commit()
    conn.close()


def seed_companies():
    conn = get_conn()
    c = conn.cursor()
    count = c.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    if count == 0:
        for slug in COMPANY_SLUGS:
            name = ' '.join(
                word.capitalize()
                for word in slug.replace('-', ' ').split()
                if word
            )
            c.execute(
                "INSERT OR IGNORE INTO companies (name, slug) VALUES (?, ?)",
                (name, slug)
            )
        conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Companies
# ---------------------------------------------------------------------------

def get_companies(active_only=False):
    conn = get_conn()
    if active_only:
        rows = conn.execute(
            "SELECT * FROM companies WHERE active=1 ORDER BY name COLLATE NOCASE"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM companies ORDER BY name COLLATE NOCASE"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_company(name, slug):
    conn = get_conn()
    conn.execute(
        "INSERT INTO companies (name, slug) VALUES (?, ?)", (name, slug)
    )
    conn.commit()
    conn.close()


def get_company_by_slug(slug):
    conn = get_conn()
    row = conn.execute("SELECT * FROM companies WHERE slug=?", (slug,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_company(id, name, slug, active, notes=''):
    conn = get_conn()
    conn.execute(
        "UPDATE companies SET name=?, slug=?, active=?, notes=? WHERE id=?",
        (name, slug, active, notes or None, id)
    )
    conn.commit()
    conn.close()


def toggle_company_active(id):
    conn = get_conn()
    conn.execute("UPDATE companies SET active = 1 - active WHERE id=?", (id,))
    conn.commit()
    conn.close()


def delete_company(id):
    conn = get_conn()
    conn.execute("DELETE FROM companies WHERE id=?", (id,))
    conn.commit()
    conn.close()


def update_company_post(slug, last_post_text, last_post_url,
                        last_post_time=None, last_activity_type='post'):
    conn = get_conn()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute(
        "UPDATE companies SET last_post_text=?, last_post_url=?, "
        "last_post_time=?, last_activity_type=?, last_checked=? WHERE slug=?",
        (last_post_text, last_post_url, last_post_time, last_activity_type, now, slug)
    )
    conn.commit()
    conn.close()


def update_company_checked(slug):
    conn = get_conn()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute(
        "UPDATE companies SET last_checked=? WHERE slug=?",
        (now, slug)
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# People
# ---------------------------------------------------------------------------

def get_people(active_only=False):
    """Return people rows joined with company name."""
    conn = get_conn()
    sql = '''
        SELECT p.*, c.name AS company_name
        FROM people p
        LEFT JOIN companies c ON p.company_id = c.id
        {}
        ORDER BY p.name COLLATE NOCASE
    '''.format("WHERE p.active=1" if active_only else "")
    rows = conn.execute(sql).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_person(name, profile_slug, profile_url, company_id=None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO people (name, profile_slug, profile_url, company_id) VALUES (?, ?, ?, ?)",
        (name, profile_slug, profile_url, company_id or None)
    )
    conn.commit()
    conn.close()


def get_person_by_slug(slug):
    conn = get_conn()
    row = conn.execute("SELECT * FROM people WHERE profile_slug=?", (slug,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_person(id, name, profile_slug, profile_url, company_id, active, notes=''):
    conn = get_conn()
    conn.execute(
        "UPDATE people SET name=?, profile_slug=?, profile_url=?, "
        "company_id=?, active=?, notes=? WHERE id=?",
        (name, profile_slug, profile_url, company_id or None, active, notes or None, id)
    )
    conn.commit()
    conn.close()


def toggle_person_active(id):
    conn = get_conn()
    conn.execute("UPDATE people SET active = 1 - active WHERE id=?", (id,))
    conn.commit()
    conn.close()


def delete_person(id):
    conn = get_conn()
    conn.execute("DELETE FROM people WHERE id=?", (id,))
    conn.commit()
    conn.close()


def update_person_activity(profile_slug, activity_text, activity_time,
                           activity_url, activity_type):
    conn = get_conn()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute(
        "UPDATE people SET last_activity_text=?, last_activity_time=?, "
        "last_activity_url=?, last_activity_type=?, last_checked=? WHERE profile_slug=?",
        (activity_text, activity_time, activity_url, activity_type, now, profile_slug)
    )
    conn.commit()
    conn.close()


def update_person_checked(profile_slug):
    conn = get_conn()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute(
        "UPDATE people SET last_checked=? WHERE profile_slug=?",
        (now, profile_slug)
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def get_setting(key, default=None):
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row['value'] if row else default


def set_setting(key, value):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, value)
    )
    conn.commit()
    conn.close()


def get_all_settings():
    conn = get_conn()
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    return {r['key']: r['value'] for r in rows}


# ---------------------------------------------------------------------------
# Monitor Runs
# ---------------------------------------------------------------------------

def log_run(companies_checked, new_posts_count, email_sent, status,
            people_checked=0, new_people_activity_count=0, notes=''):
    conn = get_conn()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute(
        "INSERT INTO monitor_runs "
        "(run_time, companies_checked, new_posts_count, people_checked, "
        "new_people_activity_count, email_sent, status, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (now, companies_checked, new_posts_count, people_checked,
         new_people_activity_count, email_sent, status, notes)
    )
    conn.commit()
    conn.close()


def get_recent_runs(limit=30):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM monitor_runs ORDER BY run_time DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
