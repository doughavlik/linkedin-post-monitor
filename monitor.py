import requests
import time
import re
from datetime import datetime, timedelta

from db import (
    get_companies, update_company_post, update_company_checked,
    get_people, update_person_activity, update_person_checked,
    get_setting,
)

LINKEDIN_BASE = "https://www.linkedin.com"
VOYAGER_PATH = "/voyager/api/feed/updates"
BATCH_SIZE = 20
BATCH_DELAY_SECONDS = 2


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def get_linkedin_session():
    li_at = get_setting('linkedin_li_at', '').strip()
    jsessionid = get_setting('linkedin_jsessionid', '').strip()

    if not li_at or not jsessionid:
        return None, "LinkedIn cookies not configured — go to Settings to add them."

    csrf_token = jsessionid.strip('"')

    session = requests.Session()
    session.cookies.set('li_at', li_at, domain='.linkedin.com')
    session.cookies.set('JSESSIONID', f'"{csrf_token}"', domain='.linkedin.com')

    session.headers.update({
        'csrf-token': csrf_token,
        'accept': 'application/vnd.linkedin.normalized+json+2.1',
        'x-restli-protocol-version': '2.0.0',
        'x-li-lang': 'en_US',
        'x-li-track': (
            '{"clientVersion":"1.13.4","mpVersion":"1.13.4","osName":"web",'
            '"timezoneOffset":-5,"timezone":"America/Chicago",'
            '"deviceFormFactor":"DESKTOP","mpName":"voyager-web"}'
        ),
        'user-agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/121.0.0.0 Safari/537.36'
        ),
        'referer': 'https://www.linkedin.com/',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
    })

    return session, None


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def parse_timestamp(text):
    """
    Parse LinkedIn relative time string → (value: int, unit: str) or None.
    Units: 'm' minutes, 'h' hours, 'd' days, 'w' weeks.
    """
    if not text:
        return None
    text = text.strip().lower()

    if 'just now' in text or text == '0m':
        return (0, 'm')

    match = re.search(r'(\d+)\s*([mhdw])', text)
    if match:
        return (int(match.group(1)), match.group(2))

    return None


def relative_to_datetime(text, ref_time=None):
    """
    Convert a LinkedIn relative timestamp like '5h •' to an absolute datetime string.
    Returns 'YYYY-MM-DD HH:MM' (approximate) or None if unparseable.
    """
    parsed = parse_timestamp(text)
    if parsed is None:
        return None

    if ref_time is None:
        ref_time = datetime.now()

    value, unit = parsed
    delta_map = {'m': timedelta(minutes=value), 'h': timedelta(hours=value),
                 'd': timedelta(days=value), 'w': timedelta(weeks=value)}
    delta = delta_map.get(unit)
    if delta is None:
        return None

    return (ref_time - delta).strftime('%Y-%m-%d %H:%M')


def is_new_activity(timestamp_text, threshold_minutes=35):
    """
    Returns True if the timestamp indicates activity within threshold_minutes.
    Uses 35 min buffer to catch items posted just after the previous check.
    Also flags exactly '1h' since it straddles the 30-min boundary.
    """
    parsed = parse_timestamp(timestamp_text)
    if parsed is None:
        return False
    value, unit = parsed
    if unit == 'm':
        return value <= threshold_minutes
    if unit == 'h':
        return value <= 1
    return False


# ---------------------------------------------------------------------------
# Activity type detection
# ---------------------------------------------------------------------------

def detect_activity_type(update):
    """
    Best-effort detection of the activity type from a feed UpdateV2 object.
    Returns one of: 'post', 'share', 'article', 'comment', 'reaction'.
    """
    # Explicit reshare flag (value may be an empty dict, so check is-not-None)
    if update.get('resharedUpdate') is not None:
        return 'share'

    # Check content $type
    content = update.get('content', {})
    if isinstance(content, dict):
        ct = content.get('$type', '')
        if 'Article' in ct:
            return 'article'
        # Content that is itself another UpdateV2 → it's a reshare/quote
        if 'UpdateV2' in ct:
            return 'share'

    # Reaction type present directly on the update
    if update.get('reactionType'):
        return 'reaction'

    # Social detail hints
    social = update.get('socialDetail', {})
    if isinstance(social, dict) and (social.get('reactionType') or social.get('reactionText')):
        return 'reaction'

    # If the update has a parent/root update URN it's likely a comment
    try:
        update_type = update.get('updateMetadata', {}).get('updateType', '')
        if 'COMMENT' in update_type.upper():
            return 'comment'
    except Exception:
        pass

    return 'post'


# ---------------------------------------------------------------------------
# Company batch checker
# ---------------------------------------------------------------------------

def _parse_feed_response(data, fallback_url_prefix):
    """
    Extract the most recent activity from a Voyager feed API response.
    Returns dict with keys: activity_text, activity_time, activity_url, activity_type.
    """
    ref_time = datetime.now()
    included = data.get('included', [])

    # Gather ALL UpdateV2 objects regardless of sub-type
    updates = [
        item for item in included
        if item.get('$type') == 'com.linkedin.voyager.feed.render.UpdateV2'
    ]

    # Also look for top-level Comment objects
    comments = [
        item for item in included
        if 'Comment' in item.get('$type', '')
        and item.get('actor') is not None
    ]

    candidates = updates + comments
    if not candidates:
        return None

    latest = candidates[0]

    # Extract relative time text
    activity_text = None
    for path in [
        ['actor', 'subDescription', 'text'],
        ['header', 'text', 'text'],
        ['commentary', 'text', 'text'],
    ]:
        try:
            val = latest
            for key in path:
                val = val[key]
            if val:
                activity_text = val
                break
        except (KeyError, TypeError):
            pass

    activity_time = relative_to_datetime(activity_text, ref_time)
    activity_type = detect_activity_type(latest)

    # Extract URL
    activity_url = None
    try:
        urn = latest['updateMetadata']['updateUrn']
        activity_url = f"https://www.linkedin.com/feed/update/{urn}/"
    except (KeyError, TypeError):
        activity_url = fallback_url_prefix

    return {
        'activity_text': activity_text,
        'activity_time': activity_time,
        'activity_url': activity_url,
        'activity_type': activity_type,
    }


def check_company_batch(slugs, session):
    """
    Check a batch of company slugs.
    Returns { slug: result_dict | { error: str } }
    """
    results = {}

    for slug in slugs:
        try:
            resp = session.get(
                LINKEDIN_BASE + VOYAGER_PATH,
                params={
                    'companyUniversalName': slug,
                    'moduleKey': 'ORGANIZATION_MEMBER_FEED_DESKTOP',
                    'numComments': 0,
                    'numLikes': 0,
                    'q': 'companyFeedByUniversalName',
                    'sortOrder': 'RELEVANCE',
                    'start': 0,
                    'count': 10,
                },
                timeout=15,
            )

            if resp.status_code == 401:
                results[slug] = {'error': 'auth_expired'}
                continue
            if resp.status_code != 200:
                results[slug] = {'error': f'http_{resp.status_code}'}
                continue

            parsed = _parse_feed_response(
                resp.json(),
                fallback_url_prefix=f"https://www.linkedin.com/company/{slug}/posts/",
            )

            if parsed is None:
                results[slug] = {
                    'activity_text': None, 'activity_time': None,
                    'activity_url': None, 'activity_type': None, 'is_new': False,
                }
            else:
                parsed['is_new'] = is_new_activity(parsed['activity_text'])
                results[slug] = parsed

        except requests.exceptions.Timeout:
            results[slug] = {'error': 'timeout'}
        except Exception as e:
            results[slug] = {'error': str(e)[:120]}

    return results


# ---------------------------------------------------------------------------
# People batch checker
# ---------------------------------------------------------------------------

def check_people_batch(slugs, session):
    """
    Check a batch of person profile slugs via memberFeedByMemberIdentity.
    Returns { slug: result_dict | { error: str } }
    """
    results = {}

    for slug in slugs:
        try:
            resp = session.get(
                LINKEDIN_BASE + VOYAGER_PATH,
                params={
                    'memberIdentity': slug,
                    'q': 'memberFeedByMemberIdentity',
                    'count': 10,
                    'start': 0,
                },
                timeout=15,
            )

            if resp.status_code == 401:
                results[slug] = {'error': 'auth_expired'}
                continue
            if resp.status_code != 200:
                results[slug] = {'error': f'http_{resp.status_code}'}
                continue

            parsed = _parse_feed_response(
                resp.json(),
                fallback_url_prefix=f"https://www.linkedin.com/in/{slug}/recent-activity/",
            )

            if parsed is None:
                results[slug] = {
                    'activity_text': None, 'activity_time': None,
                    'activity_url': None, 'activity_type': None, 'is_new': False,
                }
            else:
                parsed['is_new'] = is_new_activity(parsed['activity_text'])
                results[slug] = parsed

        except requests.exceptions.Timeout:
            results[slug] = {'error': 'timeout'}
        except Exception as e:
            results[slug] = {'error': str(e)[:120]}

    return results


# ---------------------------------------------------------------------------
# Main monitor entry point
# ---------------------------------------------------------------------------

def run_monitor():
    """
    Check all active companies and people.
    Returns (new_company_activity, checked_companies,
             new_people_activity, checked_people, error).
    """
    session, err = get_linkedin_session()
    if err:
        return [], 0, [], 0, err

    # --- Companies ---
    companies = get_companies(active_only=True)
    slug_to_company = {c['slug']: c for c in companies}
    company_slugs = list(slug_to_company.keys())
    new_company_activity = []
    auth_expired = False

    for i in range(0, len(company_slugs), BATCH_SIZE):
        batch = company_slugs[i: i + BATCH_SIZE]
        results = check_company_batch(batch, session)

        for slug, result in results.items():
            company = slug_to_company.get(slug, {})

            if result.get('error') == 'auth_expired':
                auth_expired = True
                update_company_checked(slug)
                continue

            if result.get('error'):
                update_company_checked(slug)
                continue

            update_company_post(
                slug,
                result.get('activity_text'),
                result.get('activity_url'),
                last_post_time=result.get('activity_time'),
                last_activity_type=result.get('activity_type'),
            )

            if result.get('is_new') and result.get('activity_url'):
                new_company_activity.append({
                    'name': company.get('name', slug),
                    'slug': slug,
                    'url': result['activity_url'],
                    'timestamp': result.get('activity_text'),
                    'activity_time': result.get('activity_time'),
                    'activity_type': result.get('activity_type', 'post'),
                    'entity': 'company',
                })

        if i + BATCH_SIZE < len(company_slugs):
            time.sleep(BATCH_DELAY_SECONDS)

    # --- People ---
    people = get_people(active_only=True)
    slug_to_person = {p['profile_slug']: p for p in people}
    person_slugs = list(slug_to_person.keys())
    new_people_activity = []

    for i in range(0, len(person_slugs), BATCH_SIZE):
        batch = person_slugs[i: i + BATCH_SIZE]
        results = check_people_batch(batch, session)

        for slug, result in results.items():
            person = slug_to_person.get(slug, {})

            if result.get('error') == 'auth_expired':
                auth_expired = True
                update_person_checked(slug)
                continue

            if result.get('error'):
                update_person_checked(slug)
                continue

            update_person_activity(
                slug,
                result.get('activity_text'),
                result.get('activity_time'),
                result.get('activity_url'),
                result.get('activity_type'),
            )

            if result.get('is_new') and result.get('activity_url'):
                new_people_activity.append({
                    'name': person.get('name', slug),
                    'slug': slug,
                    'url': result['activity_url'],
                    'timestamp': result.get('activity_text'),
                    'activity_time': result.get('activity_time'),
                    'activity_type': result.get('activity_type', 'post'),
                    'company_name': person.get('company_name', ''),
                    'entity': 'person',
                })

        if i + BATCH_SIZE < len(person_slugs):
            time.sleep(BATCH_DELAY_SECONDS)

    error = None
    if auth_expired:
        error = "LinkedIn session expired — please update your cookies in Settings."

    return (
        new_company_activity, len(companies),
        new_people_activity, len(people),
        error,
    )
