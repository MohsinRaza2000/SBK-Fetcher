# -*- coding: utf-8 -*-
"""Fetch bid.aaajapan.com sales statistics and forward them to the SBK portal.

  python aaa_fetch.py [--max-seconds S] [--maker ID] [--workers N] [--once] [--plan]

Runs on GitHub Actions: aaajapan refuses the portal's own server, but not these
machines. It does not page through makers blindly any more - it COMPARES COUNTS.
The source says how many results a maker has in a sale-date window (the first
page of any answer carries the total); the portal says how many it already holds
for the same window (aaa-stats-ingest.php?have=, which costs the source nothing);
and only a window that is short gets read. Every page read goes to the ingest,
which writes car_stats and answers how many of its rows were new.

  --max-seconds  how long this run may last (the workflow passes 1200)
  --maker ID     one maker only (1 = TOYOTA) - for trying things by hand
  --workers N    how many signed-in readers share the work (default 3)
  --once         one round of work, then stop - no waiting for the next check
  --plan         print what this run would look at, and stop. Asks the source
                 nothing (the portal's counts are read, nothing is written).

State lives in aaa_fetch_state.json beside this file and the workflow commits
it back: the day's request count, the windows already known to be complete, and
where the check of the newest sale days has got to.
"""
import argparse, datetime, io, json, os, re, ssl, sys, threading, time, urllib.parse, urllib.request, http.cookiejar

BASE = 'https://bid.aaajapan.com'
# Everything secret comes from the environment, so this file is safe in a public
# repo. GitHub Actions passes them from repository Secrets; locally, set them in
# the shell before running. No credential is ever in the code.
PORTAL = os.environ.get('AAA_INGEST_URL', 'https://auction.sbkautotrading.com/aaa-stats-ingest.php')
INGEST_TOKEN = os.environ.get('AAA_INGEST_TOKEN', '')
USER = os.environ.get('AAA_USER', '')
PW = os.environ.get('AAA_PASS', '')
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
# 1.5s, the same gap the auction harvester uses. It was 1.0 until 17 September
# 2026; the gap was never what got the first account closed, but there is no
# reason for this to be brisker than the feed we actually depend on.
GAP = 1.5
PAGE_ROWS = 20          # fixed at the source: list_size is accepted and ignored

# How many signed-in readers work at once (the owner's yes, 14 September 2026).
# They take windows from one shared list, so two can never read the same page.
WORKERS = int(os.environ.get('AAA_WORKERS', '3'))
# A round starts with ONE reader. Another signs in only when a reader has been on
# one window for EXTRA_AFTER seconds (a long read) while others wait - so a
# round of quick checks costs one sign-in (4 requests), not three (12).
EXTRA_AFTER = 20
EXTRA_GAP = 10          # at least this long between two extra sign-ins
POLL = 2.0              # how often the run looks at its readers
# The floor between ANY two requests to the source, across all readers - one
# clock for everyone (see the sbk-source-rate-safety note). Never lower it
# without the owner: a blocked source costs days and there is no appeal.
GLOBAL_MIN_GAP = 1.5

# ---------------------------------------------------------------------------
# HOW MUCH, not just how fast.
#
# The first account was closed on 16 September 2026 after this job pulled
# 915,048 rows - the source's whole price archive - in about forty hours. The
# gap was obeyed the entire time; there was no ceiling on the TOTAL. So: a day's
# allowance and a run's allowance, both counted in the one place every source
# request passes through. When either is spent the readers stop cleanly - the
# portal's counts say where to carry on, so nothing is lost. Both are repository
# Variables (AAA_DAILY_BUDGET, AAA_RUN_LIMIT); neither is raised without the owner.
DAILY_BUDGET = int(os.environ.get('AAA_DAILY_BUDGET', '10000'))
RUN_LIMIT    = int(os.environ.get('AAA_RUN_LIMIT', '600'))
# ONE day's catch-up, on that UTC day only (the owner, 25 September 2026: "start
# today, but safely"). The restart loop of 24-25 September left about 1.1 lakh
# rows unread; 6,000 more requests on this one day go to them - the SAME 1.5 s
# between requests and the same 600 a run, so no minute is any busier than any
# other day's, and tomorrow the allowance is back to 10,000 by itself.
CATCH_UP = {'2026-09-25': 6000}
DAILY_BUDGET += CATCH_UP.get(time.strftime('%Y-%m-%d', time.gmtime()), 0)

# ---------------------------------------------------------------------------
# THE ORDER THE SOURCE SERVES ROWS IN - and why, until 19 September 2026, this
# job kept asking for the wrong pages.
#
# A result list is sorted by MODEL NAME, A to Z - not by date. NISSAN's first
# 500 rows are its "180 SX" and "AD" rows from sixty-eight different sale days.
# The old fetcher believed "the newest sales sit on page one", so its freshness
# pass (pages 1-3 of every maker) and its daily top-up (pages 1-25) re-read the
# same alphabetical first pages thirty times over: on 19 September a whole run
# of 396 requests added NOTHING (in_db did not move once), the day's 10,000 were
# gone by 04:23 UTC, and every maker's last four sale days sat almost empty -
# NISSAN 18 Sep: 0 of 2,420, HONDA 17 Sep: 6 of 1,949, TOYOTA 17 Sep: 45 of 7,686.
# It could not notice, because it steered by the ingest's `written`, which
# counts re-read rows as well as new ones.
#
# And it is why TOYOTA stopped at "PRIUS". One query is served only to about
# 200,000 results; TOYOTA's unbounded list reached PRIUS and ended, so every
# TOYOTA sale day in the portal holds its models "86" to "PRIUS" and nothing
# after - RAV4, SIENTA, VOXY, YARIS are missing from all of them (~179,000 rows).
#
# So now: COMPARE COUNTS, and read only what is short.
#   * The portal's counts per maker and sale day come from the ingest (?have=).
#   * For a maker and a date window, page ONE from the source carries the total.
#     Equal or more on our side - the window is complete, move on.
#   * Short and small (READ_ALL pages or fewer) - read it.
#   * Short and big - split it into single sale days, each asked the same way.
#   * A day is read starting where our rows run out (`ours // 20`): the rows the
#     portal holds for a day are nearly always its A-to-Z beginning - for TOYOTA
#     exactly the "86".."PRIUS" part - so the missing ones follow them. If the
#     day is still short at the end, the pages before the start are read too,
#     BACKWARDS from the start: whatever was missed is most likely just behind it.
#   * Past windows found complete are remembered (`done`), so they cost nothing
#     afterwards; a window read to the end and still short is remembered too
#     (`short`) and not read again at that size.
#
# Two passes, the newest first:
#   RECENT   the last RECENT_DAYS sale days of every maker, every RECENT_EVERY.
#            While Japan's halls are selling (09:00-18:00 JST) today's list is
#            half-written, so it joins at 18:00 JST instead of being read twice.
#   HISTORY  everything older, back HISTORY_DAYS (the source keeps about 93),
#            a month at a time. It stops when only RESERVE requests of the day
#            are left, so the recent check always has something to spend.
RECENT_DAYS  = 5
RECENT_EVERY = 90 * 60
HISTORY_DAYS = 88
READ_ALL     = 30
RESERVE      = int(os.environ.get('AAA_RESERVE', '2000'))
# How short a window may stay without the pages before the start being read
# again: five rows for settled days (a source row that shares hall, day and lot
# with another is one row here, so a day can be one or two short for good). The
# newest days, which the source can still be adding to, wait for 2% - a late
# hall adds rows all through the A-Z list, and reading a 7,000-row day again for
# a handful would eat the day; the day is looked at once more when it settles.
SLACK        = 5
RECENT_SLACK = 0.02
JST          = datetime.timedelta(hours=9)

# ---------------------------------------------------------------------------
# WHAT THE 24 SEPTEMBER 2026 LOGS SHOWED, and the three rules that answer it.
#
# 1. A big day that needs more pages than one run may spend was read from the
#    same page every run. TOYOTA 27 Aug (about 8,500 rows) was short by a few
#    rows scattered through its A-Z list; reading backwards from where our rows
#    end takes ~420 pages, two readers share a run's 600, so each got ~300, the
#    run ended, and the next run started the day again - ten runs, 5,800 of the
#    day's 10,000 requests, 0 new rows. Now where a read stopped is kept
#    (state 'cursor': the source's count, the page to go on from, which way),
#    and the next run carries on from that page. A changed count starts afresh.
# 2. Pages that bring rows come first. Reading a day's A-Z tail fills about 20
#    rows a page; hunting a handful of rows backwards through a whole day fills
#    almost none. So in a round every window has its forward read first, and
#    the backward hunts wait until nothing else is left (`deferred`).
# 3. An empty answer from a session that answered WITH rows a moment ago is
#    not a lapsed session. The source's empty answers do not carry the member
#    mark, so every small maker with nothing in a window cost a fresh sign-in
#    (4 requests) and a second ask - about 20 of every recent check's 98. Now
#    it is believed for the moment: a recent window is simply looked at again
#    at the next check, a settled one again tomorrow - never filed empty for good.
FRESH_SECS   = 300


def today_utc():
    return time.strftime('%Y-%m-%d', time.gmtime())

ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE


class Blocked(Exception):
    """The source refused us. Everything stops until a person looks."""


class Spent(Exception):
    """The allowance is used up. Not an error - the job simply stops here."""


class Door(Blocked):
    """Turned away at the very first page, BEFORE we have said who we are.

    Nothing identifies us at that point but the address of the machine asking,
    so this is the source refusing that one GitHub machine - not the account.
    Seen 2026-09-18 00:00 UTC: one runner got 403 on /st?classic, the next run
    (another machine) four hours later signed in and worked all morning. A 403
    AFTER signing in is still a plain Blocked and still stops everything.
    """


class Stop(Exception):
    """This run's time is up, or another reader has called a halt."""


class Reserve(Exception):
    """History has had its share of today; the rest is kept for the newest days."""


class Fatal(Exception):
    """Something no retry will fix (e.g. the portal is an older ingest)."""


# How many runs in a row may be turned away at the door before the job stops
# asking and says so out loud (a failed run = an e-mail to the owner).
DOOR_LIMIT = 4
# After a refusal once signed in (or a fault no retry fixes) the job fails ONE
# run - one e-mail - and then leaves the source alone this long, instead of
# knocking again on every schedule. `--resume` ends the wait early.
HALT_HOURS = 24

# The portal shows staff a green or red signal for the aaajapan ID (the owner's
# request of 24 September 2026). It can only know what this job tells it, so
# every run - however it ends - reports how its sign-in went: see
# report_health() and the portal's aaa-stats-ingest.php ?health=1.
#   login: 'ok' | 'refused' (bad username or password) | 'noaccess' (signed in,
#          but statistics are not reachable) | 'door' (this machine turned away)
#          | '' (no sign-in this run - the allowance is spent, or a stop holds)
HEALTH = {'login': '', 'why': ''}
RUN = {}
HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, 'aaa_fetch_state.json')
RETRY_MARK = os.path.join(HERE, '.retry')
# Left by a run that ended cleanly and wants the chain to go on (see fetch.yml).
NEXT_MARK = os.path.join(HERE, '.next')


class Throttle(object):
    """One clock AND one purse for every reader.

    Every source request in this file goes through take(), so nothing can slip
    past either the gap or the day's allowance. The count is per UTC day and is
    carried in the state file, so a run that stops for any reason cannot hand
    the next run a clean slate for the same day.
    """

    def __init__(self, gap, daily=0, run_cap=0):
        self.gap = gap
        self.daily = daily
        self.run_cap = run_cap
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.day = today_utc()
        self.used = 0
        self.this_run = 0

    def load(self, saved):
        """Pick up what earlier runs spent today. Another day starts at nought."""
        with self.lock:
            self.day = today_utc()
            self.used = int(saved.get('used', 0) or 0) \
                if isinstance(saved, dict) and saved.get('day') == self.day else 0

    def snapshot(self):
        with self.lock:
            return {'day': self.day, 'used': self.used}

    def left(self):
        with self.lock:
            if today_utc() != self.day:
                return self.daily or -1
            return max(0, self.daily - self.used) if self.daily else -1

    def run_left(self):
        with self.lock:
            return max(0, self.run_cap - self.this_run) if self.run_cap else -1

    def take(self):
        """Claim one request, or raise Spent. Waits out the gap before returning."""
        with self.lock:
            if today_utc() != self.day:      # midnight passed while we were running
                self.day = today_utc()
                self.used = 0
            if self.daily and self.used >= self.daily:
                raise Spent("today's allowance of %s requests is spent"
                            % format(self.daily, ','))
            if self.run_cap and self.this_run >= self.run_cap:
                raise Spent('this run has used its %s requests'
                            % format(self.run_cap, ','))
            self.used += 1
            self.this_run += 1
            now = time.time()
            due = max(now, self.next_at)
            self.next_at = due + self.gap
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)


THROTTLE = Throttle(GLOBAL_MIN_GAP, DAILY_BUDGET, RUN_LIMIT)


def reserve_reached():
    """True once history has had its share of today's allowance."""
    left = THROTTLE.left()
    return bool(DAILY_BUDGET) and 0 <= left <= RESERVE


def new_opener():
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
                                       urllib.request.HTTPSHandler(context=ctx))


def aaa(op, url, data=None, ref='/st?classic'):
    THROTTLE.take()
    r = urllib.request.Request(url, data=(data.encode() if data else None),
                               headers={'User-Agent': UA, 'Referer': BASE + ref})
    try:
        with op.open(r, timeout=90) as x:
            return x.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        if e.code in (403, 429):
            raise Blocked('%s on %s' % (e.code, url))
        raise


def login():
    """Sign in the way the page's own form does.

    Two things changed on 16 September 2026 and both are worth keeping written
    down. The form moved: it posts to **/st** with `ref=st` (it was /aj_3), and
    the page's button first asks **m?name=login&file=xloader**, which answers
    `q: 1` for good details and the reason in words for bad ones. That answer is
    the quickest way to tell a dead account from a changed page.

    And the old success check was wrong: it accepted the page if the word
    "logout" appeared anywhere, and that word is in the navigation **even for a
    guest**. The check now looks for the two things we actually need - the maker
    list and the search form - and nothing else.
    """
    op = new_opener()
    try:
        aaa(op, BASE + '/st?classic')
    except Blocked as e:
        HEALTH.update(login='door', why=str(e)[:200])
        raise Door(str(e))
    except (urllib.error.URLError, OSError) as e:
        # The source did not answer at all (down, or this machine cannot reach
        # it). Same remedy as a refusal at the door: another machine, later.
        HEALTH.update(login='door', why=('no answer: %s' % str(e))[:200])
        raise Door('no answer: %s' % str(e)[:80])
    time.sleep(GAP)

    creds = urllib.parse.urlencode({'username': USER, 'password': PW,
                                    'is_login': '1', 'ref': 'st'})
    ans = aaa(op, BASE + '/m?name=login&file=xloader', creds, '/st?classic')
    q = re.search(r"'q'\s*:\s*'([^']*)'", ans)
    if q and q.group(1).strip() != '1':
        HEALTH.update(login='refused', why=q.group(1).strip()[:200])
        raise RuntimeError('the source refused these details: %s' % q.group(1).strip())
    time.sleep(GAP)

    aaa(op, BASE + '/st', creds, '/st?classic'); time.sleep(GAP)
    h = aaa(op, BASE + '/st?classic')
    mk = {}
    m = re.search(r'id=manuf_str[^>]*>([^<]*)<', h)
    for bit in (m.group(1).split(';') if m else []):
        p = bit.split(':', 1)
        if len(p) == 2 and p[0].strip() and p[1].strip().lower() != 'any':
            mk[p[0].strip()] = p[1].strip()
    i = h.find('<form id=poisk')
    form = {}
    if i >= 0:
        seg = h[i:h.find('</form>', i) + 7]
        for tag in re.findall(r'<input[^>]*>', seg):
            n = re.search(r'name=[\'"]?([\w\[\]]+)', tag)
            if n and not n.group(1).lower().startswith('lose_time_here'):
                v = re.search(r'value=(["\'])(.*?)\1', tag, re.S)
                form[n.group(1)] = v.group(2) if v else ''
    if not mk or not form:
        HEALTH.update(login='noaccess', why='signed in, but the statistics search is not on the page')
        raise RuntimeError('signed in, but the maker list and search form are not on '
                           'the page - the source has changed, or this account cannot '
                           'reach statistics')
    HEALTH.update(login='ok', why='')
    return op, form, mk


def page(op, form, vid, pg, d1='', d2=''):
    """One page of a maker's results, optionally inside a sale-date window.

    Returns (rows, navi); navi['rows'] is the window's total. RAISES when the
    answer is not a result list at all. The old version returned ([], {}) for
    that, which a count-driven reader would take for "this window is empty" and
    then file as complete for good - and a signed-out session answers exactly
    like that.
    """
    f = dict(form); f['vendor'] = str(vid); f['model'] = ''; f['page'] = str(max(1, pg))
    f['list_size'] = str(PAGE_ROWS); f['tpl'] = ''; f['is_stat'] = '0'
    # The sale-date window (YYYY-MM-DD, both ends included). `model` must stay
    # EMPTY - "Any", which is what the box shows, returns nothing at all.
    f['stDt1'] = d1; f['stDt2'] = d2
    url = BASE + '/st?file=loader&ajx=' + str(int(time.time() * 1000)) + '0-form'
    body = aaa(op, url, urllib.parse.urlencode(f))
    mm = re.search(r"'tpl_poisk':\s*'var data\s*=\s*(\{.*?\});'", body, re.S)
    if not mm:
        raise RuntimeError('the answer carried no result list (%d bytes)' % len(body))
    raw = mm.group(1).replace('\\"', '"').replace("\\'", "'").replace('\\/', '/')
    navi = {}
    nm = re.search(r'navi:\{(.*?)\},\s*body:', raw, re.S)
    if nm:
        navi = dict(re.findall(r'(\w+):"([^"]*)"', nm.group(1)))
    rows = []
    bm = re.search(r'body:\[(.*)\]\s*\}\s*;?\s*$', raw, re.S)
    if bm:
        for one in re.findall(r'\{a:"(?:[^"\\]|\\.)*".*?\}(?=,\{a:"|$)', bm.group(1), re.S):
            r = dict((k, v) for k, v in re.findall(r'(\w+):"((?:[^"\\]|\\.)*)"', one))
            if r:
                rows.append(r)
    return rows, navi


def total_of(navi):
    """The window's size as the source reports it."""
    v = str(navi.get('rows') or '0').strip()
    return int(v) if v.isdigit() else 0


def send(rows, maker):
    """POST one page to the portal. Raises with what the server actually said.

    The answer's `new` is what matters: the rows the table did not have before
    (`written` counts refreshed rows too). `new_days` splits `new` by sale day,
    which keeps this run's own tally of the portal's counts exact."""
    data = json.dumps({'maker': maker, 'rows': rows}).encode('utf-8')
    r = urllib.request.Request(PORTAL + '?t=' + INGEST_TOKEN, data=data,
                               headers={'User-Agent': 'aaa-fetch', 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(r, timeout=90, context=ctx) as x:
            status, raw = x.status, x.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    body = raw.decode('utf-8', 'replace')
    try:
        return json.loads(body)
    except ValueError:
        snippet = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', body)).strip()[:200]
        raise RuntimeError('portal answered HTTP %s, not JSON: %s' % (status, snippet or '(empty body)'))


def portal_have(d1, d2):
    """What the portal holds per maker and sale day - {'TOYOTA|2026-09-18': n}.

    Our own server, our own table: costs the source nothing."""
    url = PORTAL + '?t=' + INGEST_TOKEN + '&have=days&from=%s&to=%s' % (d1, d2)
    why = ''
    for attempt in range(3):
        try:
            r = urllib.request.Request(url, headers={'User-Agent': 'aaa-fetch'})
            with urllib.request.urlopen(r, timeout=120, context=ctx) as x:
                j = json.loads(x.read().decode('utf-8', 'replace'))
            if j.get('ok') and isinstance(j.get('have'), dict):
                return dict((k.upper(), int(v)) for k, v in j['have'].items())
            why = 'unexpected answer: %s' % str(j)[:120]
        except Exception as e:
            why = str(e)[:120]
        time.sleep(10 * (attempt + 1))
    raise RuntimeError('the portal would not say what it holds: %s' % why)


def report_health(error=''):
    """Tell the portal how the aaajapan ID fared this run - it turns that into the
    green or red signal staff see on the Statistics page.

    Sent at the end of EVERY run, however it ended, so a silence the portal can
    measure means GitHub stopped running this. Nothing here may break a run: a
    report that cannot be delivered is printed and forgotten."""
    try:
        s = RUN.get('s') or {}
        tally = RUN.get('tally') or {}
        halted = s.get('halted') or {}
        if not isinstance(halted, dict):
            halted = {'why': str(halted), 'at': 0}
        body = {
            'login': HEALTH['login'],
            'why': (HEALTH['why'] or str(error))[:200],
            'halted': str(halted.get('why', '') or '')[:300],
            'halted_at': int(float(halted.get('at', 0) or 0)),
            'door': int(s.get('door', 0) or 0),
            'spent': bool(DAILY_BUDGET) and THROTTLE.left() == 0,
            'used': int(THROTTLE.snapshot().get('used', 0)),
            'budget': DAILY_BUDGET,
            'pages': int(tally.get('pages', 0)),
            'new': int(tally.get('new', 0)),
            'run': os.environ.get('GITHUB_RUN_ID', ''),
        }
        r = urllib.request.Request(PORTAL + '?t=' + INGEST_TOKEN + '&health=1',
                                   data=json.dumps(body).encode('utf-8'),
                                   headers={'User-Agent': 'aaa-fetch', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(r, timeout=30, context=ctx) as x:
            ok = json.loads(x.read().decode('utf-8', 'replace')).get('ok')
        print('ID signal to the portal: login=%s halted=%s spent=%s -> %s'
              % (body['login'] or '-', body['halted'] or '-', body['spent'], 'kept' if ok else 'NOT kept'), flush=True)
    except Exception as e:
        print('ID signal not delivered (%s) - the portal turns red if this goes on' % str(e)[:120], flush=True)


# ------------------------------------------------------------------- dates
def jst_now(now=None):
    """Japan's clock - the sale dates are Japan's dates."""
    return datetime.datetime.fromtimestamp(now or time.time(), datetime.timezone.utc) + JST


def windows(now=None):
    """(recent_from, recent_to, history_from, history_to) as dates."""
    t = jst_now(now)
    today = t.date()
    # 09:00-18:00 JST the halls are still selling and today's list is still
    # being written; it is read once it is finished, not every hour while it grows.
    last = today - datetime.timedelta(days=1) if 9 <= t.hour < 18 else today
    r_from = today - datetime.timedelta(days=RECENT_DAYS - 1)
    h_to = r_from - datetime.timedelta(days=1)
    h_from = today - datetime.timedelta(days=HISTORY_DAYS)
    return r_from, last, h_from, h_to


def next_change(now=None):
    """Seconds until the recent window next moves (00:00, 09:00, 18:00 JST)."""
    t = jst_now(now)
    for h in (9, 18, 24):
        if t.hour < h:
            edge = t.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(hours=h)
            return (edge - t).total_seconds()
    return 3600.0


def month_chunks(a, b):
    """[a, b] cut at month ends, newest first."""
    out = []
    end = b
    while end >= a:
        first = end.replace(day=1)
        out.append((max(first, a), end))
        end = first - datetime.timedelta(days=1)
    return out


def days_newest(a, b):
    d = b
    while d >= a:
        yield d
        d -= datetime.timedelta(days=1)


def iso(d):
    return d.isoformat() if hasattr(d, 'isoformat') else str(d)


def as_date(s):
    return s if isinstance(s, datetime.date) else datetime.date.fromisoformat(str(s))


# -------------------------------------------------------------------- state
OLD_KEYS = ('mIdx', 'page', 'sweeps', 'fIdx', 'fPage', 'fresh_at', 'w', 'totals')


def load_state():
    s = {}
    if os.path.exists(STATE):
        try:
            s = json.load(io.open(STATE, encoding='utf-8'))
        except Exception:
            s = {}
    # The page cursors of the old blind walk mean nothing to a count-driven
    # reader; carrying them would only make the file lie about what it does.
    for k in OLD_KEYS:
        s.pop(k, None)
    for k in ('done', 'short', 'rshort', 'later', 'cursor'):
        if not isinstance(s.get(k), dict):
            s[k] = {}
    s.setdefault('sent', 0)
    s.setdefault('added', 0)
    return s


def save_state(s):
    # The purse is written with everything else, every time, so however a run
    # ends the next one knows what today has already cost.
    s['budget'] = THROTTLE.snapshot()
    tmp = STATE + '.tmp'
    io.open(tmp, 'w', encoding='utf-8').write(json.dumps(s, sort_keys=True))
    os.replace(tmp, STATE)


def prune(s, r_from, h_from):
    """Forget windows the source no longer keeps, and recent notes that have aged."""
    old = iso(h_from - datetime.timedelta(days=7))
    for k in ('done', 'short'):
        for key in [x for x in s[k] if x.split('|')[-1] < old]:
            del s[k][key]
    for key in [x for x in s['rshort'] if x.split('|')[1] < iso(r_from)]:
        del s['rshort'][key]
    for key in [x for x, t in s['later'].items() if time.time() - t >= 86400]:
        del s['later'][key]
    # A read left half-way is carried on for three days; after that it starts afresh.
    for key in [x for x, c in s['cursor'].items()
                if x.split('|')[-1] < old or time.time() - float((c or {}).get('t', 0)) >= 3 * 86400]:
        del s['cursor'][key]


# ------------------------------------------------------------------- ledger
class Ledger(object):
    """The portal's count per maker and sale day - kept current as pages go in."""

    def __init__(self, have):
        self.lock = threading.Lock()
        self.have = dict(have)

    def count(self, name, d1, d2):
        a, b = as_date(d1), as_date(d2)
        n = 0
        with self.lock:
            while a <= b:
                n += int(self.have.get('%s|%s' % (name, a.isoformat()), 0))
                a += datetime.timedelta(days=1)
        return n

    def add(self, name, per_day):
        with self.lock:
            for d, c in (per_day or {}).items():
                k = '%s|%s' % (name, d)
                self.have[k] = int(self.have.get(k, 0)) + int(c)

    def size(self, name):
        p = name + '|'
        with self.lock:
            return sum(v for k, v in self.have.items() if k.startswith(p))


# ---------------------------------------------------------------------- job
class Job(object):
    """What the readers of one run share: the task list, the state, the tally."""

    def __init__(self, s, names, ledger, a, began, tally):
        self.s, self.names, self.ledger, self.a, self.began = s, names, ledger, a, began
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.halted = None
        self.blocked = False
        self.fatal = False
        self.history_off = False
        self.tasks, self.ti = [], 0
        # Rule 2: windows whose forward read is done but which still need the
        # pages before it wait here until every window has had its forward read.
        self.allow_back = False
        self.deferred = []
        self.first = {}         # window -> the source's count, from its page 1 earlier this round
        self.busy = {}          # reader id -> when it took the window it is on
        self.failed = set()     # windows that raised this round - not retried in this run
        self.bad_streak = 0     # windows in a row that failed; three stop the run
        self.doubts = 0         # fresh sign-ins spent on doubtful "none" answers
        # Shared by every round of the run: rows the portal did not have, and
        # source pages read (sign-ins not included).
        self.tally = tally
        self.dirty = 0

    def say(self, msg):
        print(msg, flush=True)

    def saw(self, makers):
        with self.lock:
            self.s['makers'] = dict(makers)
            for k, v in makers.items():
                self.names.setdefault(k, v)

    def guard(self, settled):
        if self.stop.is_set():
            raise Stop()
        if self.a.max_seconds and time.time() - self.began > self.a.max_seconds:
            raise Stop()
        if settled and (self.history_off or reserve_reached()):
            raise Reserve()

    def take(self):
        with self.lock:
            while True:
                while self.ti < len(self.tasks):
                    t = self.tasks[self.ti]
                    self.ti += 1
                    if t[0] == 'H' and self.history_off:
                        continue
                    return t
                if self.deferred and not self.allow_back:
                    # Every window has had its forward read: now the backward hunts.
                    self.allow_back = True
                    self.tasks.extend(self.deferred)
                    self.deferred = []
                    continue
                return None

    def defer(self, task):
        """A window that only the pages before its start can complete - later in the round."""
        with self.lock:
            self.bad_streak = 0
            if self.allow_back:
                self.tasks.append(task)      # the hunts have begun meanwhile: join them
            else:
                self.deferred.append(task)

    def cursor(self, key, n):
        """Where an unfinished read of this window stopped - while the source still counts it the same."""
        with self.lock:
            c = self.s['cursor'].get(key)
            if isinstance(c, dict) and int(c.get('n', -1)) == int(n):
                return dict(c)
            self.s['cursor'].pop(key, None)
            return None

    def cursor_dir(self, key):
        with self.lock:
            return (self.s['cursor'].get(key) or {}).get('dir')

    def set_cursor(self, key, n, start, nxt, direction):
        with self.lock:
            self.s['cursor'][key] = {'n': int(n), 'start': int(start), 'next': int(nxt),
                                     'dir': direction, 't': int(time.time())}

    def drop_cursor(self, key):
        with self.lock:
            self.s['cursor'].pop(key, None)

    def give_back(self, t):
        with self.lock:
            self.ti -= 1
            self.tasks[self.ti] = t

    def may_doubt(self):
        """At most five fresh sign-ins a round for a doubtful "none" (20 requests):
        past that, a doubtful window is simply looked at again tomorrow."""
        with self.lock:
            if self.doubts >= 5:
                return False
            self.doubts += 1
            return True

    def pending(self):
        """Windows nobody has taken yet (history ones only while history may run)."""
        with self.lock:
            return (any(t[0] == 'R' or not self.history_off for t in self.tasks[self.ti:])
                    or bool(self.deferred and not self.history_off))

    def long_busy(self, secs):
        with self.lock:
            return any(time.time() - t0 >= secs for t0 in self.busy.values())

    def halt(self, why, blocked=False, fatal=False):
        with self.lock:
            if self.halted is None:
                self.halted = why
            self.blocked = self.blocked or blocked
            self.fatal = self.fatal or fatal
            save_state(self.s)
        self.stop.set()

    def took(self, name, res):
        with self.lock:
            k = int(res.get('new') or 0)
            self.tally['new'] += k
            self.s['added'] = int(self.s.get('added', 0)) + k
            self.s['sent'] = int(self.s.get('sent', 0)) + int(res.get('written') or 0)
        self.ledger.add(name.upper(), res.get('new_days') or {})

    def counted(self):
        with self.lock:
            self.tally['pages'] += 1

    def known(self, key, ours):
        """What is already known of a settled window: 'done', 'short', 'unsure'
        (a doubtful answer, looked at again after a day) - or None."""
        with self.lock:
            d = self.s['done'].get(key)
            if d is not None and ours >= d:
                return 'done'
            if key in self.s['short']:
                return 'short'
            t = self.s['later'].get(key)
            if t is not None and time.time() - t < 86400:
                return 'unsure'
            return None

    def rshort(self, key):
        with self.lock:
            return self.s['rshort'].get(key)

    def mark(self, key, kind, n):
        with self.lock:
            self.s['cursor'].pop(key, None)          # settled either way: no read to carry on
            for k in ('done', 'short', 'rshort', 'later'):
                if k != kind:
                    self.s[k].pop(key, None)
            self.s[kind][key] = int(n)
            self.dirty += 1
            if self.dirty >= 10:
                save_state(self.s)
                self.dirty = 0

    def finished(self, task):
        """A recent task that ran to its end comes off the pass's list."""
        with self.lock:
            self.bad_streak = 0
        if task[0] != 'R':
            return
        with self.lock:
            rp = self.s.get('recent') or {}
            left = rp.get('left') or []
            if task[1] in left:
                left.remove(task[1])
            if not left and rp:
                self.say('---- the newest sale days are checked for every maker (%s .. %s) ----'
                         % tuple(rp.get('window', '|').split('|')))
            save_state(self.s)


# ------------------------------------------------------------------- reader
class Reader(object):
    """One signed-in session and the reading it does. One per worker."""

    def __init__(self, tag, job, signed=None):
        self.tag, self.job = tag, job
        self.alive = 0.0            # when this session last answered WITH rows (rule 3)
        self.op, self.form, makers = signed or login()
        job.saw(makers)

    def fresh(self):
        return time.time() - self.alive < FRESH_SECS

    def looks_ours(self, navi):
        """Is this empty answer marked the way answers WITH rows are for us?

        A lapsed session answers "nothing here" too, and filing a full window as
        empty for good would lose it. So an empty answer is trusted only when it
        carries the same `is_user` mark our answers with rows carry (remembered
        in the state across runs); anything else is only noted for a day."""
        mark = self.job.s.get('member')
        return mark is not None and navi.get('is_user', '') == mark

    def ask(self, vid, pg, d1, d2, settled, want=False):
        """One page from the source, with a fresh sign-in after a bad answer.

        `want`: the window's own count says this page has rows. An empty one is
        then a bad answer, not the end of the list - taking it for the end would
        file a half-read day as read."""
        for attempt in range(3):
            self.job.guard(settled)
            try:
                rows, navi = page(self.op, self.form, vid, pg, iso(d1), iso(d2))
                self.job.counted()
                if want and not rows:
                    raise RuntimeError('page %d came back empty' % pg)
                if rows:
                    if total_of(navi) < len(rows):
                        raise RuntimeError('an answer with rows but no total')
                    self.job.s['member'] = navi.get('is_user', '')
                    self.alive = time.time()
                return rows, navi
            except (Blocked, Spent):
                raise
            except Exception as e:
                self.relogin('bad answer: %s (%s p%d %s..%s)'
                             % (str(e)[:80], self.job.names.get(vid, vid), pg, iso(d1), iso(d2)))
        raise RuntimeError('three bad answers in a row')

    def relogin(self, why):
        self.job.say('[%s] %s - signing in again' % (self.tag, why))
        time.sleep(5)
        try:
            self.op, self.form, makers = login()
        except (Blocked, Spent):
            raise           # a refusal here is a real refusal: everyone stops
        except Exception as e:
            # Signed in once this run and cannot any more - the account, or the
            # page, has changed. Retrying would only spend requests.
            raise Fatal('could not sign in again: %s' % str(e)[:100])
        self.job.saw(makers)

    def put(self, rows, name):
        """Forward one page; returns how many of its rows the portal did not have."""
        if not rows:
            return 0
        for attempt in range(1, 9):
            try:
                res = send(rows, name)
                if not res.get('ok', False):
                    raise RuntimeError('portal said: %s' % str(res)[:160])
                if 'new' not in res:
                    raise Fatal('the portal is an older ingest - it does not say which rows are new')
                self.job.took(name, res)
                return int(res.get('new') or 0)
            except Fatal:
                raise
            except Exception as e:
                # One page must never hold a reader: back off, then let it go.
                self.job.say('[%s] ingest error (try %d) %s' % (self.tag, attempt, str(e)[:160]))
                time.sleep(min(60, 5 * attempt))
        self.job.say('[%s]   giving up on that page for now - its window stays short and '
                     'is read again later' % self.tag)
        return 0

    def fill(self, vid, d1, d2, settled, retried=False):
        """Make the portal's count for [d1, d2] match the source's, reading as little as possible.

        Returns 'done', 'short' or 'unsure'. A window split into days is filed
        only when every day gave a straight answer; a doubtful day leaves the
        whole window to be looked at again in a day, never filed short for good."""
        job = self.job
        name = job.names[vid]
        up = name.upper()
        key = '%s|%s|%s' % (vid, iso(d1), iso(d2))
        ours = job.ledger.count(up, d1, d2)
        if settled:
            known = job.known(key, ours)
            if known:
                return known
            if not job.allow_back and job.cursor_dir(key) == 'b':
                return 'deferred'       # its forward read is done; the rest waits (rule 2)
        seen = job.first.get(key)
        if seen:
            # Its page 1 was read earlier this round (a window put back by rule 2):
            # the count is known and its rows are in - no need to ask again.
            n, navi = seen, {'rows': str(seen)}
        else:
            rows, navi = self.ask(vid, 1, d1, d2, settled)
            n = total_of(navi)
            self.put(rows, name)
            job.first[key] = n
        ours = job.ledger.count(up, d1, d2)
        if ours >= n:
            if n > 0:
                if settled:
                    job.mark(key, 'done', n)
                return 'done'
            if ours == 0 and not self.looks_ours(navi) and self.fresh():
                # Rule 3: nothing here, from a session that answered with rows
                # moments ago. Believed for now, never filed as empty for good.
                if settled:
                    job.mark(key, 'later', int(time.time()))
                    return 'unsure'
                return 'done'
            if (ours > 0 or not self.looks_ours(navi)) and not retried and job.may_doubt():
                # "None" for days the source still keeps and we hold rows for - or
                # a "none" not marked the way our answers are - is far more likely
                # a lapsed session than the truth. Sign in afresh and ask again.
                self.relogin('the source said none (%s %s..%s, we hold %s)'
                             % (name, iso(d1), iso(d2), format(ours, ',')))
                return self.fill(vid, d1, d2, settled, retried=True)
            if ours == 0 and self.looks_ours(navi):
                if settled:
                    job.mark(key, 'done', 0)     # empty there and here, answered like ours
                return 'done'
            if settled:
                job.mark(key, 'later', int(time.time()))
            return 'unsure'
        if not settled and job.rshort(key) == n:
            return 'short'      # read to the end at exactly this size already
        last = -(-n // PAGE_ROWS)
        if last > READ_ALL and d1 != d2:
            doubt = waits = False
            for d in days_newest(d1, d2):
                r = self.fill(vid, d, d, settled)
                doubt = doubt or r == 'unsure'
                waits = waits or r == 'deferred'
            ours = job.ledger.count(up, d1, d2)
            if ours >= n:
                if settled:
                    job.mark(key, 'done', n)
                return 'done'
            if waits:
                return 'deferred'       # a day of it waits for its backward read
            if doubt:
                if settled:
                    job.mark(key, 'later', int(time.time()))
                return 'unsure'
            job.mark(key, 'short' if settled else 'rshort', n)
            return 'short'
        return self.read(vid, name, key, d1, d2, n, settled)

    def read(self, vid, name, key, d1, d2, n, settled):
        """Read one short window, starting where our rows run out - or where the
        last run stopped reading it (rule 1)."""
        job = self.job
        up = name.upper()
        last = -(-n // PAGE_ROWS)
        before = job.ledger.count(up, d1, d2)
        cur = job.cursor(key, n)
        start = min(last, max(2, int(cur['start']))) if cur else max(2, min(last, before // PAGE_ROWS))
        slack = SLACK if settled else max(SLACK, int(n * RECENT_SLACK))
        asked = [1]

        def sweep(pages, way):
            for pg in pages:
                job.set_cursor(key, n, start, pg, way)       # stopped here, the next run starts here
                rows, navi = self.ask(vid, pg, d1, d2, settled, want=True)
                asked[0] += 1
                self.put(rows, name)
                if job.ledger.count(up, d1, d2) >= n:
                    return True
            return False

        whole = False
        if not cur or cur.get('dir') == 'f':
            whole = sweep(range(int(cur['next']) if cur else start, last + 1), 'f')
        ours = job.ledger.count(up, d1, d2)
        # A hunt already under way goes on to its end: the slack decides whether
        # one is begun, not whether one that found some rows is abandoned half-way.
        hunting = bool(cur and cur.get('dir') == 'b' and int(cur['next']) < start - 1)
        if not whole and start > 2 and (hunting or n - ours > slack):
            if settled and not job.allow_back:
                # Rule 2: the pages before the start hold a few rows at most; they
                # are read once every window has had its forward read.
                job.set_cursor(key, n, start, start - 1, 'b')
                return 'deferred'
            back = int(cur['next']) if (cur and cur.get('dir') == 'b') else start - 1
            whole = sweep(range(min(back, start - 1), 1, -1), 'b')
            ours = job.ledger.count(up, d1, d2)
        if ours >= n:
            if settled:
                job.mark(key, 'done', n)
            else:
                job.drop_cursor(key)
        else:
            job.mark(key, 'short' if settled else 'rshort', n)
        span = iso(d1) if d1 == d2 else '%s..%s' % (iso(d1)[5:], iso(d2)[5:])
        job.say('[%s] %-14s %-12s | source %6s | had %6s -> %6s | %d pages%s'
                % (self.tag, name, span, format(n, ','), format(before, ','), format(ours, ','),
                   asked[0], '' if ours >= n else ' | still %s short' % format(n - ours, ',')))
        return 'done' if ours >= n else 'short'


def worker(wid, job, sessions):
    tag = 'w%d' % wid
    while not job.stop.is_set():
        task = job.take()
        if task is None:
            return
        rd = sessions.get(wid)
        if rd is None:
            try:
                rd = Reader(tag, job)
                sessions[wid] = rd
            except Blocked as e:
                # Moments after the run's first sign-in worked from this same
                # machine, so this is a real refusal, not a stray address.
                job.say('[%s] BLOCKED at sign-in: %s -- stopping every reader.' % (tag, e))
                job.halt('blocked at sign-in', blocked=True)
                return
            except Spent as e:
                job.say('[%s] %s.' % (tag, e))
                job.halt(str(e))
                return
            except Exception as e:
                job.say('[%s] sign-in failed: %s - leaving the work to the others' % (tag, str(e)[:80]))
                job.give_back(task)
                return
        kind, vid, d1, d2 = task
        with job.lock:
            job.busy[wid] = time.time()
        try:
            if rd.fill(vid, d1, d2, kind == 'H') == 'deferred':
                job.defer(task)
            else:
                job.finished(task)
        except Spent as e:
            job.say('[%s] %s. Stopping - the rest waits for the next run.' % (tag, e))
            job.halt(str(e))
            return
        except Blocked as e:
            job.say('[%s] BLOCKED: %s -- the source is refusing us. Stopping every reader.' % (tag, e))
            job.halt('blocked', blocked=True)
            return
        except Reserve:
            if not job.history_off:
                job.history_off = True
                job.say('[%s] history has had its share of today (%s requests are kept for the '
                        'newest days) - it carries on tomorrow' % (tag, format(RESERVE, ',')))
        except Stop:
            return
        except Fatal as e:
            job.say('[%s] %s -- stopping.' % (tag, e))
            job.halt(str(e), fatal=True)
            return
        except Exception as e:
            with job.lock:
                job.failed.add(task)
                job.bad_streak += 1
                streak = job.bad_streak
            job.say('[%s] %s %s..%s set aside for this run: %s'
                    % (tag, job.names.get(vid, vid), iso(d1), iso(d2), str(e)[:100]))
            if streak >= 3:
                # Something systematic (the page changed, the account can no
                # longer see data). Going on would spend the day on nothing.
                job.say('[%s] three windows in a row failed -- stopping so a person looks.' % tag)
                job.halt('three windows in a row failed', fatal=True)
                return
        finally:
            with job.lock:
                job.busy.pop(wid, None)


# --------------------------------------------------------------------- plan
def plan(job, now, only=''):
    """The windows worth a request right now: recent ones first, then history."""
    s = job.s
    r_from, r_to, h_from, h_to = windows(now)
    vids = sorted(job.names, key=lambda v: (-job.ledger.size(job.names[v].upper()), str(v)))
    if only:
        vids = [v for v in vids if str(v) == str(only)]
    tasks = []

    if only:
        # Trying one maker by hand must not disturb the shared recent pass.
        tasks += [('R', v, r_from, r_to) for v in vids]
    else:
        win = '%s|%s' % (iso(r_from), iso(r_to))
        rp = s.get('recent') or {}
        if rp.get('window') != win or (not rp.get('left') and now - float(rp.get('at', 0)) >= RECENT_EVERY):
            rp = {'window': win, 'at': now, 'left': list(vids)}
            s['recent'] = rp
        left = set(rp.get('left') or [])
        tasks += [('R', v, r_from, r_to) for v in vids if v in left]

    # History waits while today's reserve is all that is left.
    if not reserve_reached():
        for a, b in month_chunks(h_from, h_to):
            for v in vids:
                key = '%s|%s|%s' % (v, iso(a), iso(b))
                if not job.known(key, job.ledger.count(job.names[v].upper(), a, b)):
                    tasks.append(('H', v, a, b))
    return tasks


def seconds_to_next(s, now):
    """How long until there is something to ask the source again."""
    rp = s.get('recent') or {}
    due = 0.0 if rp.get('left') else float(rp.get('at', 0)) + RECENT_EVERY - now
    wait = min(due, next_change(now))
    if THROTTLE.left() == 0:
        wait = max(wait, 86400 - (now % 86400) + 30)     # the allowance comes back at 00:00 UTC
    return max(0.0, wait)


# ---------------------------------------------------------------------- run
def sign_in_first(s):
    """The run's first sign-in, which is where a refusal at the door shows up."""
    try:
        signed = login()
    except Door as e:
        door = int(s.get('door', 0) or 0) + 1
        s['door'] = door
        save_state(s)                   # the refused request still counts against today
        if door < DOOR_LIMIT:
            io.open(RETRY_MARK, 'w').write('door')
            print('turned away at the door (%s). That is the address of this GitHub machine, not the '
                  'account - asking again from another machine (%d of %d).'
                  % (e, door, DOOR_LIMIT - 1), flush=True)
            return None
        if door == DOOR_LIMIT:
            print('turned away at the door %d runs in a row (%s). Stopping loudly so a person looks.'
                  % (door, e), flush=True)
            sys.exit(1)
        print('still turned away at the door (%d runs in a row; the owner was told at %d). '
              'Trying again on the next schedule.' % (door, DOOR_LIMIT), flush=True)
        return None
    if s.get('door'):
        print('in again after %d refusal(s) at the door' % int(s['door']), flush=True)
    s['door'] = 0
    return signed


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument('--maker', default='')
    ap.add_argument('--max-seconds', type=int, default=0)
    ap.add_argument('--workers', type=int, default=WORKERS)
    ap.add_argument('--once', action='store_true')
    ap.add_argument('--plan', action='store_true')
    ap.add_argument('--resume', action='store_true', help='end a stop after a refusal, once a person has looked')
    a = ap.parse_args()

    if not INGEST_TOKEN:
        sys.exit('AAA_INGEST_TOKEN must be set - the portal counts are behind it.')
    if not (USER and PW) and not a.plan:
        HEALTH.update(login='nocreds', why='AAA_USER / AAA_PASS are not set on GitHub')
        sys.exit('AAA_USER and AAA_PASS must be set in the environment.')
    for mark in (NEXT_MARK, RETRY_MARK):
        if os.path.exists(mark):
            os.remove(mark)

    began = time.time()
    s = load_state()
    RUN['s'] = s                        # report_health() reads the stop and the door count from here
    THROTTLE.load(s.get('budget'))
    if a.resume and s.pop('halted', None):
        print('resumed by hand', flush=True)
    stopped = s.get('halted')
    if stopped and not a.plan:
        since = time.time() - float(stopped.get('at', 0) or 0)
        if since < HALT_HOURS * 3600:
            print('stopped %.1f hours ago: %s. The source is left alone for %d hours after that - '
                  'or run with --resume once a person has looked.'
                  % (since / 3600.0, stopped.get('why', '?'), HALT_HOURS), flush=True)
            return
        print('%d hours since the stop (%s) - trying once more' % (HALT_HOURS, stopped.get('why', '?')), flush=True)
        s.pop('halted', None)
    names = dict(s.get('makers') or {})
    tally = {'new': 0, 'pages': 0}
    RUN['tally'] = tally
    sessions = {}           # worker id -> Reader, kept for the whole run
    first = None            # the run's first sign-in, before a Reader exists for it
    job = None
    halted = None
    blocked = fatal = False
    failed = set()
    rounds = 0

    def time_left():
        return (a.max_seconds - (time.time() - began)) if a.max_seconds else 1e9

    while time_left() > 30:
        now = time.time()
        r_from, r_to, h_from, h_to = windows(now)
        prune(s, r_from, h_from)
        tasks = []
        if THROTTLE.left() == 0 and not a.plan:
            if rounds == 0:
                print("today's %s requests are spent - nothing asks the source until 00:00 UTC."
                      % format(DAILY_BUDGET, ','), flush=True)
        else:
            if not names:
                if a.plan:
                    sys.exit('no maker list yet - it is learnt at the first sign-in')
                first = sign_in_first(s)
                if first is None:
                    return
                names = dict(first[2])
                s['makers'] = dict(names)
            try:
                have = portal_have(iso(h_from), iso(r_to + datetime.timedelta(days=1)))
            except RuntimeError as e:
                # Without the portal's counts there is nothing to compare, and
                # nowhere to put rows either. End quietly; the schedule tries again.
                print('%s - ending this run without asking the source anything.' % e, flush=True)
                save_state(s)
                return
            ledger = Ledger(have)
            job = Job(s, names, ledger, a, began, tally)
            tasks = [t for t in plan(job, now, a.maker) if t not in failed]

            if a.plan:
                r = [t for t in tasks if t[0] == 'R']
                h = [t for t in tasks if t[0] == 'H']
                print('recent window %s .. %s: %d makers to check | history %s .. %s: %d windows not '
                      'yet known complete | today %s of %s requests used'
                      % (r_from, r_to, len(r), h_from, h_to, len(h),
                         format(THROTTLE.snapshot()['used'], ','), format(DAILY_BUDGET, ',')))
                for t in r[:12]:
                    print('   R %-14s %s .. %s  portal holds %s' % (names[t[1]], t[2], t[3],
                          format(ledger.count(names[t[1]].upper(), t[2], t[3]), ',')))
                for t in h[:40]:
                    print('   H %-14s %s .. %s  portal holds %s' % (names[t[1]], t[2], t[3],
                          format(ledger.count(names[t[1]].upper(), t[2], t[3]), ',')))
                return

        if tasks:
            if 0 not in sessions:
                if first is None:
                    first = sign_in_first(s)
                    if first is None:
                        return
                sessions[0] = Reader('w0', job, signed=first)
            for rd in sessions.values():
                rd.job = job
            nr = len([t for t in tasks if t[0] == 'R'])
            want = max(1, min(a.workers, len(tasks)))
            print('round %d | %d recent + %d history windows | up to %d readers | gap %.1fs | today %s of %s '
                  'used, %s per run'
                  % (rounds + 1, nr, len(tasks) - nr, want, GLOBAL_MIN_GAP,
                     format(THROTTLE.snapshot()['used'], ','), format(DAILY_BUDGET, ','),
                     format(RUN_LIMIT, ',')), flush=True)
            job.tasks = tasks
            threads = []

            def spawn():
                t = threading.Thread(target=worker, args=(len(threads), job, sessions),
                                     name='w%d' % len(threads), daemon=True)
                threads.append(t)
                t.start()

            spawn()
            last_spawn = time.time()
            while True:
                alive = [t for t in threads if t.is_alive()]
                if not alive:
                    break
                alive[0].join(timeout=POLL)
                if (len(threads) < want and not job.stop.is_set() and job.pending()
                        and time.time() - last_spawn >= EXTRA_GAP and job.long_busy(EXTRA_AFTER)):
                    spawn()
                    last_spawn = time.time()
            rounds += 1
            failed |= job.failed
            save_state(s)
            if job.halted:
                halted, blocked, fatal = job.halted, job.blocked, job.fatal
                break
            if job.ti < len(job.tasks) and not job.history_off:
                break           # readers stopped early (time, or none could sign in) - never spin
            if a.once:
                break
            continue            # look again: the answer is usually "nothing more for now"

        if a.once or not a.max_seconds:
            break
        # Nothing to ask for now. Wait inside this run for the next check of the
        # newest days if it comes before the run's end; otherwise end here, and
        # the workflow starts the next run (.next) - which asks the source
        # nothing until there is something to ask.
        wait = seconds_to_next(s, time.time())
        if wait > time_left() - 90:
            time.sleep(max(0, time_left() - 60))
            break
        print('nothing to ask for now - next look in %d min' % (wait // 60 + 1), flush=True)
        time.sleep(wait + 5)

    save_state(s)
    snap = THROTTLE.snapshot()
    print('stopped%s | this run: %s source requests, %s pages read, %s new rows (%.1f per page) | '
          'today %s of %s used | windows known complete %s, read to the end and short %s'
          % ((' (%s)' % halted) if halted else '',
             format(THROTTLE.this_run, ','), format(tally['pages'], ','), format(tally['new'], ','),
             (tally['new'] / float(tally['pages'])) if tally['pages'] else 0.0,
             format(snap['used'], ','), format(DAILY_BUDGET, ','),
             format(len(s['done']), ','), format(len(s['short']), ',')), flush=True)

    if blocked or fatal:
        s['halted'] = {'why': halted, 'at': time.time()}
        save_state(s)
        print('this run fails on purpose, so the owner hears about it once; the source is now left '
              'alone for %d hours.' % HALT_HOURS, flush=True)
        sys.exit(1)
    # Ask for the next run only after a clean, full-length run. A crash, a
    # refusal or a short test run leaves no mark, so nothing can spin.
    if time.time() - began >= 180:
        io.open(NEXT_MARK, 'w').write('next')


if __name__ == '__main__':
    # However the run ends - done, stopped, refused, or an error - the portal
    # hears how the ID fared (a --plan run signs in to nothing and says nothing).
    failure = ''
    try:
        run()
    except SystemExit as e:
        failure = '' if e.code in (None, 0) else str(e.code)
        raise
    except BaseException as e:
        failure = '%s: %s' % (type(e).__name__, e)
        raise
    finally:
        if '--plan' not in sys.argv:
            report_health(failure)
