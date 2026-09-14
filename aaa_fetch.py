# -*- coding: utf-8 -*-
"""Fetch bid.aaajapan.com sales statistics and forward them to the SBK portal.

  python aaa_fetch.py [--recent N] [--maker ID] [--max-seconds S] [--once]

This runs on a machine aaajapan does NOT block (the owner's PC, or a small
always-on cloud box) - NOT the SBK server, whose IP aaajapan refuses. It logs in,
pages through each maker's results, and POSTs each page to the portal's
aaa-stats-ingest.php, which writes them to car_stats. All the DB knowledge lives
in the ingest endpoint; this side just logs in, pages and forwards.

  --recent N     only the first N pages of each maker (daily top-up of new results)
  --maker ID     one maker only (e.g. 1 = TOYOTA)
  --max-seconds  stop after S seconds (for cron); default: run until a full sweep
                 finishes, then keep sweeping
  --once         do a single full sweep and stop

State (which maker, which page) is kept in aaa_fetch_state.json beside this file,
so a stop and restart carries on where it left off. Gentle: GAP seconds between
aaajapan requests. Full first pull is ~60,000 requests, ~17 hours.
"""
import argparse, io, json, os, re, ssl, sys, threading, time, urllib.parse, urllib.request, http.cookiejar

BASE = 'https://bid.aaajapan.com'
# Everything secret comes from the environment, so this file is safe in a public
# repo. GitHub Actions passes them from repository Secrets; locally, set them in
# the shell before running (see the header). No credential is ever in the code.
PORTAL = os.environ.get('AAA_INGEST_URL', 'https://auction.sbkautotrading.com/aaa-stats-ingest.php')
INGEST_TOKEN = os.environ.get('AAA_INGEST_TOKEN', '')
USER = os.environ.get('AAA_USER', '')
PW = os.environ.get('AAA_PASS', '')
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
GAP = 1.0
# After the first full sweep, later runs only re-read the first RECENT_PAGES of
# each maker - the newest results sit on page 1, so this is the daily top-up
# without re-pulling 1.2M rows every time. --full forces a whole sweep.
RECENT_PAGES = 25
# The freshness pass: the first FRESH_PAGES pages of every maker, at most once
# every FRESH_EVERY seconds. See the two-job comment in run().
FRESH_PAGES = 3
FRESH_EVERY = 20 * 3600
HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, 'aaa_fetch_state.json')

# How many makers are worked on at once (the owner's yes, 14 September 2026, to
# cut the ~6.7-day first pull). Each worker owns its own slice of the maker list
# and its own cursor, so they never ask for the same page.
WORKERS = int(os.environ.get('AAA_WORKERS', '3'))
# The floor between ANY two requests to the source, across all workers. The source
# takes ~7.6 s to answer, so three workers sit near 0.3 requests a second on their
# own; this is the guard that keeps a fast answer from ever turning into a burst.
# Never raise the rate past this without the owner - a blocked source costs days.
GLOBAL_MIN_GAP = 0.5

ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE


class Blocked(Exception):
    pass


class Throttle(object):
    """One clock for every worker: no two source requests closer than `gap`."""

    def __init__(self, gap):
        self.gap = gap
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            due = max(now, self.next_at)
            self.next_at = due + self.gap
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)


THROTTLE = Throttle(GLOBAL_MIN_GAP)


def new_opener():
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
                                       urllib.request.HTTPSHandler(context=ctx))


def aaa(op, url, data=None, ref='/st?classic'):
    THROTTLE.wait()
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
    op = new_opener()
    aaa(op, BASE + '/aj_3', ref='/aj_3'); time.sleep(GAP)
    aaa(op, BASE + '/aj_3', urllib.parse.urlencode({'username': USER, 'password': PW, 'is_login': '1', 'ref': 'aj_3'}), '/aj_3'); time.sleep(GAP)
    h = aaa(op, BASE + '/st?classic')
    if 'logout' not in h:
        raise RuntimeError('login failed (no logout link)')
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
        raise RuntimeError('makers or form not found after login')
    return op, form, mk


def page(op, form, vid, pg):
    f = dict(form); f['vendor'] = str(vid); f['model'] = ''; f['page'] = str(max(1, pg))
    f['list_size'] = '20'; f['tpl'] = ''; f['is_stat'] = '0'
    url = BASE + '/st?file=loader&ajx=' + str(int(time.time() * 1000)) + '0-form'
    body = aaa(op, url, urllib.parse.urlencode(f))
    mm = re.search(r"'tpl_poisk':\s*'var data\s*=\s*(\{.*?\});'", body, re.S)
    if not mm:
        return [], {}
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


def send(rows, maker):
    """POST one page to the portal. Raises with what the server actually said.

    The bare json.loads() used to fail with "Expecting value: line 1 column 1",
    which says nothing about the cause - and on 14 Sep the portal's host started
    answering a runner with a non-JSON body, so the job retried the same page for
    an hour learning nothing. The status and the first bytes are part of the error
    now, so the next stall names itself."""
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


def load_state():
    if os.path.exists(STATE):
        try:
            return json.load(io.open(STATE, encoding='utf-8'))
        except Exception:
            pass
    return {'mIdx': 0, 'page': 1, 'sweeps': 0, 'sent': 0, 'totals': {},
            'fIdx': None, 'fPage': 1, 'fresh_at': 0}


def save_state(s):
    io.open(STATE, 'w', encoding='utf-8').write(json.dumps(s))


def fresh_cursor():
    return {'mIdx': 0, 'page': 1, 'fIdx': None, 'fPage': 1, 'fresh_at': 0, 'sweeps': 0}


def worker(wid, my_ids, s, lock, a, began, stop):
    """One worker: its own login, its own slice of makers, its own cursor.

    Workers never share a maker, so two of them can never ask for the same page,
    and the state file keeps a cursor each. Everything that IS shared - the row
    totals, the sent count, writing the file - goes through `lock`. The source is
    protected by THROTTLE, which all workers obey.

    Each worker does the same two jobs as before over its own slice:

      FRESH    the first FRESH_PAGES pages of every maker it owns. The newest
               sales sit on page one, so this is what keeps the portal current -
               without it nothing but TOYOTA would update for weeks, because the
               backfill is still inside TOYOTA's 18,920 pages.
      BACKFILL the deep cursor, one page at a time, filling in the history.
    """
    tag = 'w%d' % wid
    try:
        op, form, makers = login()
    except Exception as e:
        print('[%s] login failed: %s' % (tag, str(e)[:80]), flush=True)
        return
    with lock:
        c = s['w'].setdefault(str(wid), fresh_cursor())
    print('[%s] %d makers, resume maker#%d page %d' % (tag, len(my_ids), c['mIdx'], c['page']), flush=True)

    stale = 0
    while not stop.is_set():
        if a.max_seconds and time.time() - began > a.max_seconds:
            break
        if c['fIdx'] is None and time.time() - float(c.get('fresh_at', 0)) > FRESH_EVERY:
            c['fIdx'] = 0; c['fPage'] = 1
            print('[%s] ---- fresh pass over its makers ----' % tag, flush=True)
        fresh = c['fIdx'] is not None
        if fresh and c['fIdx'] >= len(my_ids):
            c['fresh_at'] = time.time(); c['fIdx'] = None
            with lock: save_state(s)
            print('[%s] ---- fresh pass done ----' % tag, flush=True)
            continue
        if not fresh and c['mIdx'] >= len(my_ids):
            c['sweeps'] += 1; c['mIdx'] = 0; c['page'] = 1
            with lock: save_state(s)
            print('[%s] ==== finished its makers (sweep %d) ====' % (tag, c['sweeps']), flush=True)
            if a.once:
                break
            continue

        vid = my_ids[c['fIdx'] if fresh else c['mIdx']]
        name = makers.get(vid, vid)
        pg = c['fPage'] if fresh else c['page']
        try:
            rows, navi = page(op, form, vid, pg)
        except Blocked as e:
            print('[%s] BLOCKED: %s -- the source is refusing us. Stopping every worker.' % (tag, e), flush=True)
            with lock: save_state(s)
            stop.set()
            return
        except Exception as e:
            print('[%s] read error %s (%s p%d) - relogin in 5s' % (tag, str(e)[:60], name, pg), flush=True)
            time.sleep(5)
            try:
                op, form, makers = login()
            except Exception as e2:
                print('[%s] relogin failed: %s' % (tag, str(e2)[:60]), flush=True); time.sleep(30)
            continue

        total = int(navi.get('rows', 0) or 0)
        with lock:
            if total:
                s['totals'][vid] = total
            elif vid in s['totals']:
                total = int(s['totals'][vid])
        last = -(-total // 20) if total > 0 else 0

        res = {}
        if rows:
            try:
                res = send(rows, name)
                with lock:
                    s['sent'] = int(s.get('sent', 0)) + res.get('written', 0)
                stale = 0
            except Exception as e:
                # One page must never hold a worker: back off, then skip it.
                stale += 1
                print('[%s] ingest error (try %d) %s (%s p%d)' % (tag, stale, str(e)[:160], name, pg), flush=True)
                if stale >= 8:
                    print('[%s]   giving up on %s p%d for now - moving on' % (tag, name, pg), flush=True)
                    stale = 0
                    if fresh: c['fPage'] += 1
                    else: c['page'] += 1
                    with lock: save_state(s)
                    continue
                time.sleep(min(60, 5 * stale)); continue

        if fresh:
            if (last > 0 and pg >= min(last, FRESH_PAGES)) or pg >= FRESH_PAGES or not rows:
                c['fIdx'] += 1; c['fPage'] = 1
            else:
                c['fPage'] += 1
        else:
            cap = a.recent if a.recent > 0 else last
            if (last > 0 and pg >= min(last, cap if cap else last)) or (not rows and pg >= 1):
                print('[%s] %-16s p%-5d of %-6d | %s rows at source | in_db %s'
                      % (tag, name, pg, last, format(total, ','),
                         format(res.get('in_db', 0), ',') if rows else '-'), flush=True)
                c['mIdx'] += 1; c['page'] = 1
            else:
                c['page'] += 1
        with lock: save_state(s)
        time.sleep(GAP)


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument('--recent', type=int, default=0)
    ap.add_argument('--maker', default='')
    ap.add_argument('--max-seconds', type=int, default=0)
    ap.add_argument('--once', action='store_true')
    ap.add_argument('--full', action='store_true', help='force a whole sweep even after the first')
    ap.add_argument('--workers', type=int, default=WORKERS)
    a = ap.parse_args()

    if not (USER and PW and INGEST_TOKEN):
        sys.exit('AAA_USER, AAA_PASS and AAA_INGEST_TOKEN must be set in the environment.')

    began = time.time()
    op, form, makers = login()          # once, just to learn the maker list
    ids = list(makers.keys())
    if a.maker:
        ids = [i for i in ids if str(i) == str(a.maker)]
    if not ids:
        sys.exit('no makers to work on')

    s = load_state()
    s.setdefault('totals', {})
    s.setdefault('sent', 0)
    if 'w' not in s:
        # Carry an older single-cursor state over: the maker it had reached keeps
        # its page, and every other worker starts at the top of its own slice.
        s['w'] = {}
        old_i = int(s.get('mIdx', 0) or 0)
        old_vid = ids[old_i] if 0 <= old_i < len(ids) else None
        old_page = int(s.get('page', 1) or 1)
        print('carrying older progress over: maker %s page %d'
              % (makers.get(old_vid, '-'), old_page), flush=True)
        for wid in range(max(1, a.workers)):
            c = fresh_cursor()
            slice_ids = ids[wid::max(1, a.workers)]
            if old_vid in slice_ids:
                c['mIdx'] = slice_ids.index(old_vid); c['page'] = old_page
            s['w'][str(wid)] = c
        save_state(s)

    nw = max(1, min(a.workers, len(ids)))
    swept = min((int(c.get('sweeps', 0) or 0) for c in s['w'].values()), default=0)
    if a.recent == 0 and not a.full and swept >= 1:
        a.recent = RECENT_PAGES     # the history is in; keep later runs light
    print('login ok | %d makers | %d workers | source gap %.1fs (floor %.1fs) | sent so far %s'
          % (len(ids), nw, GAP, GLOBAL_MIN_GAP, format(int(s.get('sent', 0)), ',')), flush=True)

    lock = threading.Lock()
    stop = threading.Event()
    threads = []
    for wid in range(nw):
        t = threading.Thread(target=worker, args=(wid, ids[wid::nw], s, lock, a, began, stop),
                             name='w%d' % wid, daemon=True)
        t.start(); threads.append(t)
        time.sleep(2)               # stagger the logins
    for t in threads:
        t.join()
    with lock:
        save_state(s)

    done = ', '.join('w%s %d/%d makers' % (wid, c.get('mIdx', 0), len(ids[int(wid)::nw]))
                     for wid, c in sorted(s['w'].items()))
    print('stopped | rows sent all-time %s | %s' % (format(int(s.get('sent', 0)), ','), done), flush=True)


if __name__ == '__main__':
    run()
