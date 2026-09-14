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
import argparse, io, json, os, re, ssl, sys, time, urllib.parse, urllib.request, http.cookiejar

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
HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, 'aaa_fetch_state.json')

ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE


class Blocked(Exception):
    pass


def new_opener():
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
                                       urllib.request.HTTPSHandler(context=ctx))


def aaa(op, url, data=None, ref='/st?classic'):
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
    return {'mIdx': 0, 'page': 1, 'sweeps': 0, 'sent': 0, 'totals': {}}


def save_state(s):
    io.open(STATE, 'w', encoding='utf-8').write(json.dumps(s))


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument('--recent', type=int, default=0)
    ap.add_argument('--maker', default='')
    ap.add_argument('--max-seconds', type=int, default=0)
    ap.add_argument('--once', action='store_true')
    ap.add_argument('--full', action='store_true', help='force a whole sweep even after the first')
    a = ap.parse_args()

    if not (USER and PW and INGEST_TOKEN):
        sys.exit('AAA_USER, AAA_PASS and AAA_INGEST_TOKEN must be set in the environment.')

    began = time.time()
    op, form, makers = login()
    ids = list(makers.keys())
    if a.maker:
        ids = [i for i in ids if str(i) == str(a.maker)]
    s = load_state()
    if s['mIdx'] >= len(ids):
        s['mIdx'] = 0; s['page'] = 1
    # The heavy first pull is one full sweep; after that, keep it light with the
    # newest pages only, unless the caller forces a full sweep.
    if a.recent == 0 and not a.full and int(s.get('sweeps', 0)) >= 1:
        a.recent = RECENT_PAGES
    print('login ok | makers %d | resume maker#%d page %d | in state: sent %d, sweeps %d'
          % (len(ids), s['mIdx'], s['page'], s.get('sent', 0), s.get('sweeps', 0)), flush=True)

    stale = 0
    while True:
        if a.max_seconds and time.time() - began > a.max_seconds:
            print('time budget reached', flush=True); break
        if s['mIdx'] >= len(ids):
            s['sweeps'] += 1; s['mIdx'] = 0; s['page'] = 1; save_state(s)
            print('==== full sweep %d done ====' % s['sweeps'], flush=True)
            if a.once:
                break
            continue
        vid = ids[s['mIdx']]; name = makers[vid]
        try:
            rows, navi = page(op, form, vid, s['page'])
        except Blocked as e:
            print('BLOCKED: %s -- this machine is now refused by aaajapan. Stopping.' % e, flush=True)
            save_state(s); sys.exit(2)
        except Exception as e:
            print('read error %s (%s p%d) - relogin in 5s' % (str(e)[:60], name, s['page']), flush=True)
            time.sleep(5)
            try:
                op, form, makers = login(); ids = [i for i in makers.keys() if not a.maker or str(i) == str(a.maker)]
            except Exception as e2:
                print('relogin failed: %s' % str(e2)[:60], flush=True); time.sleep(30)
            continue

        total = int(navi.get('rows', s['totals'].get(vid, 0)) or 0)
        s['totals'][vid] = total
        last = -(-total // 20) if total > 0 else 0

        if rows:
            try:
                res = send(rows, name)
                s['sent'] += res.get('written', 0)
                stale = 0
            except Exception as e:
                # One page must never hold the whole job: back off, then skip it.
                stale += 1
                wait = min(60, 5 * stale)
                print('ingest error (try %d) %s (%s p%d) - waiting %ds'
                      % (stale, str(e)[:160], name, s['page']), flush=True)
                if stale >= 8:
                    print('  giving up on %s p%d for now - moving on' % (name, s['page']), flush=True)
                    stale = 0
                    s['page'] += 1
                    save_state(s)
                    continue
                time.sleep(wait); continue

        cap = a.recent if a.recent > 0 else last
        end_of_maker = (last > 0 and s['page'] >= min(last, cap if cap else last)) or (not rows and s['page'] >= 1)
        if end_of_maker:
            print('%-16s p%-5d of %-6d | %s rows total | in_db %s'
                  % (name, s['page'], last, format(total, ','), format(res.get('in_db', 0), ',') if rows else '-'), flush=True)
            s['mIdx'] += 1; s['page'] = 1
        else:
            s['page'] += 1
        save_state(s)
        time.sleep(GAP)

    print('stopped | sent this state total %d | sweeps %d' % (s.get('sent', 0), s.get('sweeps', 0)), flush=True)


if __name__ == '__main__':
    run()
