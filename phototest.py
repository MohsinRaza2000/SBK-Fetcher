# -*- coding: utf-8 -*-
"""One-off: can a datacenter IP (this runner) fetch an aaajapan photo from 8.ajes.com?
Logs in, grabs a token from the first search page, tries the photo with the session.
Prints the HTTP status/size so we know whether a Cloudflare Worker proxy is worth building."""
import os, re, ssl, time, urllib.parse, urllib.request, http.cookiejar

BASE = 'https://bid.aaajapan.com'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
USER = os.environ.get('AAA_USER', ''); PW = os.environ.get('AAA_PASS', '')
ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj), urllib.request.HTTPSHandler(context=ctx))


def req(u, d=None, ref='/st?classic'):
    return op.open(urllib.request.Request(u, data=(d.encode() if d else None), headers={'User-Agent': UA, 'Referer': BASE + ref}), timeout=90)


req(BASE + '/aj_3', ref='/aj_3'); time.sleep(1)
req(BASE + '/aj_3', urllib.parse.urlencode({'username': USER, 'password': PW, 'is_login': '1', 'ref': 'aj_3'}), '/aj_3'); time.sleep(1)
h = req(BASE + '/st?classic').read().decode('utf-8', 'replace')
print('logged in:', 'logout' in h)
i = h.find('<form id=poisk'); seg = h[i:h.find('</form>', i) + 7]
form = {}
for tag in re.findall(r'<input[^>]*>', seg):
    n = re.search(r'name=[\'"]?([\w\[\]]+)', tag)
    if n and not n.group(1).lower().startswith('lose_time_here'):
        v = re.search(r'value=(["\'])(.*?)\1', tag, re.S); form[n.group(1)] = v.group(2) if v else ''
time.sleep(1)
f = dict(form); f.update({'vendor': '1', 'model': '', 'page': '1', 'list_size': '20', 'tpl': '', 'is_stat': '0'})
body = req(BASE + '/st?file=loader&ajx=' + str(int(time.time() * 1000)) + '0-form', urllib.parse.urlencode(f)).read().decode('utf-8', 'replace')
raw = re.search(r"'tpl_poisk':\s*'var data\s*=\s*(\{.*?\});'", body, re.S).group(1).replace('\\"', '"').replace("\\'", "'").replace('\\/', '/')
one = re.findall(r'\{a:"(?:[^"\\]|\\.)*".*?\}(?=,\{a:"|$)', re.search(r'body:\[(.*)\]\s*\}\s*;?\s*$', raw, re.S).group(1), re.S)[0]
xt = dict(re.findall(r'(\w+):"((?:[^"\\]|\\.)*)"', one)).get('x', '')
print('token:', xt[:30])
for suffix in ['', '&h=50']:
    u = 'https://8.ajes.com/imgs/' + xt + suffix
    try:
        r = op.open(urllib.request.Request(u, headers={'User-Agent': UA, 'Referer': BASE + '/st?classic'}), timeout=60)
        b = r.read()
        print('PHOTO %-6s -> %s %s %d bytes  %s' % (suffix or 'full', r.status, r.headers.get('Content-Type'), len(b),
              'IMAGE OK' if r.headers.get('Content-Type', '').startswith('image') else 'not image'))
    except urllib.error.HTTPError as e:
        print('PHOTO %-6s -> HTTP %s (datacenter IP refused)' % (suffix or 'full', e.code))
