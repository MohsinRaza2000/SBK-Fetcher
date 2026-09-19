# -*- coding: utf-8 -*-
"""How far behind the statistics are - measured at the source, counters only.

Run by the `probe` workflow, which has the credentials. It fetches NO rows: for
each slice it asks for page one and reads the total the source itself reports,
then prints it beside what the portal already holds (passed in as OURS, a JSON
map built from our own database).

Slices asked for: the last six sale days for the three biggest makers, and
TOYOTA month by month - about two dozen requests in all, at the usual gap.
"""
import json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('AAA_DAILY_BUDGET', '200')
os.environ.setdefault('AAA_RUN_LIMIT', '200')
import aaa_fetch as F

OURS = json.loads(os.environ.get('OURS') or '{}')
DAYS = os.environ.get('DAYS', '2026-09-18,2026-09-17,2026-09-16,2026-09-15,2026-09-14,2026-09-13').split(',')
MAKERS = os.environ.get('MAKERS', 'TOYOTA,NISSAN,HONDA').split(',')
MONTHS = [('2026-09-01', '2026-09-30'), ('2026-08-01', '2026-08-31'),
          ('2026-07-01', '2026-07-31'), ('2026-06-01', '2026-06-30')]

op, form, makers = F.login()
by_name = {v.upper(): k for k, v in makers.items()}
print('the source lists %d makers' % len(makers), flush=True)

print('\n== the last sale days: what the source holds, and what we hold ==', flush=True)
print('%-10s %-11s %9s %9s %9s' % ('maker', 'day', 'source', 'ours', 'missing'), flush=True)
miss_days = 0
for name in MAKERS:
    vid = by_name.get(name.upper())
    if not vid:
        print('%-10s (not in the maker list)' % name, flush=True)
        continue
    for day in DAYS:
        try:
            rows, navi = F.page(op, form, vid, 1, day, day)
            src = int(navi.get('rows') or 0)
        except Exception as e:
            print('%-10s %-11s  asking failed: %s' % (name, day, str(e)[:60]), flush=True)
            continue
        mine = int(OURS.get('days', {}).get('%s|%s' % (name, day), 0))
        gap = max(0, src - mine)
        miss_days += gap
        print('%-10s %-11s %9s %9s %9s' % (name, day, '{:,}'.format(src), '{:,}'.format(mine),
                                           '{:,}'.format(gap) if gap else '-'), flush=True)

print('\n== TOYOTA, month by month ==', flush=True)
print('%-9s %9s %9s %9s' % ('month', 'source', 'ours', 'missing'), flush=True)
vid = by_name.get('TOYOTA')
miss_month = 0
for first, last in MONTHS:
    try:
        rows, navi = F.page(op, form, vid, 1, first, last)
        src = int(navi.get('rows') or 0)
    except Exception as e:
        print('%-9s  asking failed: %s' % (first[:7], str(e)[:60]), flush=True)
        continue
    mine = int(OURS.get('months', {}).get('TOYOTA|%s' % first[:7], 0))
    gap = max(0, src - mine)
    miss_month += gap
    print('%-9s %9s %9s %9s' % (first[:7], '{:,}'.format(src), '{:,}'.format(mine),
                                '{:,}'.format(gap) if gap else '-'), flush=True)

print('\nmissing in the six days shown (three makers): {:,}'.format(miss_days), flush=True)
print('missing in TOYOTA across four months        : {:,}'.format(miss_month), flush=True)
print('requests this probe used                    : %d' % F.THROTTLE.snapshot()['used'], flush=True)
