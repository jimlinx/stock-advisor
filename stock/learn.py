"""The learning loop's arithmetic: score every past proposal against the S&P 500, by script.

A proposal is judged by what the stock did next compared with SPY over 5, 20 and 60 trading days.
"Excess" is the stock's return minus SPY's for a BUY, and the reverse for a SELL (a sale was right
if the stock then lagged). Scores are kept in the `scores` table, so the monthly review and the
Learning page read numbers, not prices.

Decision classes, because what the owner DID says as much as what the market did:
  taken      approved: filled in a sandbox, or drafted in IBKR
  rejected   the owner said no (with a reason, if one was picked)
  ignored    left until a later run replaced it
  hands-off  LIVE proposals of 24-26 Sep 2026, deliberately left alone while the LIVE household
             served as the benchmark: they say nothing about the owners' preferences
"""
import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import feeds

HORIZONS = (5, 20, 60)
# See "hands-off" above. The frozen benchmark accounts took over that job on 2026-09-26.
HANDS_OFF = ('family', '2026-09-26')
SCHEMA = '''
CREATE TABLE IF NOT EXISTS scores (proposal_id INTEGER NOT NULL, horizon INTEGER NOT NULL, ret REAL NOT NULL,
  bench_ret REAL NOT NULL, excess REAL NOT NULL, PRIMARY KEY (proposal_id, horizon));
CREATE TABLE IF NOT EXISTS lessons (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, text TEXT NOT NULL,
  evidence TEXT, n INTEGER, status TEXT NOT NULL DEFAULT 'proposed',   -- proposed, active, retired, discarded
  created TEXT NOT NULL, decided_by TEXT, decided_at TEXT);
CREATE TABLE IF NOT EXISTS signals (day TEXT NOT NULL, symbol TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
  value TEXT NOT NULL, PRIMARY KEY (day, symbol, kind, name));  -- kind lens (pass/mixed/fail) or pattern (bullish/bearish)
CREATE TABLE IF NOT EXISTS signal_scores (day TEXT NOT NULL, symbol TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
  horizon INTEGER NOT NULL, excess REAL NOT NULL, PRIMARY KEY (day, symbol, kind, name, horizon));
CREATE TABLE IF NOT EXISTS policy_days (day TEXT PRIMARY KEY, level TEXT NOT NULL, what TEXT);  -- the brief's policy-shock verdict
CREATE TABLE IF NOT EXISTS verdicts (grp TEXT NOT NULL, run_date TEXT NOT NULL, symbol TEXT NOT NULL, verdict TEXT NOT NULL,
  horizon INTEGER NOT NULL, excess REAL NOT NULL, PRIMARY KEY (grp, run_date, symbol, horizon));
'''
# Every verdict the analysis gives (a card per holding and per opportunity) is scored too, not just the trades: many more
# data points than proposals, so the review sees sooner whether, say, its SELL verdicts really lag the market.
BULLISH, BEARISH = ('BUY', 'ADD'), ('SELL', 'TRIM')
SYD, NY = ZoneInfo('Australia/Sydney'), ZoneInfo('America/New_York')


def klass(p):
    if p['grp'] == HANDS_OFF[0] and p['run_date'] <= HANDS_OFF[1]:
        return 'hands-off'
    return {'filled': 'taken', 'drafted': 'taken', 'rejected': 'rejected', 'expired': 'ignored'}.get(p['status'])


def dedup(rows):
    """Reruns on one day repeat the same idea: keep one per (household, symbol, side, day),
    preferring the one acted on, else the latest."""
    best = {}
    for p in rows:
        k = (p['grp'], p['symbol'], p['side'], p['run_date'])
        rank = (p['cls'] == 'taken', p['id'])
        if k not in best or rank > best[k][0]:
            best[k] = (rank, p)
    return [p for _, p in best.values()]


def first_close(days, proposed_at):
    """Index of the first US close after the proposal was made: that day's close if it was made during
    or before the New York session, else the next day's."""
    t = datetime.fromisoformat(proposed_at).replace(tzinfo=SYD).astimezone(NY)
    us = t.date().isoformat() if t.hour < 16 else (t.date() + timedelta(days=1)).isoformat()
    return next((i for i, d in enumerate(days) if d >= us), None)


def score_one(p, rows, spy, spy_start):
    """{horizon: (ret, bench_ret, excess)} for the horizons that have matured."""
    start = p['fill_price'] or p['ref_price']
    days = [d for d, _ in rows]
    i0 = first_close(days, p['proposed_at'])
    if not start or i0 is None:
        return {}
    spy_at = dict(spy)
    if not spy_start:  # no benchmark reading at proposal time: the previous SPY close
        before = [x for d, x in spy if d < days[i0]]
        spy_start = before[-1] if before else None
    out = {}
    for h in HORIZONS:
        if i0 + h - 1 < len(rows) and spy_start and rows[i0 + h - 1][0] in spy_at:
            d, px = rows[i0 + h - 1]
            ret, bench = px / start - 1, spy_at[d] / spy_start - 1
            out[h] = (ret, bench, (ret - bench) if p['side'] == 'BUY' else (bench - ret))
    return out


def proposals(c):
    rows = [dict(r) for r in c.execute(
        'SELECT p.*, COALESCE(a.grp, "a" || a.id) AS grp, s.spy AS spy_start FROM proposals p '
        'JOIN accounts a ON a.id=p.account_id '
        'LEFT JOIN snapshots s ON s.account_id=p.account_id AND s.day=p.run_date')]
    for p in rows:
        p['cls'] = klass(p)
    return dedup([p for p in rows if p['cls']])


def score(c):
    """Score every decided proposal whose horizons have matured. Returns how many rows were written."""
    ps = proposals(c)
    closes = feeds.closes(c, sorted({p['symbol'] for p in ps} | {'SPY'}))
    n = 0
    for p in ps:
        for h, (ret, bench, ex) in score_one(p, closes.get(p['symbol'], []), closes.get('SPY', []), p['spy_start']).items():
            n += c.execute('INSERT OR REPLACE INTO scores VALUES (?,?,?,?,?)', (p['id'], h, ret, bench, ex)).rowcount
    n += score_verdicts(c, closes)
    n += score_signals(c, closes)
    c.execute('INSERT OR REPLACE INTO meta VALUES ("scored_at", ?)', (datetime.now().isoformat(timespec='seconds'),))
    c.commit()
    return n


def verdict_cards(c):
    """[(grp, run_date, at, symbol, verdict)] from the last analysis report of each household and day."""
    last = {}
    for r in c.execute("SELECT grp, run_date, at, body FROM reports WHERE grp IS NOT NULL AND grp NOT IN ('research', 'review') "
                       "AND body LIKE '{%' ORDER BY id"):
        last[(r[0], r[1])] = r
    out = []
    for grp, day, at, body in last.values():
        a = json.loads(body)
        for x in a.get('holdings', []) + a.get('opportunities', []):
            out.append((grp, day, at, x['symbol'].upper().strip(), x['verdict']))
    return out


def score_verdicts(c, closes=None):
    """Score each verdict: the stock's return minus SPY's from the first close after the analysis, over each horizon."""
    cards = verdict_cards(c)
    closes = closes or {}
    need = sorted({x[3] for x in cards} - set(closes))
    if need:
        closes = {**closes, **feeds.closes(c, need + ['SPY'])}
    spy = dict(closes.get('SPY', []))
    n = 0
    for grp, day, at, sym, verdict in cards:
        rows = closes.get(sym, [])
        i0 = first_close([d for d, _ in rows], at)
        if i0 is None or rows[i0][0] not in spy:
            continue
        for h in HORIZONS:
            if i0 + h < len(rows) and rows[i0 + h][0] in spy:
                ex = (rows[i0 + h][1] / rows[i0][1] - 1) - (spy[rows[i0 + h][0]] / spy[rows[i0][0]] - 1)
                n += c.execute('INSERT OR REPLACE INTO verdicts VALUES (?,?,?,?,?,?)', (grp, day, sym, verdict, h, ex)).rowcount
    return n


def verdict_summary(c):
    """{verdict: {horizon: (n, average excess, share beating SPY)}}."""
    g = {}
    for v, h, x in c.execute('SELECT verdict, horizon, excess FROM verdicts'):
        g.setdefault(v, {}).setdefault(h, []).append(x)
    return {v: {h: (len(xs), sum(xs) / len(xs), sum(x > 0 for x in xs) / len(xs)) for h, xs in sorted(hs.items())}
            for v, hs in g.items()}


# ---------------------------------------------------------------- investor lenses and patterns on trial
LENS_EVERY = 7   # a stock's lens results are recorded once a week: daily rows would be the same verdict many times over
PATTERN_GAP = 10  # the same pattern on the same stock within this many days is one event
# A pattern becomes evidence the advisor may use once, over 20 trading days, it has at least PROVE_N cases that beat
# the S&P 500 (in its own direction) by PROVE_EDGE on average, PROVE_HIT of the time; it fails with that many cases and
# no edge at all.
PROVE_N, PROVE_EDGE, PROVE_HIT, PROVE_H = 30, 0.01, 0.55, 20


def record_signals(c, day, symbol, lens_results, patterns, sides):
    """Store a stock's lens verdicts (weekly) and any pattern seen on its last bar. day: that bar's New York date."""
    since = (date.fromisoformat(day) - timedelta(days=LENS_EVERY)).isoformat()
    for k, (v, _) in lens_results.items():
        if not c.execute('SELECT 1 FROM signals WHERE symbol=? AND kind="lens" AND name=? AND day>?', (symbol, k, since)).fetchone():
            c.execute('INSERT OR IGNORE INTO signals VALUES (?,?,?,?,?)', (day, symbol, 'lens', k, v))
    gap = (date.fromisoformat(day) - timedelta(days=PATTERN_GAP)).isoformat()
    for k in patterns:
        if not c.execute('SELECT 1 FROM signals WHERE symbol=? AND kind="pattern" AND name=? AND day>?', (symbol, k, gap)).fetchone():
            c.execute('INSERT OR IGNORE INTO signals VALUES (?,?,?,?,?)', (day, symbol, 'pattern', k, sides[k]))


def score_signals(c, closes=None):
    """Each recorded signal: the stock's return minus SPY's from that day's close, over each horizon."""
    rows = c.execute('SELECT * FROM signals').fetchall()
    closes = closes or {}
    need = sorted({r[1] for r in rows} - set(closes))
    if need:
        closes = {**closes, **feeds.closes(c, need + ['SPY'])}
    spy = dict(closes.get('SPY', []))
    n = 0
    for day, sym, kind, name, value in rows:
        xs = closes.get(sym, [])
        i0 = next((i for i, (d, _) in enumerate(xs) if d >= day), None)
        if i0 is None or xs[i0][0] not in spy:
            continue
        for h in HORIZONS:
            if i0 + h < len(xs) and xs[i0 + h][0] in spy:
                ex = (xs[i0 + h][1] / xs[i0][1] - 1) - (spy[xs[i0 + h][0]] / spy[xs[i0][0]] - 1)
                n += c.execute('INSERT OR REPLACE INTO signal_scores VALUES (?,?,?,?,?,?)', (day, sym, kind, name, h, ex)).rowcount
    return n


def signal_summary(c, kind):
    """{name: {value: {horizon: (n, average excess, share right)}}}. Bearish patterns are turned round, so a positive
    number always means the signal was right."""
    g = {}
    for name, value, h, x in c.execute('SELECT s.name, s.value, x.horizon, x.excess FROM signal_scores x JOIN signals s '
                                       'USING (day, symbol, kind, name) WHERE x.kind=?', (kind,)):
        g.setdefault(name, {}).setdefault(value, {}).setdefault(h, []).append(-x if value == 'bearish' else x)
    return {k: {v: {h: (len(xs), sum(xs) / len(xs), sum(x > 0 for x in xs) / len(xs)) for h, xs in sorted(hs.items())}
                for v, hs in vs.items()} for k, vs in g.items()}


def pattern_status(stats):
    """'proven', 'failed' or 'on trial' from a pattern's {horizon: (n, average, hit)} (one direction)."""
    n, avg, hit = stats.get(PROVE_H, (0, 0, 0))
    if n >= PROVE_N and avg >= PROVE_EDGE and hit >= PROVE_HIT:
        return 'proven'
    return 'failed' if n >= PROVE_N and avg <= 0 else 'on trial'


def statuses(c):
    """{pattern: status} for every pattern seen so far."""
    return {k: pattern_status(next(iter(vs.values()))) for k, vs in signal_summary(c, 'pattern').items()}


def plan_outcome(plan, closes, start):
    """What an exit plan met first after `start` (a date), from daily closes: ('target', day), ('review level', day) or
    (None, None) while neither has been reached."""
    for d, x in closes:
        if d <= start:
            continue
        if plan.get('target_price') and x >= plan['target_price']:
            return 'target', d
        if plan.get('stop_price') and x <= plan['stop_price']:
            return 'review level', d
    return None, None


def churn(ps, days=28):
    """Ideas reversed within about 20 trading days (28 calendar days): a buy then a sell of the same
    stock in the same household, or the other way round."""
    out = []
    by = {}
    for p in sorted(ps, key=lambda p: p['run_date']):
        by.setdefault((p['grp'], p['symbol']), []).append(p)
    for lst in by.values():
        for a, b in zip(lst, lst[1:]):
            if a['side'] != b['side'] and (date.fromisoformat(b['run_date']) - date.fromisoformat(a['run_date'])).days <= days:
                out.append((a, b))
    return out


def rows(c):
    """Deduplicated proposals joined with their scores: {'id', ..., 'cls', 'scores': {h: excess}}."""
    ps = proposals(c)
    sc = {}
    for r in c.execute('SELECT proposal_id, horizon, excess FROM scores'):
        sc.setdefault(r[0], {})[r[1]] = r[2]
    shock = dict(c.execute('SELECT day, level FROM policy_days'))
    for p in ps:
        p['scores'] = sc.get(p['id'], {})
        p['policy'] = shock.get(p['run_date'])  # None: a day before the brief recorded it
    return ps


def summary(ps, h, key):
    """{group value: (n, average excess, hit rate)} at horizon h."""
    g = {}
    for p in ps:
        if h in p['scores']:
            g.setdefault(key(p), []).append(p['scores'][h])
    return {k: (len(v), sum(v) / len(v), sum(x > 0 for x in v) / len(v)) for k, v in g.items()}


def test():
    import sqlite3
    c = sqlite3.connect(':memory:')
    c.executescript(SCHEMA)
    record_signals(c, '2026-10-01', 'X', {'graham': ('pass', '')}, ['double_top'], {'double_top': 'bearish'})
    record_signals(c, '2026-10-05', 'X', {'graham': ('fail', '')}, ['double_top'], {'double_top': 'bearish'})  # within a week: not again
    record_signals(c, '2026-10-09', 'X', {'graham': ('fail', '')}, [], {})
    assert c.execute('SELECT COUNT(*) FROM signals').fetchone()[0] == 3
    days = [f'2026-10-{d:02d}' for d in range(1, 31)]
    cl = {'X': [(d, 100.0 - i) for i, d in enumerate(days)], 'SPY': [(d, 100.0) for d in days]}
    assert score_signals(c, cl) > 0
    s = signal_summary(c, 'pattern')['double_top']['bearish'][5]
    assert s[0] == 1 and s[1] > 0 and s[2] == 1.0, s   # X fell 5% while SPY stood still: the bearish call was right
    assert pattern_status({20: (40, 0.02, 0.6)}) == 'proven' and pattern_status({20: (40, -0.01, 0.4)}) == 'failed'
    assert pattern_status({20: (10, 0.05, 0.9)}) == 'on trial'
    days = ['2026-09-24', '2026-09-25', '2026-09-28', '2026-09-29', '2026-09-30', '2026-10-01']
    stock = list(zip(days, [100, 101, 102, 103, 104, 110.0]))
    spy = list(zip(days, [500, 500, 505, 505, 510, 525.0]))
    # 23:50 Sydney on the 25th = 09:50 New York on the 25th: the 25th's close is the first
    p = {'side': 'BUY', 'fill_price': None, 'ref_price': 100.0, 'proposed_at': '2026-09-25T23:50:00'}
    assert first_close(days, p['proposed_at']) == 1
    # 07:00 Sydney on the 26th = 17:00 New York on the 25th: after the close, so the next day
    assert first_close(days, '2026-09-26T07:00:00') == 2
    s = score_one(p, stock, spy, 500.0)
    assert list(s) == [5] and abs(s[5][0] - 0.10) < 1e-9 and abs(s[5][1] - 0.05) < 1e-9 and abs(s[5][2] - 0.05) < 1e-9, s
    s = score_one({**p, 'side': 'SELL'}, stock, spy, None)  # no snapshot: the previous close (500)
    assert abs(s[5][2] - (-0.05)) < 1e-9, s
    assert klass({'grp': 'family', 'run_date': '2026-09-25', 'status': 'expired'}) == 'hands-off'
    assert klass({'grp': 'family', 'run_date': '2026-09-29', 'status': 'expired'}) == 'ignored'
    assert klass({'grp': 'family-sandbox', 'run_date': '2026-09-25', 'status': 'filled'}) == 'taken'
    assert klass({'grp': 'x', 'run_date': '2026-09-29', 'status': 'invalid'}) is None
    P = lambda i, cls, side='BUY', d='2026-09-25': {'id': i, 'grp': 'g', 'symbol': 'A', 'side': side, 'run_date': d, 'cls': cls}
    assert [p['id'] for p in dedup([P(1, 'ignored'), P(2, 'taken'), P(3, 'ignored')])] == [2]
    assert len(churn([P(1, 'taken'), P(2, 'taken', 'SELL', '2026-10-10'), P(3, 'taken', 'BUY', '2026-12-30')])) == 1
    assert plan_outcome({'target_price': 103.5, 'stop_price': 90}, stock, '2026-09-25') == ('target', '2026-09-30')
    assert plan_outcome({'target_price': 200, 'stop_price': 90}, stock, '2026-09-25') == (None, None)
    import sqlite3
    c = sqlite3.connect(':memory:')
    feeds.setup(c)
    c.executescript(SCHEMA + 'CREATE TABLE reports (id INTEGER PRIMARY KEY, account_id INTEGER, run_date TEXT, body TEXT, at TEXT, grp TEXT);')
    c.execute("INSERT INTO reports (grp, run_date, at, body) VALUES ('g', '2026-09-24', '2026-09-24T23:50:00', ?)",
              (json.dumps({'holdings': [{'symbol': 'AAA', 'verdict': 'HOLD'}], 'opportunities': []}),))
    assert score_verdicts(c, {'AAA': stock, 'SPY': spy}) == 1  # 24th close 100 -> 5 closes on, 110: +10% against SPY's +5%
    (v, (n, avg, hit)), = [(v, hs[5]) for v, hs in verdict_summary(c).items()]
    assert v == 'HOLD' and n == 1 and abs(avg - 0.05) < 1e-9 and hit == 1, verdict_summary(c)
    print('learn ok')
