#!/usr/bin/env python3
"""stock.example.com: a US-stock advisor for Alex's and Sam's IBKR accounts.

Every trade -- SANDBOX or LIVE -- is a proposal that a human approves or rejects on the
web page, so rehearsing in a sandbox is the same workflow as trading for real.

  python3 stock.py serve            web app on 172.17.0.1:8089, behind Caddy basic auth
  python3 stock.py analyse [ID]     daily run: market brief, then proposals per account
  python3 stock.py test             self-check of the sandbox money path

Analysis runs through headless Claude Code (`claude -p`) on the owner's subscription, which
reaches IBKR through two claude.ai connectors: the built-in IBKR one (Alex) and a custom one
named "IBKR Sam" (https://api.ibkr.com/v1/api/mcp-public), because IBKR allows one account per
authorisation. Both live in claude.ai, which renews their sign-in. A Claude Code `claude mcp add`
server was tried first: IBKR gave it a 24-hour token with no refresh, so it lapsed daily.

LIVE approval never places an order. The IBKR connector can only create an order
INSTRUCTION (a draft); the page links to it and the account holder submits it in the IBKR
app. SANDBOX approval fills immediately at the current Yahoo price, with IBKR's fixed fees.

Everything is in USD. Totals also show AUD: IBKR's rate for live accounts, Yahoo's for sandboxes.
"""
import html, json, os, re, secrets, sqlite3, statistics, subprocess, sys, threading, time, urllib.parse, urllib.request
from http.cookies import SimpleCookie
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


import dividends
import feeds
import learn
import lenses
import tax
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
import webauth

HOME = '/var/lib/stock'
DB = os.path.join(HOME, 'stock.db')
CLAUDE = '/root/.local/bin/claude'
SELF = os.path.abspath(__file__)
HOST, PORT = '172.17.0.1', 8089
ORIGIN = 'https://stock.example.com'
# Sliding: each visit pushes expiry out again, so a phone used weekly stays signed in and a
# session left alone for this long ends. Logging out ends it at once.
SESSION_TTL = 30 * 86400
LOCKOUT = (10, 900)  # 10 failed logins from one address within 15 minutes locks that address out

# Tool-name prefix of each account's IBKR connector. Sandboxes borrow Alex's for market data.
CONNECTORS = {'alex': 'mcp__claude_ai_Interactive_Brokers_IBKR__', 'sam': 'mcp__claude_ai_IBKR_Sam__'}
READ_TOOLS = ['get_account_summary', 'get_account_positions', 'get_account_balances',
              'get_account_orders', 'get_account_trades', 'get_price_snapshot',
              'get_price_history', 'search_contracts', 'get_company_themes',
              'get_company_connections', 'get_theme_details', 'search_investment_topics',
              'get_pa_performance_all_periods', 'get_pa_allocation']
WRITE_TOOLS = ['create_order_instruction', 'delete_order_instruction', 'create_alert',
               'update_alert', 'delete_alert', 'set_alert_status', 'create_watchlist',
               'edit_watchlist', 'delete_watchlist']
# Read-only tools of BOTH connectors, for every analysis run: Claude sometimes reaches for the account
# holder's own connector (Sam's, for a trade in her account), and refusing it cost a proposal once.
ALL_READ = [p + t for p in CONNECTORS.values() for t in READ_TOOLS]
# The connectors' names in Claude Code, as `claude mcp list` shows them.
CONNECTOR_NAMES = {'alex': 'claude.ai Interactive Brokers (IBKR)', 'sam': 'claude.ai IBKR Sam'}
# Claude Code remembers a connector whose sign-in lapsed and stops trying it for a while, even after the account holder
# has signed in again (2026-10-05: Sam reconnected, and runs kept skipping her connector for 40 minutes).
AUTH_CACHE = '/root/.claude/mcp-needs-auth-cache.json'
# Analysis runs must never write anything, anywhere. Denials beat any allow in settings.json.
DENY = ['Bash', 'Edit', 'Write', 'NotebookEdit', 'Agent'] + \
       [p + t for p in CONNECTORS.values() for t in WRITE_TOOLS]

SCHEMA = '''
CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('SANDBOX','LIVE')), owner TEXT NOT NULL,
  cash REAL NOT NULL DEFAULT 0,          -- USD; sandbox only (live comes from snapshot)
  snapshot TEXT, snapshot_at TEXT,       -- live: raw IBKR JSON {summary, positions, balances}
  created TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS positions (account_id INTEGER NOT NULL, symbol TEXT NOT NULL,
  qty REAL NOT NULL, avg_price REAL NOT NULL, PRIMARY KEY (account_id, symbol));
CREATE TABLE IF NOT EXISTS proposals (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL,
  run_date TEXT NOT NULL, symbol TEXT NOT NULL, conid INTEGER, side TEXT NOT NULL,
  qty REAL NOT NULL, order_type TEXT NOT NULL, limit_price REAL, ref_price REAL,
  reason TEXT, confidence TEXT, status TEXT NOT NULL DEFAULT 'pending',
  decided_by TEXT, decided_at TEXT, fill_price REAL, fee REAL, link TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS reports (id INTEGER PRIMARY KEY, account_id INTEGER,  -- NULL = market brief
  run_date TEXT NOT NULL, body TEXT NOT NULL, at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage (id INTEGER PRIMARY KEY, at TEXT NOT NULL, kind TEXT NOT NULL,
  account_id INTEGER, ok INTEGER NOT NULL, cost_usd REAL, input_tokens INTEGER, output_tokens INTEGER,
  cache_read_tokens INTEGER, cache_write_tokens INTEGER, turns INTEGER, seconds REAL, model TEXT);
CREATE TABLE IF NOT EXISTS trace (usage_id INTEGER NOT NULL, seq INTEGER NOT NULL, kind TEXT NOT NULL,
  tool TEXT, detail TEXT, PRIMARY KEY (usage_id, seq));   -- kind: search, fetch, ibkr, note, other
CREATE TABLE IF NOT EXISTS conids (symbol TEXT PRIMARY KEY, conid INTEGER NOT NULL);  -- IBKR ids never change
CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user TEXT NOT NULL, expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots (account_id INTEGER NOT NULL, day TEXT NOT NULL,
  value_usd REAL NOT NULL, spy REAL NOT NULL, PRIMARY KEY (account_id, day));
CREATE TABLE IF NOT EXISTS plans (grp TEXT NOT NULL, symbol TEXT NOT NULL, plan TEXT NOT NULL,  -- exit plan, JSON
  set_on TEXT NOT NULL, source TEXT NOT NULL, PRIMARY KEY (grp, symbol));   -- source: proposal or holding
CREATE TABLE IF NOT EXISTS perf (account_id INTEGER NOT NULL, day TEXT NOT NULL, ret REAL NOT NULL, currency TEXT NOT NULL,
  PRIMARY KEY (account_id, day));  -- LIVE: IBKR's time-weighted daily return, in the account's base currency
CREATE TABLE IF NOT EXISTS sandbox_divs (account_id INTEGER NOT NULL, symbol TEXT NOT NULL, day TEXT NOT NULL,
  qty REAL NOT NULL, per_share REAL NOT NULL, net_usd REAL NOT NULL, PRIMARY KEY (account_id, symbol, day));
'''


def db():
    c = sqlite3.connect(DB, timeout=30, factory=webauth.Conn)  # `with db() as c:` closes as well as commits
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL')
    c.executescript(SCHEMA)
    c.executescript(tax.SCHEMA)
    feeds.setup(c)
    if 'usage_id' not in [r[1] for r in c.execute('PRAGMA table_info(reports)')]:
        c.execute('ALTER TABLE reports ADD COLUMN usage_id INTEGER')  # which run wrote it: its trace
    # Household: accounts with the same grp are ONE investment, analysed together (Alex,
    # 2026-09-25: "we just split it into two accounts"). NULL = an account on its own.
    if 'grp' not in [r[1] for r in c.execute('PRAGMA table_info(accounts)')]:
        c.execute('ALTER TABLE accounts ADD COLUMN grp TEXT')
        c.execute('ALTER TABLE reports ADD COLUMN grp TEXT')
    c.executescript(learn.SCHEMA)
    # Frozen: a buy-and-hold benchmark, never analysed and never given proposals (Alex, 2026-09-26: a
    # clone of the LIVE household, so LIVE itself can trade while the advisor is measured against it).
    if 'frozen' not in [r[1] for r in c.execute('PRAGMA table_info(accounts)')]:
        c.execute('ALTER TABLE accounts ADD COLUMN frozen INTEGER NOT NULL DEFAULT 0')
    # Profile: whose space an account belongs to. 'family' is Alex and Sam's; a friend gets their own (Alex,
    # 2026-10-05: Chris, sandbox and frozen only). Research, analysis runs and the Learning page are shared by all.
    if 'profile' not in [r[1] for r in c.execute('PRAGMA table_info(accounts)')]:
        c.execute("ALTER TABLE accounts ADD COLUMN profile TEXT NOT NULL DEFAULT 'family'")
    if 'proposed_at' not in [r[1] for r in c.execute('PRAGMA table_info(proposals)')]:
        c.execute('ALTER TABLE proposals ADD COLUMN proposed_at TEXT')  # when, for scoring against later prices
        c.execute('ALTER TABLE proposals ADD COLUMN reject_reason TEXT')
    if 'realised' not in [r[1] for r in c.execute('PRAGMA table_info(proposals)')]:
        c.execute('ALTER TABLE proposals ADD COLUMN realised REAL')  # a sandbox SELL's gain after fees, USD
    if 'plan' not in [r[1] for r in c.execute('PRAGMA table_info(proposals)')]:
        c.execute('ALTER TABLE proposals ADD COLUMN plan TEXT')  # the exit plan a BUY came with, JSON
        # Older proposals: the household report of the same day was written seconds after them.
        c.execute('UPDATE proposals SET proposed_at = COALESCE((SELECT MIN(r.at) FROM reports r JOIN accounts a '
                  'ON a.id=proposals.account_id WHERE r.run_date=proposals.run_date AND r.grp=COALESCE(a.grp, "a" || a.id)), '
                  'run_date || "T12:00:00")')
    if not c.execute('SELECT 1 FROM accounts WHERE mode="LIVE"').fetchone():
        for owner in ('alex', 'sam'):
            c.execute('INSERT INTO accounts (name, mode, owner, created, grp) VALUES (?,?,?,?,?)',
                      (owner.title(), 'LIVE', owner, now(), 'family'))
        c.commit()
    return c


def now():
    return datetime.now().isoformat(timespec='seconds')


def look(a):
    """How an account is shown: LIVE, SANDBOX, or FROZEN for a benchmark (a sandbox in the database)."""
    return 'FROZEN' if a['frozen'] else a['mode']


ADMINS = {'alex'}  # can open every profile and switch between them
CTX = threading.local()  # the signed-in user and the profile they are looking at, for the request being answered


def profile_of(user):
    """The profile a user belongs to: the family (the IBKR account holders) or their own."""
    return 'family' if user in CONNECTORS else user


def family():
    """The request comes from Alex or Sam: the family's own pages (LIVE, tax, dividends, usage) and controls."""
    return getattr(CTX, 'user', None) in CONNECTORS


def profiles(c):
    return sorted({'family'} | {profile_of(u) for u in webauth.users()} |
                  {r[0] for r in c.execute('SELECT DISTINCT profile FROM accounts')}, key=lambda p: (p != 'family', p))


def current_profile(c, user, wanted):
    return wanted if user in ADMINS and wanted in profiles(c) else profile_of(user)


def can_see(user, a):
    return bool(a) and (user in ADMINS or a['profile'] == profile_of(user))


def grp_key(a):
    return a['grp'] or f'a{a["id"]}'


def members(c, a):
    """The accounts analysed together with `a`, itself included."""
    if not a['grp']:
        return [a]
    return c.execute('SELECT * FROM accounts WHERE grp=? ORDER BY id', (a['grp'],)).fetchall()


# ---------------------------------------------------------------- prices (Yahoo, no key)
QUOTES = {}  # symbol -> (time, price, previous close, New York date of the price); the test preloads this


def quote(sym):
    return quote_full(sym)[0]


def prev_close(sym):
    return quote_full(sym)[1]


def session_day(sym):
    """The New York date of the latest price: the session that 'today' means for this symbol."""
    quote_full(sym)
    q = QUOTES.get(sym, ())
    return q[3] if len(q) > 3 else datetime.now(learn.NY).date().isoformat()


def quote_full(sym):
    t, p, *prev = QUOTES.get(sym, (0, None))
    if p is not None and (t == 0 or time.time() - t < 300):
        return p, (prev or [None])[0]
    # ponytail: Yahoo's unofficial chart API; swap for IBKR snapshots if it starts refusing.
    y = sym.strip().replace(' ', '-').replace('.', '-')
    req = urllib.request.Request(
        f'https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(y)}?range=1d&interval=1d',
        headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as r:
        m = json.load(r)['chart']['result'][0]['meta']
    p, prev = float(m['regularMarketPrice']), m.get('chartPreviousClose')
    day = datetime.fromtimestamp(m.get('regularMarketTime') or time.time(), learn.NY).date().isoformat()
    QUOTES[sym] = (time.time(), p, prev, day)
    return p, prev


def remember_conids(c, pairs):
    c.executemany('INSERT OR REPLACE INTO conids VALUES (?,?)',
                  [(str(s).upper(), int(i)) for s, i in pairs if s and isinstance(i, int) and i > 0])
    c.commit()


def known_conids(c, symbols):
    q = ','.join('?' * len(symbols))
    return dict(c.execute(f'SELECT symbol, conid FROM conids WHERE symbol IN ({q})', list(symbols)).fetchall()) if symbols else {}


def trend_stats(closes):
    """Trend figures from daily closes (oldest first): what Claude used to fetch 6-month charts for."""
    last = closes[-1]
    chg = lambda n: round(100 * (last / closes[-1 - n] - 1), 1) if len(closes) > n else None
    sma = lambda n: sum(closes[-n:]) / n if len(closes) >= n else None
    hi, lo = max(closes[-252:]), min(closes[-252:])
    out = {'last': round(last, 2), 'chg_1m_pct': chg(21), 'chg_3m_pct': chg(63), 'chg_6m_pct': chg(126),
           'high_52w': round(hi, 2), 'low_52w': round(lo, 2), 'pct_from_high': round(100 * (last / hi - 1), 1)}
    for n in (50, 200):
        m = sma(n)
        out[f'vs_sma{n}_pct'] = round(100 * (last / m - 1), 1) if m else None
    out.update(signals(closes))
    return out


def rsi(closes, n=14):
    """Wilder's relative strength index of the last close: above 70 overbought, below 30 oversold."""
    if len(closes) <= n:
        return None
    ch = [b - a for a, b in zip(closes, closes[1:])]
    up = sum(max(x, 0) for x in ch[:n]) / n
    dn = sum(max(-x, 0) for x in ch[:n]) / n
    for x in ch[n:]:
        up, dn = (up * (n - 1) + max(x, 0)) / n, (dn * (n - 1) + max(-x, 0)) / n
    return 100.0 if dn == 0 else 100 - 100 / (1 + up / dn)


def signals(closes):
    """The chart read by script, from daily closes (oldest first). Only the measures with research behind them:
    trend (price against rising or falling 50- and 200-day averages), 12-1 momentum (the year's return leaving out the
    last month, the best documented momentum measure), nearness to the 52-week high, 55-day breakouts, RSI extremes,
    realised volatility and recent support and resistance. Classical shapes (head and shoulders, triangles, flags) are
    left out: they are hard to define and their record is weak."""
    n, last = len(closes), closes[-1]
    sma = lambda k, end=0: sum(closes[n - end - k:n - end]) / k if n - end >= k else None
    out = {}
    if n >= 253:
        out['mom_12_1_pct'] = round(100 * (closes[-22] / closes[-253] - 1), 1)
    s50, s200 = sma(50), sma(200)
    s200_then, s50_then = sma(200, 20), sma(50, 20)
    rising = lambda now, then: None if now is None or then is None else now > then
    if n >= 220:
        cross = None
        for back in range(0, min(60, n - 200)):
            a, b = sma(50, back) - sma(200, back), sma(50, back + 1) - sma(200, back + 1)
            if (a > 0) != (b > 0):
                cross = (back, 'golden cross (50-day rose above 200-day)' if a > 0 else 'death cross (50-day fell below 200-day)')
                break
        if cross:
            out['cross'] = f'{cross[1]} {cross[0]} trading days ago'
    r = rsi(closes)
    if r is not None:
        out['rsi14'] = round(r)
    if n >= 56:
        prior = closes[-56:-1]
        out['breakout'] = ('new 55-day high' if last > max(prior) else 'new 55-day low' if last < min(prior) else None)
    rets = [b / a - 1 for a, b in zip(closes, closes[1:])]
    vol = lambda k: (statistics.pstdev(rets[-k:]) * 252 ** .5) if len(rets) >= k else None
    if vol(20):
        out['vol_20d_pct'], out['vol_60d_pct'] = round(100 * vol(20)), round(100 * vol(60)) if vol(60) else None
    if n >= 60:
        out['support_20d'], out['support_60d'], out['resistance_60d'] = (round(min(closes[-20:]), 2), round(min(closes[-60:]), 2),
                                                                          round(max(closes[-60:]), 2))
    # The setup, in words: what a trend-follower would call this chart.
    up200, up50 = rising(s200, s200_then), rising(s50, s50_then)
    hi = max(closes[-252:])
    setup = []
    if s50 and s200:
        if last > s50 > s200 and up200:
            setup.append('uptrend (above rising 50- and 200-day averages)')
        elif last < s50 < s200 and up200 is False:
            setup.append('downtrend (below falling 50- and 200-day averages)')
        elif s50 > s200 and up200 and s200 < last < s50:
            setup.append('pullback within an uptrend (between the 50- and 200-day averages)')
        elif last > s200 and not up200:
            setup.append('recovering (above a 200-day average that is still falling)')
        elif last < s200 and up200:
            setup.append('broken below a rising 200-day average')
    if s50 and last > 1.15 * s50 or (r or 0) > 75:
        setup.append('stretched: far above its 50-day average or overbought')
    if (r or 50) < 30:
        setup.append('oversold (RSI under 30)')
    if last >= 0.97 * hi:
        setup.append('within 3% of its 52-week high')
    if out.get('breakout'):
        setup.append(out['breakout'])
    out['setup'] = '; '.join(setup) or 'no clear trend'
    return out


def price_stats(symbols):
    """{symbol: trend stats} from a year of Yahoo daily closes, kept in the database and topped up daily."""
    with db() as c:
        data = feeds.closes(c, symbols)
    return {sym: trend_stats([x for _, x in rows]) for sym, rows in data.items() if len(rows) > 1}


def trend_text(t):
    if not t:
        return ''
    part = lambda k, label: f'{label} {t[k]:+.1f}%' if t.get(k) is not None else None
    return ', '.join(x for x in (part('chg_1m_pct', '1 month'), part('chg_6m_pct', '6 months'),
                                 part('mom_12_1_pct', '12-1 momentum'), part('pct_from_high', 'from 52-week high'),
                                 part('vs_sma200_pct', 'vs 200-day average'),
                                 f'RSI {t["rsi14"]}' if t.get('rsi14') is not None else None,
                                 f'volatility {t["vol_60d_pct"]}% a year' if t.get('vol_60d_pct') else None) if x) + (
        f'. Chart: {t["setup"]}' + (f'; {t["cross"]}' if t.get('cross') else '') if t.get('setup') else '')


def aud_per_usd():
    return 1 / quote('AUDUSD=X')


# ---------------------------------------------------------------- sandbox money path
def ibkr_fee(qty, price):
    """IBKR Pro fixed pricing for US stocks: $0.005/share, $1 minimum, 1% of value maximum."""
    return round(min(max(1.0, 0.005 * qty), 0.01 * qty * price), 2)


def fill(c, prop, price):
    """Apply an approved SANDBOX proposal at `price`: (fee, realised gain or None for a BUY). Raises ValueError,
    changing nothing."""
    aid, sym, q = prop['account_id'], prop['symbol'], prop['qty']
    if prop['order_type'] == 'LIMIT':
        lim = prop['limit_price']
        if (prop['side'] == 'BUY' and price > lim) or (prop['side'] == 'SELL' and price < lim):
            raise ValueError(f'limit ${lim:,.2f} not reached (now ${price:,.2f}); left pending')
    fee = ibkr_fee(q, price)
    cash = c.execute('SELECT cash FROM accounts WHERE id=?', (aid,)).fetchone()['cash']
    pos = c.execute('SELECT qty, avg_price FROM positions WHERE account_id=? AND symbol=?',
                    (aid, sym)).fetchone()
    held, avg = (pos['qty'], pos['avg_price']) if pos else (0, 0)
    if prop['side'] == 'BUY':
        cost = q * price + fee
        if cost > cash + 1e-9:
            raise ValueError(f'needs ${cost:,.2f}, cash is ${cash:,.2f}')
        c.execute('UPDATE accounts SET cash=cash-? WHERE id=?', (cost, aid))
        c.execute('INSERT OR REPLACE INTO positions VALUES (?,?,?,?)',
                  (aid, sym, held + q, (held * avg + cost) / (held + q)))
        return fee, None
    else:
        if q > held + 1e-9:
            raise ValueError(f'holds {held:g} {sym}, cannot sell {q:g}')
        c.execute('UPDATE accounts SET cash=cash+? WHERE id=?', (q * price - fee, aid))
        if held - q < 1e-9:
            c.execute('DELETE FROM positions WHERE account_id=? AND symbol=?', (aid, sym))
        else:
            c.execute('UPDATE positions SET qty=? WHERE account_id=? AND symbol=?', (held - q, aid, sym))
    return fee, q * (price - avg) - fee  # avg already carries the buy fees


# ---------------------------------------------------------------- account state, all USD
def state(c, a):
    """{'positions': [...], 'cash', 'value', 'aud_per_usd', 'at'} in USD for either mode."""
    if a['mode'] == 'SANDBOX':
        rows = []
        for p in c.execute('SELECT * FROM positions WHERE account_id=? ORDER BY symbol', (a['id'],)):
            try:
                last = quote(p['symbol'])
            except Exception:
                last = None
            rows.append({'symbol': p['symbol'], 'qty': p['qty'], 'avg': p['avg_price'], 'last': last})
        try:
            rate = aud_per_usd()
        except Exception:
            rate = None
        return finish(rows, a['cash'], rate, now())
    snap = json.loads(a['snapshot'] or '{}')
    if not snap:
        return None
    bal = {b['currency']: b for b in snap.get('balances', {}).get('balances', [])}
    rate = bal.get('USD', {}).get('exchange_rate')  # AUD per USD, IBKR's own
    rows = [{'symbol': p['contract_description'], 'qty': p['position'], 'avg': p['average_price'],
             'last': p['market_price'], 'conid': p['contract_id']}
            for p in snap.get('positions', {}).get('positions', []) if p.get('currency') == 'USD']
    # Cash in USD terms: every currency's cash converted, so AUD cash is not invisible.
    cash = snap['summary']['total_cash_value'] / rate if rate else bal.get('USD', {}).get('cash_balance', 0)
    s = finish(rows, cash, rate, a['snapshot_at'])
    s['cash_usd_only'] = bal.get('USD', {}).get('cash_balance')
    s['value'] = snap['summary']['net_liquidation'] / rate if rate else s['value']
    return s


def finish(rows, cash, rate, at):
    for r in rows:
        r['value'] = r['qty'] * r['last'] if r['last'] is not None else None
        r['pnl'] = (r['last'] - r['avg']) * r['qty'] if r['last'] is not None else None
    value = cash + sum(r['value'] or 0 for r in rows)
    return {'positions': rows, 'cash': cash, 'value': value, 'aud_per_usd': rate, 'at': at}


# ---------------------------------------------------------------- headless Claude
def claude(prompt, tools, schema, kind, account_id=None, timeout=1500, model='sonnet', effort=None):
    """Run `claude -p`; return (structured_output, {tool_name: raw result text}).

    stream-json exposes each tool's raw result, so IBKR numbers are stored as IBKR sent
    them rather than as Claude retyped them."""
    # Every run is a fresh session: nothing carries over, so nothing needs compacting. What these
    # flags cut is the fixed cost each turn re-reads: only the built-in tools this job uses (plus
    # ToolSearch, without which all 70+ connector tool definitions load up front), no skills
    # list, no transcript on disk. Measured 2026-09-25: ~23k -> ~9.3k tokens of context per turn.
    # --bare would cut more but skips the login ("Not logged in").
    # The model is always named: left out, a run takes whatever /model was last set to in an
    # interactive session, so the app's cost would follow someone's unrelated choice.
    builtin = ','.join([t for t in tools if not t.startswith('mcp__')] + ['ToolSearch'])
    cmd = [CLAUDE, '-p', '--output-format', 'stream-json', '--verbose', '--json-schema', json.dumps(schema),
           '--tools', builtin, '--disable-slash-commands', '--no-session-persistence',
           '--model', model, *(['--effort', effort] if effort else []),
           # A write tool is unblocked only for the run that names it (an approved LIVE draft); every other
           # run keeps all of them blocked.
           *(['--allowedTools', *tools] if tools else []), '--disallowedTools', *[d for d in DENY if d not in tools]]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         cwd=HOME, text=True)
    killer = threading.Timer(timeout, p.kill)
    killer.start()
    p.stdin.write(prompt)
    p.stdin.close()
    names, raw, out, steps, other, texts = {}, {}, None, [], [], []
    for line in p.stdout:  # read as it streams, so the progress bar moves during the run
        try:
            ev = json.loads(line)
        except ValueError:
            other.append(line)
            continue
        if not isinstance(ev, dict):
            continue
        if ev.get('type') == 'result':
            out = ev
        msg = ev.get('message')  # some events (retries, warnings) carry a plain string here
        for part in (msg.get('content') or [] if isinstance(msg, dict) else []):
            if not isinstance(part, dict):
                continue
            if part.get('type') == 'tool_use':
                names[part['id']] = part['name']
                steps.append(step(part['name'], part.get('input') or {}))
                progress_step(steps[-1])
            elif part.get('type') in ('text', 'thinking') and ev.get('type') == 'assistant':
                note = (part.get('text') or part.get('thinking') or '').strip()
                if note:
                    steps.append(('note', None, note[:3000]))
                    if part.get('type') == 'text':
                        texts.append(note)
            elif part.get('type') == 'tool_result':
                body = part.get('content')
                if isinstance(body, list):
                    body = ''.join(b.get('text', '') for b in body if isinstance(b, dict))
                full = names.get(part.get('tool_use_id'), '?')
                raw[full] = raw[full.rsplit('__', 1)[-1]] = body  # full name tells two accounts apart
                if names.get(part.get('tool_use_id')) == 'WebSearch' and isinstance(body, str) and 'Links: ' in body:
                    try:
                        links = json.JSONDecoder().raw_decode(body, body.index('Links: ') + 7)[0]
                        steps.append(('found', 'WebSearch', json.dumps([{'title': x.get('title', ''), 'url': x.get('url', '')}
                                                                         for x in links[:10]])))
                    except (ValueError, AttributeError):
                        pass
    p.wait()
    killer.cancel()
    # What each run costs. On the subscription nothing is billed per call: total_cost_usd is
    # what the same tokens would cost on the API, the fairest yardstick for "how expensive".
    u = (out or {}).get('usage') or {}
    with db() as c:
        uid = c.execute('INSERT INTO usage (at, kind, account_id, ok, cost_usd, input_tokens, output_tokens, '
                  'cache_read_tokens, cache_write_tokens, turns, seconds, model) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                  (now(), kind, account_id, int(bool(out) and not out.get('is_error')), (out or {}).get('total_cost_usd'),
                   u.get('input_tokens'), u.get('output_tokens'), u.get('cache_read_input_tokens'),
                   u.get('cache_creation_input_tokens'), (out or {}).get('num_turns'),
                   ((out or {}).get('duration_ms') or 0) / 1000, ','.join((out or {}).get('modelUsage') or {}))).lastrowid
        c.executemany('INSERT INTO trace VALUES (?,?,?,?,?)', [(uid, i, *st) for i, st in enumerate(steps)])
    raw['_usage_id'], raw['_texts'] = uid, texts  # _texts: what Claude wrote outside the answer, in full
    if not out or out.get('is_error') or out.get('structured_output') is None:
        tail = (out or {}).get('result') or ''.join(other)[-500:] or 'no result (timed out or killed?)'
        raise RuntimeError(f'claude failed: {tail}')
    return out['structured_output'], raw


PROGRESS_FILE = os.path.join(HOME, 'progress.json')
PROGRESS = None  # set while an analysis runs; refreshes and drafts leave it alone


def progress_write(**kw):
    if PROGRESS is None:
        return
    PROGRESS.update(kw)
    tmp = PROGRESS_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(PROGRESS, f)
    os.replace(tmp, PROGRESS_FILE)


def progress_step(st):
    if PROGRESS is not None:
        kind, tool, detail = st
        label = {'search': f'Searching “{detail[:70]}”', 'fetch': f'Reading {domain(detail)}',
                 'ibkr': f'IBKR {tool}'}.get(kind, 'Writing the answer' if tool == 'final answer' else tool or '')
        progress_write(actions=PROGRESS['actions'] + 1, last=label)


def expected_seconds(kind, default):
    """How long this kind of job usually takes: the mean of the last 5 good runs."""
    with db() as c:
        r = c.execute('SELECT AVG(seconds) FROM (SELECT seconds FROM usage WHERE kind=? AND ok=1 AND seconds>0 '
                      'ORDER BY id DESC LIMIT 5)', (kind,)).fetchone()[0]
    return r or default


def progress_view():
    """Where a running analysis is, for the page: percent is an estimate from past run times."""
    if not running():
        return {'running': False}
    try:
        with open(PROGRESS_FILE) as f:
            p = json.load(f)
    except (OSError, ValueError):
        return {'running': True, 'pct': None, 'step': 'Starting', 'detail': ''}
    n, i = len(p['stages']), p['stage']
    el = time.time() - p['stage_started']
    exp = p['expected'][i]
    pct = 100 * (i + min(0.95, el / exp)) / n
    left = sum(p['expected'][i + 1:]) + max(exp - el, 60)
    return {'running': True, 'pct': round(pct), 'step': f'Step {i + 1} of {n}: {p["stages"][i]}',
            'detail': f'{p["actions"]} actions so far · {p["last"]}',
            'left': f'about {round(left / 60)} min left (estimated from recent runs)',
            'elapsed': round((time.time() - p['started']) / 60)}


def step(name, args):
    """One tool call as (kind, tool, detail) for the trace: what was asked, not what came back."""
    if name == 'WebSearch':
        return 'search', name, args.get('query', '')
    if name == 'WebFetch':
        return 'fetch', name, args.get('url', '')
    if name.startswith('mcp__'):
        return 'ibkr', name.rsplit('__', 1)[-1], json.dumps(args)
    if name == 'StructuredOutput':
        return 'other', 'final answer', ''
    return 'other', name, json.dumps(args)[:500]


def trade_period(a):
    """The shortest IBKR trade window covering the time since this account was last refreshed. The tax
    sync skips trades it has already seen, so re-reading 30 days every day only cost tokens."""
    try:
        days = (datetime.now() - datetime.fromisoformat(a['snapshot_at'])).days
    except (TypeError, ValueError):
        return 'DAYS_90'
    return 'DAYS_7' if days < 6 else 'DAYS_30' if days < 28 else 'DAYS_90'


def refresh_live(c, accts, period=None, perf=False):
    """Load LIVE accounts from IBKR in ONE Claude session. Returns {account id: error} for any that failed.
    Haiku: the job is only calling tools, and the numbers are stored from the raw tool results. perf: also IBKR's
    time-weighted returns (a year of daily figures, so only on the daily run)."""
    names = ['get_account_summary', 'get_account_positions', 'get_account_balances', 'get_account_trades'] + \
        (['get_pa_performance_all_periods'] if perf else [])
    lines, tools = [], []
    for a in accts:
        pre = CONNECTORS[a['owner']]
        tools += [pre + n for n in names]
        lines.append(f'- {", ".join(pre + n for n in names)}; get_account_trades with period {period or trade_period(a)}')
    forget_auth_failures([a['owner'] for a in accts])
    try:
        _, raw = claude('Call each of these tools exactly once, and nothing else:\n' + '\n'.join(lines) +
                        '\nThen reply {"ok": true}.', tools,
                        {'type': 'object', 'properties': {'ok': {'type': 'boolean'}}, 'required': ['ok']},
                        'refresh', accts[0]['id'] if len(accts) == 1 else None, 300, model='haiku')
    except RuntimeError:
        raw = {}  # the per-account check below says which account's data is missing
    errors = {}
    signin = needs_signin()
    for a in accts:
        pre = CONNECTORS[a['owner']]
        if pre + 'get_account_summary' not in raw:  # the connector's tools were missing: its sign-in lapsed
            # Seen both when a sign-in lapsed and during a claude.ai outage (2026-09-30: every connector failed
            # for an hour, then worked again unchanged), so it says to retry before reconnecting.
            errors[a['id']] = RuntimeError(
                f'the IBKR connection for {a["owner"].title()} did not answer. Try again in a while; if it keeps '
                'failing, reconnect it at claude.ai, Settings, Connectors (the account holder signs in).')
            ibkr_status(c, a['owner'], 'sign-in' if CONNECTOR_NAMES[a['owner']] in signin else 'no answer')
            continue
        ibkr_status(c, a['owner'], None)
        snap = {'summary': json.loads(raw[pre + 'get_account_summary']),
                'positions': json.loads(raw[pre + 'get_account_positions']),
                'balances': json.loads(raw[pre + 'get_account_balances'])}
        c.execute('UPDATE accounts SET snapshot=?, snapshot_at=? WHERE id=?', (json.dumps(snap), now(), a['id']))
        c.commit()
        remember_conids(c, [(p['contract_description'], p['contract_id']) for p in snap['positions'].get('positions', [])])
        if pre + 'get_pa_performance_all_periods' in raw:
            try:
                save_perf(c, a['id'], json.loads(raw[pre + 'get_pa_performance_all_periods']))
            except (ValueError, KeyError, TypeError) as ex:
                print('performance: skipped:', ex, file=sys.stderr)
        if pre + 'get_account_trades' in raw:
            tax.sync(c, a['owner'], raw[pre + 'get_account_trades'])
            match_fills(c, a['id'], raw[pre + 'get_account_trades'])
    return errors


def save_perf(c, aid, pa):
    """Store IBKR's daily time-weighted returns from its performance report (the last month: it covers any days the
    daily run missed). Unlike the account's value, they leave out deposits and withdrawals."""
    acct = next(iter(pa['accounts'].values()))
    p = acct['periods']['1M']
    prev, rows = 0.0, []
    for d, x in zip(p['dates'], p['cps']):
        rows.append((aid, f'{d[:4]}-{d[4:6]}-{d[6:]}', (1 + x) / (1 + prev) - 1, acct.get('base_currency') or 'USD'))
        prev = x
    c.executemany('INSERT OR REPLACE INTO perf VALUES (?,?,?,?)', rows)
    c.commit()


def needs_signin():
    """The connectors Claude Code has marked as needing a new sign-in."""
    try:
        with open(AUTH_CACHE) as f:
            return set(json.load(f))
    except (OSError, ValueError):
        return set()


def forget_auth_failures(owners):
    """Drop these owners' connectors from Claude Code's needs-sign-in list, so a run tries them again: a connector that
    is still signed out is simply listed again by that run."""
    names = {CONNECTOR_NAMES[o] for o in owners}
    try:
        with open(AUTH_CACHE) as f:
            d = json.load(f)
        if not names & set(d):
            return
        for n in names:
            d.pop(n, None)
        tmp = AUTH_CACHE + '.stock'
        with open(tmp, 'w') as f:
            json.dump(d, f)
        os.replace(tmp, AUTH_CACHE)
    except (OSError, ValueError):
        pass


def ibkr_status(c, owner, problem):
    """Record whether an owner's IBKR connection answered: problem is 'sign-in', 'no answer' or None (it worked). The
    first failure's time is kept, so the alert says since when."""
    k = f'ibkr_down_{owner}'
    if problem is None:
        c.execute('DELETE FROM meta WHERE k=?', (k,))
    else:
        since = json.loads((c.execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone() or ['{}'])[0]).get('since') or now()
        c.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (k, json.dumps({'problem': problem, 'since': since, 'at': now()})))
    c.commit()


def ibkr_alert(c):
    """A banner for the top of the page while an IBKR connection is failing, with what to do about it."""
    h = ''
    for owner in CONNECTORS:
        r = c.execute('SELECT v FROM meta WHERE k=?', (f'ibkr_down_{owner}',)).fetchone()
        a = c.execute('SELECT * FROM accounts WHERE mode="LIVE" AND owner=?', (owner,)).fetchone()
        if not r or not a:
            continue
        x, name = json.loads(r[0]), owner.title()
        last = f'{datetime.fromisoformat(a["snapshot_at"]):%-d %b %H:%M}' if a['snapshot_at'] else 'never'
        head = (f'{name} needs to sign in to IBKR again' if x['problem'] == 'sign-in' else
                f'IBKR is not answering for {name}')
        what = (f'{name}\'s IBKR sign-in at claude.ai has lapsed, so the app cannot load {name}\'s account or analyse the '
                'LIVE household.' if x['problem'] == 'sign-in' else
                'This is sometimes a short claude.ai or IBKR outage; if it keeps happening, sign in again.')
        h += (f'<div class=alert role=alert><b>{head}</b><p>{what} Failing since {datetime.fromisoformat(x["since"]):%-d %b %H:%M}; '
              f'figures shown are from {last}.</p><ol><li>{name} opens <a href="https://claude.ai/settings/connectors" '
              f'rel=noreferrer target=_blank>claude.ai, Settings, Connectors</a>.</li><li>Under "{e(CONNECTOR_NAMES[owner][10:])}", '
              f'Connect, and log in to IBKR.</li><li>Then check here:</li></ol>'
              f'<form method=post action="/a/{a["id"]}/refresh"><button data-busy="Checking IBKR… up to 30 s">Check {name}\'s '
              f'connection</button></form></div>')
    return h


def match_fills(c, aid, trades_json):
    """Mark this account's drafted proposals filled once IBKR reports the trades. The owner submits each draft in the
    IBKR app, so this is the first the app hears of it. One IBKR order is one draft: an order is matched to a draft of
    the same symbol and side approved before it traded, the one with the same share count first (the latest such), and
    only failing that to a larger one, as a partial fill. A draft left unfilled when a later draft of the same trade
    was filled was not submitted (rejected in IBKR, or replaced), so it is closed. Returns how many were marked filled."""
    orders = {}
    for t in json.loads(trades_json).get('trades', []):
        if t.get('sec_type') == 'STK':
            orders.setdefault(t.get('order_id') or t['trade_id'], []).append(t)
    drafts = [dict(p) for p in c.execute('SELECT * FROM proposals WHERE account_id=? AND status="drafted"', (aid,))]
    n = 0
    for ts in sorted(orders.values(), key=lambda ts: min(t['trade_time'] for t in ts)):
        first = datetime.fromisoformat(min(t['trade_time'] for t in ts).replace('Z', '+00:00'))
        qty = sum(t['size'] for t in ts)
        cands = [p for p in drafts if p['status'] == 'drafted' and p['symbol'] == ts[0]['symbol'] and p['side'] == ts[0]['side']
                 and datetime.fromisoformat(p['decided_at']).replace(tzinfo=learn.SYD) <= first]
        exact = [p for p in cands if abs(p['qty'] - qty) < 1e-9]
        bigger = [p for p in cands if p['qty'] > qty + 1e-9]
        if exact:
            p = max(exact, key=lambda p: p['decided_at'])
            p['status'] = 'filled'
            c.execute('UPDATE proposals SET status="filled", fill_price=?, fee=?, note="filled in IBKR" WHERE id=?',
                      (sum(t['size'] * t['price'] for t in ts) / qty, round(sum(t.get('commission') or 0 for t in ts), 2), p['id']))
            plan_on_fill(c, c.execute('SELECT * FROM accounts WHERE id=?', (aid,)).fetchone(),
                         c.execute('SELECT * FROM proposals WHERE id=?', (p['id'],)).fetchone())
            n += 1
        elif bigger:
            p = max(bigger, key=lambda p: p['decided_at'])
            c.execute('UPDATE proposals SET note=? WHERE id=?', (f'partly filled in IBKR: {qty:g} of {p["qty"]:g}', p['id']))
    for p in drafts:
        if p['status'] == 'drafted' and c.execute(
                'SELECT 1 FROM proposals WHERE account_id=? AND symbol=? AND side=? AND status="filled" AND decided_at>?',
                (aid, p['symbol'], p['side'], p['decided_at'])).fetchone():
            c.execute('UPDATE proposals SET status="expired", note="not submitted in IBKR: a later draft of this trade was filled" '
                      'WHERE id=?', (p['id'],))
    c.commit()
    return n


def draft_order(c, a, p):
    """LIVE approval: create an IBKR order INSTRUCTION (not an order). Returns its URL."""
    tool = CONNECTORS[a['owner']] + 'create_order_instruction'
    args = {'side': p['side'], 'contract_id_ex': str(p['conid']), 'quantity': p['qty'],
            'order_type': p['order_type'], 'time_in_force': 'DAY'}
    if p['order_type'] == 'LIMIT':
        args['limit_price'] = p['limit_price']
    out, raw = claude(f'Call {tool} exactly once with exactly these arguments and nothing else:\n'
                      f'{json.dumps(args)}\nReply with the URL and instruction id it returns.',
                      [tool], {'type': 'object', 'properties': {'url': {'type': 'string'},
                               'instruction_id': {'type': 'string'}}, 'required': ['url']}, 'order draft', a['id'], 300)
    if 'create_order_instruction' not in raw:
        raise RuntimeError('the instruction tool was never called')
    m = re.search(r'https://[^"\s]+', raw['create_order_instruction'])
    return m.group(0) if m else out['url']


# ---------------------------------------------------------------- daily analysis
SOURCES = {'type': 'array', 'items': {'type': 'object', 'required': ['name'], 'properties': {
    'name': {'type': 'string'}, 'url': {'type': 'string'}, 'date': {'type': 'string'}}}}
# The facts behind each card (trend, news, analysts, valuation) are in the shared research notes and shown
# from there, so a household run writes only what is particular to that portfolio. Two households holding
# the same stocks no longer each write a full page about every one of them.
# The exit plan written with every BUY, and once for each holding that has none. The stop is a review point, not an
# automatic sale (Alex, 2026-09-29): quality stocks often dip through a stop and recover.
PLAN = {'type': 'object', 'required': ['horizon_months', 'target_price', 'stop_price', 'exit_if', 'review_by'], 'properties': {
    'horizon_months': {'type': 'integer', 'minimum': 1}, 'target_price': {'type': 'number'}, 'stop_price': {'type': 'number'},
    'exit_if': {'type': 'string'}, 'review_by': {'type': 'string', 'pattern': r'^\d{4}-\d\d-\d\d$'}, 'changed': {'type': 'string'}}}
CARD = {'type': 'object', 'required': ['symbol', 'verdict', 'headline', 'fit'], 'properties': {
    'symbol': {'type': 'string'}, 'verdict': {'enum': ['HOLD', 'ADD', 'TRIM', 'SELL', 'BUY', 'WATCH']},
    'headline': {'type': 'string'}, 'fit': {'type': 'string'}, 'sources': SOURCES, 'plan': PLAN}}
PROPOSALS = {'type': 'object', 'required': ['analysis', 'proposals'], 'properties': {
    'analysis': {'type': 'object', 'required': ['portfolio', 'method', 'holdings', 'opportunities', 'cash_plan'], 'properties': {
        'portfolio': {'type': 'string'},
        'method': {'type': 'string'},
        'holdings': {'type': 'array', 'items': CARD},
        'opportunities': {'type': 'array', 'maxItems': 5, 'items': CARD},
        'cash_plan': {'type': 'object', 'required': ['summary', 'accounts', 'options'], 'properties': {
            'summary': {'type': 'string'},
            'accounts': {'type': 'array', 'items': {'type': 'object',
                'required': ['account', 'cash_usd', 'reserve_usd', 'park_usd'], 'properties': {
                'account': {'type': 'string'}, 'cash_usd': {'type': 'number'},
                'reserve_usd': {'type': 'number'}, 'park_usd': {'type': 'number'}}}},
            'options': {'type': 'array', 'items': {'type': 'object',
                'required': ['name', 'yield_pct', 'risk', 'after_tax', 'verdict'], 'properties': {
                'name': {'type': 'string'}, 'symbol': {'type': 'string'}, 'yield_pct': {'type': 'number'},
                'risk': {'type': 'string'}, 'after_tax': {'type': 'string'},
                'verdict': {'enum': ['RECOMMENDED', 'ALTERNATIVE', 'NOT NOW']}}}}}}}},
    'proposals': {'type': 'array', 'maxItems': 5, 'items': {'type': 'object',
        'required': ['symbol', 'conid', 'side', 'quantity', 'order_type', 'ref_price', 'reason', 'confidence'],
        'properties': {'symbol': {'type': 'string'}, 'conid': {'type': 'integer'},
                       'side': {'enum': ['BUY', 'SELL']}, 'quantity': {'type': 'integer', 'minimum': 1},
                       'order_type': {'enum': ['MARKET', 'LIMIT']}, 'limit_price': {'type': ['number', 'null']},
                       'ref_price': {'type': 'number'}, 'reason': {'type': 'string'},
                       'confidence': {'enum': ['low', 'medium', 'high']}, 'plan': PLAN}}}}}

BRIEF_PROMPT = '''You are the research desk for a US stock portfolio advisor. Today is {today} in Sydney; the US market opens later today. Write a market brief in markdown, at most 800 words:
1. US indexes. Start with this table, computed by the app from Yahoo daily closes; copy it as it is, then one or two lines on what it shows:
{indexes}
2. Rates, the Fed, and macro data due this week. The latest US figures, from FRED; use them rather than searching,
and say what changed:
{macro}
3. Policy announcements. What President Trump and his administration announced in the last 3 days, formally
(executive orders, tariff notices, agency rules, official statements) or on Truth Social, as reported by Reuters,
Bloomberg, AP, CNBC or the WSJ. For each: what moved and by how much (index, sector, currency, gold, oil), and whether
an earlier announcement has since been delayed, softened or reversed. Then the dated policy deadlines coming up
(tariff dates, negotiations, hearings). If nothing moved markets, say so in one line. Facts only: no guessing at
what will be announced next.
4. Geopolitical developments that move markets (conflicts, sanctions, elections).
5. Markets against their usual pattern. The app's check, from a year of daily changes:
{links}
For every pair marked BROKEN, and any 20-day move that runs against the usual direction (gold falling while the dollar
and real yields fall, oil and energy stocks parting), give the likely reason, with sources: a stronger or weaker
dollar, real yields, forced selling to raise cash, profit-taking after a long run, central bank buying or selling,
positioning, or a policy announcement. Say plainly when you cannot find a reason. Do not assume the old pattern
returns soon.
6. Sector rotation and notable earnings. Sector returns from Finviz, for reference: {sectors}
Market-wide only: news on individual holdings is the research step's job.
The numbers above are done: do not look them up again. Use WebSearch for the news (6 to 9 searches is enough, 2 or 3 of them on policy),
WebFetch only when a result must be read in full. Cite publication and date for every claim.'''
# The brief's verdict on the day, kept so the Saturday scoring can compare advice given on policy-shock days.
POLICY_SHOCK = {'type': 'object', 'required': ['level', 'what'], 'properties': {
    'level': {'enum': ['none', 'minor', 'major'], 'description': 'major: a policy announcement in the last 2 US sessions '
              'moved the S&P 500, or a whole sector, by 2% or more in a day; minor: it moved a sector or some stocks '
              'noticeably but less; none: no announcement moved markets'},
    'what': {'type': 'string', 'description': 'One sentence: the announcement and the move, or empty for none'}}}
INDEXES = [('SPY', 'S&P 500 (SPY)'), ('QQQ', 'Nasdaq 100 (QQQ)'), ('DIA', 'Dow (DIA)'), ('IWM', 'Russell 2000 (IWM)'),
           ('^VIX', 'VIX'), ('^TNX', 'US 10-year yield %'), ('^IRX', 'US 3-month T-bill yield %'),
           ('DX-Y.NYB', 'US dollar index'), ('CL=F', 'WTI crude oil'), ('GC=F', 'Gold')]

RESEARCH_PROMPT = '''You are the research desk for a family's US stock portfolios. Today is {today} in Sydney. Your notes
are shared by every portfolio's advisor, who decides trades from them: be factual and concise, and do NOT
recommend trades.

The app has already gathered the data below by script. Work FROM it: it replaces searching for news, ratings,
valuation and price trends. Use WebSearch or WebFetch only for a real gap: a stock whose data is missing, a headline
whose story matters (an accident, a lawsuit, a guidance cut), or a domicile you cannot state. Name every gap you could
not fill in "gaps".

Today's market brief:
{brief}

Known domicile and dividend withholding for an Australian resident holding through IBKR (checked earlier). Copy them
as they are; do not look them up again:
{facts}

Known IBKR contract ids. Copy them; call search_contracts only for a symbol that is not here:
{conids}

PART 1, STOCKS HELD ({n}). Per stock: Finviz data (sector, country, valuation numbers, analyst rating changes of the
last 60 days, headlines of the last 14 days with source and link), trend statistics from a year of Yahoo closes
(chg_* are % changes over about 1, 3 and 6 months; pct_from_high is below the 52-week high; vs_sma50/vs_sma200 are %
above (+) or below (-) those averages), and from Finnhub: "earnings" (next results date and the last four earnings
surprises against the consensus), "analyst_trend" (buy/hold/sell counts now and 3 months ago: a drift toward buy or
sell matters more than the level) and "insiders" (open-market buys and sales in 90 days; several insiders buying is a
notable signal, sales are routine). Also: "fundamentals" (Finviz: quality such as return on equity, margins and debt;
recent growth; how much funds and insiders own and whether funds are adding; short interest; RSI and daily range),
the chart read by the app in "trend" (mom_12_1_pct is the year's return leaving out the last month; "setup" names the
trend, a golden or death cross, a 55-day breakout, overbought or oversold; support and resistance are recent lows and
highs), and "sec_filings_30d" (the company's current-event filings with the SEC: a restatement, auditor change,
bankruptcy, delisting notice, impairment, restructuring or change of control matters a lot; a new 13D can mean an
activist):
{held}
For each write "summary" (1 or 2 sentences: the trend and what matters now), "news" (the material headlines, each
with date and source, or "no material news"), "analysts" (the rating changes, and the consensus and target from the
numbers, and whether the analyst trend is improving or worsening), "policy" (its exposure to US government policy:
tariffs and trade, China, export controls, drug pricing, defence or infrastructure spending, subsidies, antitrust,
government contracts; and any announcement in the brief that touches it, with date; "none notable" when so). Mention an upcoming results date, a run of
earnings beats or misses, insider buying, a weak balance sheet and a material SEC filing in "summary" when they matter. Domicile and withholding: from the known list; for a symbol not in it, look up (one search is usually
enough; the app stores the answer, so it is looked up only once) the country of incorporation and the dividend
withholding that applies (US companies and US-domiciled ETFs: 15% US withholding with a W-8BEN; foreign
companies and ADRs follow their home country, e.g. Chubb is Swiss and pays from capital contribution reserves free of
Swiss withholding; Finviz's "country" is the headquarters, not always the incorporation); write "unverified" rather
than guess. "sources": the headlines and pages you relied on, with dates.
{more}'''
RESEARCH_MORE = '''
PART 2, SECTORS. Sector returns (Finviz): {sectors}
The portfolios' weight by sector, all accounts together: {weights}.
In "sectors", say which 2 or 3 sectors look strongest and weakest now, and which the portfolios lack or are thin in.

PART 3, CANDIDATES. The app screened the strongest sectors ({strong}) and sectors the portfolios lack ({missing})
on Finviz for large caps with P/E under 30, above their 200-day average and rated Buy or better. The pool, with the
same data as part 1:
{pool}
Analyst upgrades today (MarketBeat): {upgrades}
Shortlist 5 to 8 not held, spread over the sectors, from the pool (or an upgraded large cap, saying why): the same
notes as part 1 plus "sector", "why" it made the list, and its conid.

{parking}'''
PARKING_PROMPT = '''PART 4, PARKING idle cash. Compare: leaving USD at IBKR (look up IBKR's current rate; the first $10,000 earns
nothing), short T-bill ETFs SGOV and BIL, dividend ETFs SCHD and VYM. Finviz data for the ETFs:
{etfs}
For each: yield, risk, domicile and withholding, with a source, and the ETF's conid.'''
PARKING_CACHED = 'PART 4 (parking) is not needed: those options were researched on {at} and are reused for a week.'
RESEARCH_TOP_UP = '''
This run only adds stocks missing from today's notes: return empty "candidates" and an empty "sectors".'''
PARK_ETFS = ['SGOV', 'BIL', 'SCHD', 'VYM']
PARK_DAYS = 7  # yields and the IBKR cash rate move slowly: re-research the parking options weekly
NOTE = {'type': 'object', 'required': ['symbol', 'conid', 'domicile', 'withholding', 'summary', 'news', 'analysts',
                                       'sources'], 'properties': {
    'symbol': {'type': 'string'}, 'conid': {'type': 'integer'}, 'domicile': {'type': 'string'},
    'withholding': {'type': 'string'}, 'summary': {'type': 'string'}, 'news': {'type': 'string'},
    'analysts': {'type': 'string'}, 'policy': {'type': 'string'}, 'sources': SOURCES}}  # valuation and trend are added by the app
NOTE['required'].append('policy')
CANDIDATE = json.loads(json.dumps(NOTE))
CANDIDATE['required'] += ['sector', 'why']
CANDIDATE['properties'].update({'sector': {'type': 'string'}, 'why': {'type': 'string'}})
RESEARCH = {'type': 'object', 'required': ['stocks', 'sectors', 'candidates', 'parking', 'gaps'], 'properties': {
    'stocks': {'type': 'array', 'items': NOTE}, 'sectors': {'type': 'string'},
    'candidates': {'type': 'array', 'maxItems': 8, 'items': CANDIDATE},
    'parking': {'type': 'array', 'items': {'type': 'object',
        'required': ['name', 'yield_pct', 'risk', 'domicile', 'withholding', 'source'], 'properties': {
        'name': {'type': 'string'}, 'symbol': {'type': 'string'}, 'conid': {'type': 'integer'},
        'yield_pct': {'type': 'number'}, 'risk': {'type': 'string'}, 'domicile': {'type': 'string'},
        'withholding': {'type': 'string'}, 'source': {'type': 'string'}}}},
    'gaps': {'type': 'string'}}}

ACCOUNT_PROMPT = '''You advise ONE investment ({mode}) held in {n} brokerage account(s): {names}.
The split into accounts is administrative: judge concentration, sector mix, risk, opportunities and
cash across all of them together, as one portfolio. Today is {today} in Sydney.

Current state in USD, per account (positions, cash, total value):
{state}

Combined: total value ${total:,.0f}, cash ${cash:,.0f}.

Today's market brief:
{brief}

Today's research notes, shared by every portfolio (facts, not decisions): notes on each stock held here, the sector
view, the candidate stocks, and the cash-parking options:
{research}

Price statistics computed by the app from Yahoo daily closes, for the stocks held here:
{stats}

Risk, computed by the app from 60 days of daily returns: the portfolio's beta to the S&P 500 (cash included), the
holdings that move together (correlation of daily returns of 0.75 or more: they are one bet, whatever their sectors),
and per stock its beta, yearly volatility and "size_usd", the position size that gives it the same risk as a typical
holding (a stock twice as volatile gets half the money). For candidates, "corr_with_portfolio": below about 0.5 adds
diversification, above 0.8 adds more of what is already held:
{risk}

Known IBKR contract ids: {conids}

Exit plans, per stock held: the plan on record ("missing" if none), any trigger the app found today (target
reached, below the review level, review date or holding period reached), and for LIVE accounts when each owner's
shares qualify for Australia's 50% capital gains discount (held more than 12 months):
{plans}

The owners' recent decisions on your earlier proposals (learn from rejections):
{history}

Tax position per owner (LIVE accounts only; from the tax register): capital gains already realised this Australian
financial year (1 July to 30 June): gains held 12 months or more ("long"), under 12 months ("short"), losses, losses
carried in from earlier years, and "net_gain", the estimated taxable gain after losses and the 50% discount. A loss
realised now saves the most when it offsets short gains. Then the parcels now at a loss that could be sold to offset
them. A sale is matched to the owner's parcels that add the least tax first:
{tax}

Lessons the owners approved from past monthly reviews of how your proposals turned out. Follow them unless today's
evidence clearly says otherwise, and then say why in "method":
{lessons}

Decide in three steps, working FROM THE NOTES. Look something up yourself (WebSearch, WebFetch, IBKR tools) only to
fill a gap in the notes or to check a price right before proposing a trade, and say in "method" what you looked up
and why.

STEP 1, HOLDINGS. A verdict on every stock held in any of the accounts, once per stock, for THIS portfolio: its weight
in the combined total, what it adds or duplicates, and whose account holds it.

STEP 2, OPPORTUNITIES. From the research candidates, pick the 3 to 5 that suit THIS portfolio best: they fill its
 gaps and add little overlap with what it holds. Each becomes an "opportunities" entry: verdict BUY when it is worth
 buying now (then also propose the trade if the rules allow), WATCH when it is good but the price, timing or cash is
 not right yet (say what would trigger a buy). You may add a name that is not a candidate only with a reason.

STEP 3, CASH PLAN. {cash_rule}

'''
SECTORS_URL = 'https://finviz.com/groups.ashx?g=sector&v=140&o=-perf13w'
SCREEN_URL = ('https://finviz.com/screener.ashx?v=111&f=cap_largeover,fa_pe_u30,sh_avgvol_o1000,ta_sma200_pa,'
              'an_recom_buybetter&o=-marketcap')
UPGRADES_URL = 'https://www.marketbeat.com/ratings/upgrades-downgrades/'
MAX_WEIGHT = 0.25  # no single stock above this share of a household's combined total (also in RULES)
PARK_ABOVE_USD = 3000  # cash above this is worth putting to work while waiting (Alex, 2026-09-24)
CASH_RULE = '''Cash CANNOT move between accounts: each account's cash can only buy in that account, so plan cash
per account. {per_account}
For each account above the threshold decide how much to keep ready for the opportunities in step 2 ("reserve_usd")
and how much can be parked meanwhile ("park_usd"); an account at or below it keeps all its cash (park_usd 0).
List every account in cash_plan.accounts. Compare the parking options in the research notes (IBKR interest,
T-bill ETFs, dividend ETFs), weighing the price risk a dividend payer carries while money is only waiting.
For each option give the yield, the risk and "after_tax" (ONE short sentence each: they are shown on a phone) for an Australian tax resident, using the withholding
that really applies to THAT security (see the domicile rule below), the rest taxed as income in Australia, and a
capital gain or loss when sold. Mark one RECOMMENDED. A parking BUY goes in the account whose cash it parks.'''
NO_CASH_RULE = '''Cash cannot move between accounts. {per_account}
No account is above the threshold: no parking needed. Return a one-line "summary" saying so, list each account in
cash_plan.accounts with reserve_usd = its cash and park_usd = 0, and no options.'''

RULES = '''Rules, all mandatory:
- Every proposal names the "account" it is placed in. A SELL must be of shares held in THAT
  account. A BUY must fit THAT account's own cash plus the proceeds of SELLs in the same account:
  cash cannot move between accounts. In a LIVE account only its USD cash ("cash_usd_only") can pay for a buy;
  its other cash would have to be converted in IBKR first, so say so in the reason if a buy relies on it.
- Avoid holding the same stock in two accounts. Adding to a stock goes in the account that already
  holds it; a new stock goes in one account only. Split a stock across accounts only when the
  account holding it cannot pay, and then say so in the reason.
- Each account belongs to a different person, and Australian residents are taxed separately: a
  capital loss only offsets gains realised by the same person. Say whose gain or loss each sale
  realises, and place trades in the account where the tax outcome is better.
- US-listed stocks and ETFs only. No options, futures, shorting, margin or leverage.
- Whole shares. Take each conid from the research notes or the known ids; call search_contracts only for a symbol
  in neither.
- Total BUY cost must fit the USD cash available plus the proceeds of SELLs in the same list.
- No single stock may exceed 25% of the COMBINED total value after the trades (the app refuses a buy that would).
- 0 to 5 proposals. Proposing nothing is a good answer when nothing is compelling; never
  trade for the sake of trading. The owners are Australian tax residents: selling a winner
  realises a capital gain, so say when a sale would.
- Every reason cites its evidence (source and date for news, the numbers for price trends).
- Tax depends on where the company is domiciled, not where it is listed: never assume US
  withholding. For every BUY, and every cash-plan option, take the issuer's country of
  incorporation and dividend withholding from the research notes (look it up only if missing)
  (US companies and US-domiciled ETFs: 15% US withholding with a W-8BEN; ADRs and foreign
  companies listed in the US follow their home country's rules, e.g. Chubb is Swiss and pays out
  of capital contribution reserves free of Swiss withholding; an Australian company's franking may
  or may not reach an ADR holder). State the domicile and the withholding in the reason, with a
  source. If you cannot verify it, say "withholding unverified" rather than guess.
- Exit plans. Every BUY carries a "plan": horizon_months (how long you mean to hold it), target_price (where you
  would take profit or re-check the case), stop_price (a REVIEW level, not an automatic sale), exit_if (1 or 2 plain
  sentences: what would break the thesis) and review_by (a date, YYYY-MM-DD). For a holding whose plan is "missing",
  put a "plan" on its card. horizon_months counts from the plan's set_on date, which a changed plan keeps. Change an existing plan only when the thesis has changed, and then say why in "plan.changed".
- A holding with a trigger: propose a SELL or TRIM, or say in its "fit" why you hold anyway. Falling through the stop
  alone is not a reason to sell a sound business; a broken thesis is.
- Capital gains discount: selling a winner within 2 months before its discount date gives up half the tax discount;
  do it only for a strong, stated reason.
- Results dates: the notes give each stock's next earnings date. A BUY within 5 trading days before results must say
  in the reason why it is worth buying before them rather than after; say so too when a SELL comes just before them.
- Chart and momentum. Read each stock's "trend" (the setup, 12-1 momentum, RSI, support and resistance) as evidence,
  never as the whole case. Prefer buying stocks in an uptrend with positive 12-1 momentum, or near their 52-week high
  (that nearness is a strength, not a reason to wait). Do not add to a stock in a downtrend (below a falling 200-day
  average, or a recent death cross) unless the reason names a concrete catalyst, and then say so. Do not chase a
  stretched or overbought stock (RSI above 75, or 15% above its 50-day average): use a limit near support or make it
  WATCH. A pullback within an uptrend towards the 50-day average or support is the preferred entry. Classic chart shapes
  and candlesticks count only as the Patterns rule below allows.
- Size by risk. Start from the stock's "size_usd" in the risk figures (the app warns on a buy above 1.5 times it), and
  adjust for conviction. Say the volatility in the reason. Prefer candidates with a low correlation with the portfolio;
  a buy that adds to a group of holdings moving together must say why that concentration is worth it.
- Exit plans from the chart. Set stop_price (the review level) just below a support level from the notes, or about
  two typical monthly moves below the price, not a round percentage. Set target_price with the 60-day resistance and the
  analysts' target in mind.
- Results and option nerves. When a BUY comes before results, state the move the options price in. Unusually high
  implied volatility (above 80% of the past year's days) or a put/call ratio far above usual means the market expects
  trouble: say what the notes show about it.
- Quality. A weak balance sheet (high debt, falling margins) or a serious SEC filing (restatement, auditor change,
  delisting notice, going concern) outweighs a good chart: name it.
- Policy announcements. Read the brief's policy section and each note's "policy". A move caused by an announcement
  is not by itself a reason to trade: announcements are often delayed, softened or reversed within days. Do not sell a
  sound holding into an announcement-driven drop, and do not chase an announcement-driven rally. A policy that changes a
  company's earnings for good (a tariff in force, a lost contract, a price cap) is a thesis change and counts. On a day
  the brief reports a policy shock, prefer LIMIT orders, and say in the reason when a proposal depends on a policy
  staying as it is.
- Markets against their pattern. When the brief marks a relationship BROKEN (gold no longer moving against the dollar
  or real yields, bonds no longer offsetting stocks), do not rely on it, for example gold or bonds as a hedge, without
  saying why it should hold again.
- Investor lenses. Each note's "lenses" gives eight investors' checks, run by the app: Buffett & Munger (quality at a
  fair price), Graham (margin of safety), Greenblatt (Magic Formula), Piotroski (financial health), Lynch (PEG and kind
  of stock), O'Neil (CAN SLIM), Minervini (trend template) and Weinstein (stage). Weigh them with the rest:
  * Prefer BUYs that pass at least one quality lens (Buffett & Munger, Piotroski or Greenblatt) AND one trend lens
    (Minervini, or Weinstein stage 2). A BUY that fails every quality lens must say why it is worth owning anyway.
  * Never BUY or ADD a stock in Weinstein stage 4 (below a falling 30-week average). A holding in stage 4: propose a
    SELL or TRIM, or say on its card why the thesis still holds.
  * Lynch's kind sets the sell discipline: name each holding's kind in its "fit" and judge it by that kind's sell rule.
  * Margin of safety: every BUY reason says what the price already assumes and why there is room for error (P/E or
    P/FCF against growth, or the Graham check). Paying up for quality is allowed; say so.
  * O'Neil's automatic 7-8% stop-loss is NOT used: stop_price stays a review level.
- Patterns. "patterns" lists candlestick and chart patterns on the last whole bar, each marked by the app's own
  scoring as proven, on trial or failed. Only a "proven" pattern counts as evidence, and only alongside other evidence.
  A pattern on trial or failed must never be a reason for a proposal; mention one only to say it was set aside.
- Do not trade small drifts: trim or top up a holding only when its weight is at least 5 points from where you want
  it, or its case has changed. Churn costs fees and tax.
- Tax-loss selling. From 1 April to 30 June, look at the tax position: if an owner has realised gains this financial
  year and holds parcels at a loss whose case is weak, a sale before 30 June offsets them. Never propose buying the
  same stock back soon after (the ATO treats a sale and quick buy-back purely for the tax loss as a wash sale); switch
  to a different stock with a similar role instead. Outside that window, still say when a sale would realise a loss
  that offsets that owner's gains.
- All money in USD.
The "analysis" is shown as one card per stock, and under each card the page shows that stock's research note
(trend, news, analysts, valuation, sources). Do NOT repeat the note: write only what is particular to THIS portfolio.
- "holdings": one entry for EVERY stock held, across all accounts (a stock held in two accounts is one entry).
  "headline" is one plain sentence with the verdict's main reason. "fit" is plain text, 1 to 3 sentences: its weight
  in the combined total, what it adds or duplicates here, whose account holds it, and what would change the verdict.
- "sources" per holding: only sources beyond the research notes (something you looked up yourself); else omit it.
- "opportunities": the step 2 entries, same shape as holdings, verdict BUY or WATCH; "fit" says why it suits this
  portfolio and, for WATCH, what would trigger a buy.
- "cash_plan": the step 3 result; "summary" is plain text.
- "portfolio": plain text, 1 to 3 short paragraphs: concentration, cash level, risks for the coming week.
- "method": plain text, 3 to 6 sentences, for the owner to audit and improve this process: what you
  checked, which evidence weighed most and why, what you could not find or verify.'''
DEEP_PROMPT = '''
THIS IS A SECOND, DEEPER REVIEW. A faster model analysed this portfolio earlier today from the same notes. Its view
and proposals are below. Do not simply agree: test its reasoning, look for risks and opportunities it missed, and
reach your own verdicts. Where you differ, say so and why in "method". Your proposals replace its proposals.
Its portfolio view: {portfolio}
Its proposals: {proposals}
'''


def analyse(only=None, deep=False):
    """The daily pipeline, or with deep=True a second review of one household by Opus that reuses today's brief,
    account data and research: no new brief, refresh or research."""
    c = db()
    today = date.today().isoformat()
    ids = [only] if only else [r['id'] for r in c.execute('SELECT id FROM accounts ORDER BY id')]
    if not deep:
        try:
            credit_dividends(c, today)
        except Exception:
            import traceback
            traceback.print_exc()  # a missed day is caught up on the next run
    groups = {}
    for aid in ids:
        a = c.execute('SELECT * FROM accounts WHERE id=?', (aid,)).fetchone()
        for m in members(c, a):
            groups.setdefault(grp_key(m), {})[m['id']] = m
    # Frozen benchmarks only get their daily value recorded: no brief, research or Claude run for them.
    for key in [k for k, ms in groups.items() if any(m['frozen'] for m in ms.values())]:
        snapshot(c, [(m, state(c, m)) for m in groups.pop(key).values()], today)
    if not groups:
        return
    brief = c.execute('SELECT body FROM reports WHERE account_id IS NULL AND grp IS NULL AND run_date=?', (today,)).fetchone()
    global PROGRESS
    stages, expected = [], []
    if not deep:
        if not brief:
            stages.append('Market brief')
            expected.append(expected_seconds('market brief', 360))
        stages.append('Research (shared by every portfolio)')
        expected.append(expected_seconds('research', 480) if not todays_research(c, today) else 30)
    for ms in groups.values():
        ms = list(ms.values())
        stages.append(('Deep review (Opus): ' if deep else 'Household: ' if len(ms) > 1 else '') +
                      ' + '.join(m['name'] for m in ms))
        expected.append(expected_seconds('deep analysis', 600) if deep else expected_seconds('analysis', 300))
    PROGRESS = {'started': time.time(), 'stages': stages, 'expected': expected, 'stage': 0,
                'stage_started': time.time(), 'actions': 0, 'last': 'Starting'}
    try:
        if deep:
            pack = todays_research(c, today)
            for key, ms in groups.items():
                ms = list(ms.values())
                try:
                    if not (brief and pack):
                        raise RuntimeError('a deep review builds on today\'s research: run the normal analysis first')
                    analyse_group(c, key, ms, brief['body'], pack, today, deep=True)
                except Exception as ex:
                    fail(c, key, ms, today, ex)
        else:
            analyse_stages(c, today, groups, brief)
    finally:
        PROGRESS = None
        try:
            os.remove(PROGRESS_FILE)
        except OSError:
            pass


def full_brief(answer, texts):
    """The brief itself. On 1 Oct 2026 Claude wrote the brief as plain text and answered with a note about it ("Market
    brief for 1 Oct 2026 written ..."), which was stored instead. A short answer without the index table loses to the
    longest text that has it."""
    if '| Index |' in answer or len(answer) > 2000:
        return answer
    best = max((t for t in texts if '| Index |' in t), key=len, default=None)
    return best or answer


def snapshot(c, pairs, today):
    """Record each account's value and SPY for the benchmark charts. pairs: [(account, state)]."""
    try:
        spy = quote('SPY')
        for a, s in pairs:
            if s:
                c.execute('INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?)', (a['id'], today, s['value'], spy))
        c.commit()
    except Exception:
        pass


def plan_on_fill(c, a, p):
    """A filled BUY's plan becomes the holding's plan; a sale that leaves the household without the stock ends it."""
    if p['side'] == 'BUY' and p['plan']:
        save_plan(c, grp_key(a), p['symbol'], json.loads(p['plan']), 'proposal')
    elif p['side'] == 'SELL' and not c.execute(
            f'SELECT 1 FROM positions WHERE symbol=? AND account_id IN ({",".join(str(m["id"]) for m in members(c, a))})',
            (p['symbol'],)).fetchone() and a['mode'] == 'SANDBOX':
        c.execute('DELETE FROM plans WHERE grp=? AND symbol=?', (grp_key(a), p['symbol']))


def save_plan(c, key, symbol, plan, source):
    """A rewrite keeps the plan's start date: the holding period counts from when the plan was first set, so rewriting
    it does not push the "holding period is up" trigger out again."""
    c.execute('INSERT INTO plans VALUES (?,?,?,?,?) ON CONFLICT (grp, symbol) DO UPDATE SET plan=excluded.plan, '
              'source=excluded.source', (key, symbol.upper(), json.dumps(plan), date.today().isoformat(), source))


def plan_triggers(plan, set_on, price, today):
    """What in an exit plan has been reached today, as short sentences. Checked by the app, not by Claude."""
    out = []
    if price and plan.get('target_price') and price >= plan['target_price']:
        out.append(f'reached its target {usd(plan["target_price"])} (now {usd(price)})')
    if price and plan.get('stop_price') and price <= plan['stop_price']:
        out.append(f'below its review level {usd(plan["stop_price"])} (now {usd(price)})')
    if re.fullmatch(r'\d{4}-\d\d-\d\d', str(plan.get('review_by'))) and plan['review_by'] <= today:  # a date, not prose
        out.append(f'review date {plan["review_by"]} reached')
    s = date.fromisoformat(set_on)
    m = s.month - 1 + plan.get('horizon_months', 10 ** 4)
    end = date(s.year + m // 12, m % 12 + 1, min(s.day, 28)).isoformat()
    if end <= today:
        out.append(f'planned holding period of {plan["horizon_months"]} months is up')
    return out


def cgt_text(parcels, today):
    """When one owner's parcels of a stock qualify for the 50% CGT discount (held more than 12 months), from the tax
    register's buy dates. parcels: [(buy_date, qty)]."""
    if not parcels:
        return None
    # The day after the anniversary, as the Tax page counts it (not +366 days: across a 29 February that is the anniversary).
    due = lambda d: (tax.plus_year(date.fromisoformat(d)) + timedelta(days=1)).isoformat()
    later = sorted((due(d), q) for d, q in parcels if due(d) > today)
    if not later:
        return 'all shares qualify for the discount'
    return (f'{sum(q for _, q in later):g} of {sum(q for _, q in parcels):g} shares qualify from {later[0][0]}'
            + (f' (the last on {later[-1][0]})' if later[-1][0] != later[0][0] else ''))


def plans_context(c, key, states, today):
    """({symbol: {plan, triggers, cgt}} for the prompt, {symbol: [triggers]}) for one household's held stocks.
    states: {name: (account, state)}."""
    plans = {r['symbol']: (json.loads(r['plan']), r['set_on'], r['source']) for r in c.execute('SELECT * FROM plans WHERE grp=?', (key,))}
    held = {}
    for a, s in states.values():
        for p in (s or {}).get('positions', []):
            held.setdefault(p['symbol'], []).append((a, p))
    ctx, trig = {}, {}
    for sym, xs in held.items():
        price = xs[0][1].get('last')
        pl = plans.get(sym)
        t = plan_triggers(pl[0], pl[1], price, today) if pl else []
        cgt = {a['name']: x for a, _ in xs if a['mode'] == 'LIVE' and (x := cgt_text(
            [(r['buy_date'], r['buy_qty']) for r in c.execute('SELECT buy_date, buy_qty FROM lots WHERE owner=? AND symbol=? '
                                                              'AND sale_date IS NULL', (a['owner'], sym))], today))}
        ctx[sym] = {'plan': dict(pl[0], set_on=pl[1]) if pl else 'missing', 'triggers': t or 'none', **({'cgt_discount': cgt} if cgt else {})}
        trig[sym] = t
    return ctx, trig


def plan_html(ctx):
    """A holding card's exit plan, triggers and capital gains discount dates. ctx: plans_context()[0][symbol]."""
    if not ctx:
        return ''
    pl, t = ctx['plan'], ctx['triggers']
    h = ''.join(f'<div class=trig>{e(x)}</div>' for x in (t if isinstance(t, list) else []))
    h += (f'<p class=plan><b>Exit plan:</b> {e(plan_line(pl, pl.get("set_on")))}</p>' if isinstance(pl, dict)
          else '<p class=mute>No exit plan yet: the next analysis writes one.</p>')
    h += ''.join(f'<p class=mute style="margin:0">CGT discount, {e(n)}: {e(x)}</p>' for n, x in (ctx.get('cgt_discount') or {}).items())
    return h


def plan_line(plan, set_on=None):
    return (f'Hold {plan["horizon_months"]} months · target {usd(plan["target_price"])} · review below '
            f'{usd(plan["stop_price"])} · review by {plan["review_by"]}' + (f' · set {set_on}' if set_on else '')
            + f'. Exit if: {plan["exit_if"]}')


PLANS_PROMPT = """You set the exit plan for every stock held by ONE family investment ({mode}) held in {n} account(s):
{names}. Treat the accounts as one portfolio. Today is {today} in Sydney. The owners are Australian tax residents.

Current state in USD, per account: {state}
Today's research notes (facts, not decisions) on each stock held: {research}
Price statistics from a year of Yahoo closes: {stats}
Plans on record, written earlier by a faster model, with today's triggers and, for LIVE accounts, when each owner's
shares qualify for the 50% capital gains discount (held more than 12 months): {plans}
Today's market brief: {brief}

For EVERY stock held, write its plan from scratch; do not simply copy the earlier one, but keep what was sound.
- horizon_months: how long this position is meant to be held, given what it is for in this portfolio (core compounder,
  cyclical, income, cash parking, speculative). It counts from the plan's set_on date (kept when you rewrite the plan),
  not from today.
- target_price: where you would take profit or re-check the case, from valuation and analyst targets in the notes, not
  a round number.
- stop_price: a REVIEW level, never an automatic sale. Set it where a fall would mean the thesis may be wrong, not at
  ordinary volatility: look at the stock's own swings (52-week range, distance from its averages).
- exit_if: 1 or 2 plain sentences naming what would break the thesis, specific to this company.
- review_by: a date (YYYY-MM-DD), normally the first results date after today, or sooner if an event is due.
- changed: one sentence on what you changed from the earlier plan and why, or "kept" if you kept it.
Capital gains discount: for a stock in a LIVE account with shares that qualify within the next few months, set the
plan so it does not push a sale just before that date without a strong reason, and say so in exit_if.
Cash parking funds (T-bill ETFs and the like) get a short horizon, a target and stop at the fund's normal range, and
exit_if about the cash being needed or rates falling.
Use WebSearch only for a real gap in the notes."""
PLANS_OUT = {'type': 'object', 'required': ['plans'], 'properties': {'plans': {'type': 'array', 'items': {
    'type': 'object', 'required': ['symbol', 'plan'], 'properties': {'symbol': {'type': 'string'},
                                                                     'plan': {**PLAN, 'required': PLAN['required'] + ['changed']}}}}}}


def rewrite_plans(model='opus'):
    """Every household's exit plans written again from scratch by `model` (Opus by default), reusing today's
    research: no proposals are made or changed. Frozen benchmarks are skipped: they never sell."""
    c = db()
    today = date.today().isoformat()
    pack = todays_research(c, today) or next((json.loads(r['body']) for r in c.execute(
        "SELECT body FROM reports WHERE grp='research' ORDER BY id DESC LIMIT 1")), {'stocks': []})
    brief = (c.execute('SELECT body FROM reports WHERE account_id IS NULL AND grp IS NULL ORDER BY id DESC LIMIT 1').fetchone()
             or {'body': 'none'})['body']
    groups = {}
    for a in c.execute('SELECT * FROM accounts WHERE frozen=0 ORDER BY id'):
        groups.setdefault(grp_key(a), []).append(a)
    for key, ms in groups.items():
        states = {a['name']: (a, state(c, a)) for a in ms}
        mine = sorted({p['symbol'] for _, s in states.values() for p in (s or {}).get('positions', [])})
        if not mine:
            continue
        out, _ = claude(PLANS_PROMPT.format(
            mode=look(ms[0]), n=len(ms), names=', '.join(states), today=today,
            state=json.dumps({n: s for n, (_, s) in states.items()}, default=str),
            research=json.dumps([x for x in pack['stocks'] if x['symbol'] in mine]),
            stats=json.dumps(price_stats(mine)), plans=json.dumps(plans_context(c, key, states, today)[0]), brief=brief),
            ['WebSearch', 'WebFetch'], PLANS_OUT, 'exit plans', ms[0]['id'], timeout=2400, model=model, effort='high')
        for x in out['plans']:
            if x['symbol'].upper() in mine:
                save_plan(c, key, x['symbol'].upper(), x['plan'], model)
        c.commit()
        print(key, len(out['plans']), 'plans')


DIV_WITHHOLDING = 0.15  # the US treaty rate: used when the facts table does not settle a security's rate


def withholding_rate(text):
    """The dividend withholding rate a facts-table sentence states: its first percentage, 0 for "no withholding" or
    "free of ... withholding", else the US default. Unverified answers are read too: their stated rate beats a guess.
    ponytail: reads the sentence, not the law; a refund later claimed (Germany, Denmark) is not modelled."""
    t = (text or '').split(' (')[0]
    if m := re.search(r'(\d+(?:\.\d+)?)\s*%', t):
        return float(m[1]) / 100
    if re.search(r'\b(no|free of|exempt)\b[^.;]*withholding|nothing is withheld', t, re.I):
        return 0.0
    return DIV_WITHHOLDING


def credit_dividends(c, today=None):
    """Pay sandbox accounts the dividends their shares earned, so a sandbox is a total return like a LIVE account.
    Credits every ex-date since the last check for the shares held now, net of withholding, once each (the
    sandbox_divs key). Run daily, the shares held now are the shares held on the ex-date, except for a trade made
    between an ex-date and the next check. The first check goes back to the day before the account's first
    snapshot: its starting positions were copied from accounts that already held them."""
    today = today or date.today().isoformat()
    accts = c.execute('SELECT * FROM accounts WHERE mode="SANDBOX"').fetchall()
    held = {a['id']: c.execute('SELECT symbol, qty FROM positions WHERE account_id=?', (a['id'],)).fetchall() for a in accts}
    hist = dividends.histories(c, sorted({p['symbol'] for ps in held.values() for p in ps}))
    rate = {r['symbol']: withholding_rate(r['withholding']) for r in c.execute('SELECT symbol, withholding FROM facts')}
    n = 0
    for a in accts:
        k = f'divs_checked_{a["id"]}'
        since = (c.execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone() or [None])[0]
        if not since:
            first = c.execute('SELECT MIN(day) FROM snapshots WHERE account_id=?', (a['id'],)).fetchone()[0] or a['created'][:10]
            since = (date.fromisoformat(first) - timedelta(days=1)).isoformat()
        for p in held[a['id']]:
            for d, amt in (hist.get(p['symbol']) or {}).get('divs', []):
                if since < d <= today:
                    net = p['qty'] * amt * (1 - rate.get(p['symbol'], DIV_WITHHOLDING))
                    if c.execute('INSERT OR IGNORE INTO sandbox_divs VALUES (?,?,?,?,?,?)',
                                 (a['id'], p['symbol'], d, p['qty'], amt, net)).rowcount:
                        c.execute('UPDATE accounts SET cash=cash+? WHERE id=?', (net, a['id']))
                        n += 1
        c.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (k, today))
    c.commit()
    return n


def fail(c, key, ms, today, ex):
    c.execute('INSERT INTO reports (account_id, grp, run_date, body, at) VALUES (?,?,?,?,?)',
              (ms[0]['id'], key, today, f'**Analysis failed:** {ex}', now()))
    c.commit()
    import traceback
    traceback.print_exc()


def analyse_stages(c, today, groups, brief):
    stage = 0
    if brief:
        brief = brief['body']
    else:
        progress_write(stage=0, stage_started=time.time(), actions=0, last='Starting')
        stage = 1
        secs = feeds.sectors(c, SECTORS_URL)
        out, raw = claude(
            BRIEF_PROMPT.format(today=today, indexes=feeds.index_table(c, INDEXES),
                                macro=feeds.macro_table(c) or 'not available today: search for the latest CPI, jobs and Fed figures',
                                links=feeds.links_table(c) or 'not available today',
                                sectors='; '.join(f'{r["sector"]} week {r["week"]}, month {r["month"]}, quarter {r["quarter"]}, '
                                                  f'YTD {r["ytd"]}' for r in secs) or 'not available today'),
            ['WebSearch', 'WebFetch'],
            {'type': 'object', 'properties': {'brief': {'type': 'string', 'description': 'The whole brief in markdown, '
             'tables and all: the text the owners read. Not a note or summary about it.'}, 'policy_shock': POLICY_SHOCK},
             'required': ['brief', 'policy_shock']},
            'market brief', effort='medium')
        brief = full_brief(out['brief'], raw['_texts'])
        shock = out.get('policy_shock') or {}
        if shock.get('level') in ('none', 'minor', 'major'):
            c.execute('INSERT OR REPLACE INTO policy_days VALUES (?,?,?)', (today, shock['level'], shock.get('what') or ''))
        c.execute('INSERT INTO reports (account_id, run_date, body, at, usage_id) VALUES (NULL,?,?,?,?)',
                  (today, brief, now(), raw['_usage_id']))
        c.commit()
    # Research: every live account is refreshed first, so the notes cover what is really held.
    progress_write(stage=stage, stage_started=time.time(), actions=0, last='Loading the accounts from IBKR')
    stage += 1
    lives = [a for ms in groups.values() for a in ms.values() if a['mode'] == 'LIVE']
    errors = refresh_live(c, lives, perf=True) if lives else {}
    failed = {key: errors[a['id']] for key, ms in groups.items() for a in ms.values() if a['id'] in errors}
    held = {}
    for key, ms in groups.items():
        for a in ms.values():
            a = c.execute('SELECT * FROM accounts WHERE id=?', (a['id'],)).fetchone()
            for p in (state(c, a) or {}).get('positions', []):
                held[p['symbol']] = held.get(p['symbol'], 0) + (p['value'] or 0)
    pack = research(c, today, brief, held)
    for key, ms in groups.items():
        ms = list(ms.values())
        progress_write(stage=stage, stage_started=time.time(), actions=0, last='Loading the accounts')
        stage += 1
        try:
            if key in failed:
                raise failed[key]
            analyse_group(c, key, ms, brief, pack, today)
        except Exception as ex:
            fail(c, key, ms, today, ex)


def check_proposals(props, states, ctx=None, today=None, shock=None):
    """The app's own checks, whatever Claude says. states: {account name: state}; ctx: {symbol: {'size_usd': suggested
    position size, 'results': next results date}} from risk_context.
    Yields (proposal, 'pending' | 'invalid', note). Invalid: unknown account, bad symbol, selling more
    shares than the account holds (all of the batch's sales together), a limit without a price, buys the account cannot
    pay for (cash never moves between accounts, so each account's buys must fit its own USD cash plus its own sales in
    the same batch; a LIVE account's AUD cash would need converting first), or a buy that takes one stock above
    MAX_WEIGHT of the combined total. Warnings, which leave a proposal pending: one stock held in two accounts (Alex
    prefers against it, 'if possible'), a buy much bigger than its risk-based size, a buy just before results, a market
    order on a day the brief rated a major policy shock. Also invalid: a buy without a positive price, and a buy of a
    stock in Weinstein stage 4 (ctx 'stage', from the investor lenses), which the rules forbid."""
    ctx, today = ctx or {}, today or date.today().isoformat()
    usd_cash = lambda s: s['cash_usd_only'] if s.get('cash_usd_only') is not None else s['cash']
    budget = {n: usd_cash(s) for n, s in states.items()}
    held = {n: {x['symbol']: x['qty'] for x in s['positions']} for n, s in states.items()}
    left = {n: dict(h) for n, h in held.items()}
    px = lambda p: p.get('limit_price') or p['ref_price']
    ok = []
    for p in props:
        p['symbol'] = p['symbol'].upper().strip()
        n, sym = p.get('account'), p['symbol']
        why = ('unknown account' if n not in states else
               'malformed symbol' if not re.fullmatch(r'[A-Z][A-Z0-9. ]{0,9}', sym) else
               f'{n} holds {held[n].get(sym, 0):g} {sym}' + (' (less what this batch already sells)' if left[n].get(sym, 0) != held[n].get(sym, 0) else '')
               if p['side'] == 'SELL' and p['quantity'] > left[n].get(sym, 0) + 1e-9 else
               'limit order without a price' if p['order_type'] == 'LIMIT' and not (p.get('limit_price') or 0) > 0 else
               'no price to judge the order by' if not (px(p) or 0) > 0 else
               f'{sym} is in Weinstein stage 4 (below a falling 30-week average): the rules forbid buying it'
               if p['side'] == 'BUY' and (ctx.get(sym) or {}).get('stage') == 4 and sym not in dividends.CASH_LIKE else None)
        ok.append([p, why])
        if not why and p['side'] == 'SELL':
            left[n][sym] -= p['quantity']
            budget[n] += p['quantity'] * px(p) - ibkr_fee(p['quantity'], px(p))
    # Each stock's value across the household after the batch, for the weight limit.
    total = sum(s['value'] for s in states.values())
    worth = {}
    for s in states.values():
        for x in s['positions']:
            worth[x['symbol']] = worth.get(x['symbol'], 0) + (x.get('value') or 0)
    for p, why in ok:
        if not why and p['side'] == 'SELL':
            worth[p['symbol']] = worth.get(p['symbol'], 0) - p['quantity'] * px(p)
    buys_by_sym = {}
    for row in ok:
        p, why = row
        if why or p['side'] != 'BUY':
            continue
        n = p['account']
        cost = p['quantity'] * px(p) + ibkr_fee(p['quantity'], px(p))
        after = worth.get(p['symbol'], 0) + cost
        if cost > budget[n] * 1.005:  # 0.5% slack for a market order's price moving
            row[1] = (f"{n}'s account cannot pay: needs {usd(cost)}, has {usd(budget[n])} in USD "
                      '(cash cannot move between accounts)')
        elif total and after > MAX_WEIGHT * total:
            row[1] = f'{p["symbol"]} would be {after / total:.0%} of the combined total (the limit is {MAX_WEIGHT:.0%})'
        else:
            budget[n] -= cost
            worth[p['symbol']] = after
            buys_by_sym.setdefault(p['symbol'], set()).add(n)
    for p, why in ok:
        if why:
            yield p, 'invalid', why
            continue
        notes = []
        if p['side'] == 'BUY':
            others = sorted(m for m in states if m != p['account'] and
                            (held[m].get(p['symbol']) or m in buys_by_sym.get(p['symbol'], ())))
            if others:
                notes.append(f'Warning: {p["symbol"]} would then be held in two accounts (also {", ".join(others)}).')
            c_ = ctx.get(p['symbol']) or {}
            size = c_.get('size_usd')
            if size and p['quantity'] * px(p) > 1.5 * size:
                notes.append(f'Warning: {usd(p["quantity"] * px(p))} is more than 1.5 times the risk-based size '
                             f'({usd(size)}) for a stock this volatile.')
            r = c_.get('results')
            if r and today <= r and (date.fromisoformat(r) - date.fromisoformat(today)).days <= 7:
                notes.append(f'Warning: results are due {r}, within a week.')
        if shock == 'major' and p['order_type'] == 'MARKET':
            notes.append('Warning: a market order on a day of a major policy shock; a limit order is safer.')
        yield p, 'pending', ' '.join(notes) or None

OPTIONS_PROMPT = """For each stock below (symbol: IBKR contract id), call get_price_snapshot ONCE with contract_id set to that
id and market_data_names ["implied_vol_underlying", "implied_volatility_percentile", "historical_vol",
"underlying_today_option_volume", "underlying_avg_option_volume"]. Copy the numbers exactly as returned: annual_iv from
implied-vol-underlying, high_52w from implied-volatility-percentile, annual_pct from historical-vol, today's callVolume and
putVolume, and avgCallVolume and avgPutVolume. Use null for anything missing. Call no other tool.
{stocks}"""
OPTIONS = {'type': 'object', 'required': ['rows'], 'properties': {'rows': {'type': 'array', 'items': {
    'type': 'object', 'required': ['symbol'], 'properties': {
        'symbol': {'type': 'string'}, **{k: {'type': ['number', 'null']} for k in (
            'annual_iv', 'iv_percentile_52w', 'hv_30d', 'calls_today', 'puts_today', 'avg_calls', 'avg_puts')}}}}}}


def options_text(r, price=None):
    """The option market's view of one stock, in words: the swing it prices in, and how nervous it is."""
    iv = r.get('annual_iv')
    if not iv:
        return None
    wk, mo = iv * (5 / 252) ** .5, iv * (21 / 252) ** .5
    out = (f'implied volatility {iv:.0%} a year, so the options price a typical move of about ±{wk:.1%} in a week and '
           f'±{mo:.1%} in a month' + (f' (±${price * mo:,.2f})' if price else ''))
    if r.get('iv_percentile_52w') is not None:
        out += f'; higher than on {r["iv_percentile_52w"]:.0%} of the past year\'s days'
    if r.get('hv_30d'):
        out += f'; the last 30 days actually moved {r["hv_30d"]:.0%} a year'
    if r.get('calls_today') is not None and r.get('puts_today') is not None and r['calls_today']:
        pc = r['puts_today'] / r['calls_today']
        avg = r['avg_puts'] / r['avg_calls'] if r.get('avg_calls') and r.get('avg_puts') is not None else None
        out += f'; put/call volume today {pc:.2f}' + (f' (usually {avg:.2f})' if avg is not None else '')
    return out


def option_stats(c, symbols, today):
    """{symbol: options text}: one short Haiku run that asks IBKR for each stock's implied volatility. Cached for the
    day; a failure leaves the notes without it rather than stopping the analysis."""
    have = c.execute('SELECT body FROM pages WHERE key="ib:options" AND day=?', (today,)).fetchone()
    rows = json.loads(have[0]) if have else {}
    ids = {s: i for s, i in known_conids(c, symbols).items() if s not in rows}
    if ids:
        try:
            out, _ = claude(OPTIONS_PROMPT.format(stocks='\n'.join(f'{s}: {i}' for s, i in sorted(ids.items()))),
                            [CONNECTORS['alex'] + 'get_price_snapshot'], OPTIONS, 'options', timeout=600, model='haiku')
            rows.update({r['symbol']: r for r in out['rows'] if r.get('symbol') in ids})
            c.execute('INSERT OR REPLACE INTO pages VALUES ("ib:options",?,?)', (today, json.dumps(rows)))
            c.commit()
        except Exception as ex:
            print('options: skipped:', ex, file=sys.stderr)
    return {s: t for s in symbols if (r := rows.get(s)) and (t := options_text(r, (price_stats([s]).get(s) or {}).get('last')))}


def todays_research(c, today):
    r = c.execute("SELECT body FROM reports WHERE grp='research' AND run_date=? ORDER BY id DESC LIMIT 1", (today,)).fetchone()
    return json.loads(r['body']) if r else None


def num(x):
    try:
        return float(str(x).rstrip('%').replace(',', ''))
    except ValueError:
        return 0.0


FACTS_PROMPT = '''For each security below, find its country of incorporation ("domicile") and the dividend withholding
tax an Australian tax resident pays when holding it through Interactive Brokers, with a W-8BEN lodged where one applies.
Background: US companies and US-domiciled ETFs withhold 15% under the treaty with a W-8BEN. ADRs and foreign companies
listed in the US follow their home country's rules and its treaty with Australia (for example Chubb is Swiss and pays
from capital contribution reserves free of Swiss withholding); the ADR depositary may also charge a fee. REITs, MLPs and
T-bill or bond ETFs have special rules (US interest-related dividends can be exempt for non-residents): say so.
Finviz's country is the headquarters, which is not always the country of incorporation.
Run WebSearch for EVERY security (one or two searches each; prefer the issuer, a tax authority or the depositary):
an answer from background knowledge alone does not count and must have "verified" false. "source" names the page
and its date. Set "verified" false also when the sources do not settle it. Keep "withholding" to one short sentence.
Securities (symbol: company, Finviz country): {items}'''
FACTS = {'type': 'object', 'required': ['facts'], 'properties': {'facts': {'type': 'array', 'items': {
    'type': 'object', 'required': ['symbol', 'domicile', 'withholding', 'verified', 'source'], 'properties': {
        'symbol': {'type': 'string'}, 'domicile': {'type': 'string'}, 'withholding': {'type': 'string'},
        'verified': {'type': 'boolean'}, 'source': {'type': 'string'}}}}}}
FACTS_RETRY_DAYS = 30  # an answer that could not be verified is tried again after this long


def lookup_facts(c, symbols, pages, today):
    """Look up domicile and dividend withholding once for securities the facts table lacks, in one short run
    of its own: left to the research run, Claude kept writing "unverified" rather than searching. Verified answers
    are kept for good; unverified ones are stored too, so they are retried monthly rather than daily."""
    retry = (date.fromisoformat(today) - timedelta(days=FACTS_RETRY_DAYS)).isoformat()
    have = {r['symbol'] for r in c.execute('SELECT symbol, withholding, at FROM facts')
            if 'unverified' not in r['withholding'].lower() or r['at'] > retry}
    todo = [x for x in dict.fromkeys(symbols) if x not in have]
    if not todo:
        return
    items = '; '.join(f'{x}: {(pages.get(x) or {}).get("company") or ""} {(pages.get(x) or {}).get("country") or "?"}'.replace('  ', ' ')
                      for x in todo)
    try:
        out, _ = claude(FACTS_PROMPT.format(items=items), ['WebSearch', 'WebFetch'], FACTS, 'facts', effort='medium')
    except RuntimeError:
        return  # research then marks them unverified, as before
    c.executemany('INSERT OR REPLACE INTO facts VALUES (?,?,?,?)', [
        (x['symbol'].upper(), x['domicile'],
         (x['withholding'] if x['verified'] else f'unverified: {x["withholding"]}') + (f' ({x["source"]})' if x['source'] else ''),
         today) for x in out['facts'] if x['symbol'].upper() in todo])
    c.commit()


def lens_notes(c, symbols, pages):
    """{symbol: {'lenses', 'patterns'}}: the eight investor lenses and any candlestick or chart pattern on the last whole
    bar, by script. Every one is also recorded, so the Saturday scoring can tell which of them work."""
    cl = feeds.closes(c, list(symbols) + ['SPY'])
    cut = feeds.bar_cut()
    whole = lambda sym: [r for r in cl.get(sym, []) if r[0] < cut]
    spy = [x for _, x in whole('SPY')]
    bars = feeds.bars(c, symbols)
    status = learn.statuses(c)
    out = {}
    for sym in symbols:
        rows = whole(sym)
        if len(rows) < 2:
            continue
        ls = lenses.lenses(pages.get(sym), [x for _, x in rows], spy)
        b = bars.get(sym, [])
        pats = (lenses.candles(b) if b and b[-1][0] == rows[-1][0] else []) + lenses.chart_patterns([x for _, x in rows])
        learn.record_signals(c, rows[-1][0], sym, ls, pats, {k: v[1] for k, v in lenses.PATTERNS.items()})
        stage = re.match(r'stage (\d)', ls.get('weinstein', ('', ''))[1])
        out[sym] = {'lenses': lenses.lens_text(ls), 'stage': int(stage[1]) if stage else None,
                    'patterns': '; '.join(f'{lenses.PATTERNS[k][0]} on {rows[-1][0]} ({lenses.PATTERNS[k][1]}, '
                                          f'{status.get(k, "on trial")})' for k in pats)}
    c.commit()
    return out


def research(c, today, brief, held):
    """The day's shared research notes: one Claude run for every stock held in any account, plus sectors,
    candidates and parking options. The app gathers the data first (Finviz pages, Yahoo closes, the stock screen,
    upgrades) so Claude summarises rather than searches. Reused all day; a later run with stocks not yet covered
    researches just those and adds them."""
    pack = todays_research(c, today)
    missing = sorted(set(held) - {x['symbol'] for x in (pack or {}).get('stocks', [])})
    if pack and not missing:
        return pack
    symbols = missing if pack else sorted(held)
    pages, stats, fh = feeds.quote_pages(c, symbols), price_stats(symbols), feeds.finnhub(c, symbols)
    filings = feeds.edgar(c, symbols)
    info = lambda syms, extra={}: json.dumps({x: {**(feeds.compact(pages.get(x)) or {'finviz': 'not available'}),
                                                  'trend': stats.get(x), **fh.get(x, {}),
                                                  **({'sec_filings_30d': filings[x]} if x in filings else {}), **extra.get(x, {})}
                                              for x in syms})
    prev = next((json.loads(r['body']) for r in c.execute(
        "SELECT body FROM reports WHERE grp='research' ORDER BY id DESC LIMIT 10")
        if json.loads(r['body']).get('parking_at')), None)
    fresh = prev and (date.fromisoformat(today) - date.fromisoformat(prev['parking_at'])).days < PARK_DAYS
    pool = []
    if pack:
        more = RESEARCH_TOP_UP
    else:
        secs = feeds.sectors(c, SECTORS_URL)
        wsec = {}
        for sym, v in held.items():
            k = (pages.get(sym) or {}).get('sector') or 'unknown'
            wsec[k] = wsec.get(k, 0) + v
        total = sum(held.values()) or 1
        ranked = [r['sector'] for r in sorted(secs, key=lambda r: -num(r['quarter']))]
        strong = ranked[:3]
        lack = [x for x in ranked if x not in wsec and x not in strong][:2]
        extra = {}
        for sec in strong + lack:
            for r in [r for r in feeds.screen(c, SCREEN_URL, sec) if r['symbol'] not in held and r['sector'] == sec][:4]:
                pool.append(r['symbol'])
                extra[r['symbol']] = {'company': r['company'], 'market_cap': r['market_cap']}
        pages.update(feeds.quote_pages(c, pool + ([] if fresh else PARK_ETFS)))
        stats.update(price_stats(pool))
        fh.update(feeds.finnhub(c, pool))
        filings.update(feeds.edgar(c, pool))
        more = RESEARCH_MORE.format(
            sectors='; '.join(f'{r["sector"]} week {r["week"]}, month {r["month"]}, quarter {r["quarter"]}, YTD {r["ytd"]}'
                              for r in secs) or 'not available: say so in "gaps"',
            weights=', '.join(f'{k} {v / total:.0%}' for k, v in sorted(wsec.items(), key=lambda kv: -kv[1])),
            strong=', '.join(strong) or 'none', missing=', '.join(lack) or 'none',
            pool=info(pool, extra) if pool else 'empty today (the screen could not be read): pick from the upgrades or say so in "gaps"',
            upgrades='; '.join(feeds.upgrades(c, UPGRADES_URL)[:25]) or 'not available',
            parking=PARKING_CACHED.format(at=prev['parking_at']) if fresh else PARKING_PROMPT.format(etfs=info(PARK_ETFS)))
    everything = symbols + pool + ([] if pack or fresh else PARK_ETFS)
    lookup_facts(c, everything, pages, today)
    q = ','.join('?' * len(everything))
    facts = {r['symbol']: {'domicile': r['domicile'], 'withholding': r['withholding']}
             for r in c.execute(f'SELECT * FROM facts WHERE symbol IN ({q})', everything)}
    schema = json.loads(json.dumps(RESEARCH))
    if pack or fresh:
        schema['required'].remove('parking')
        del schema['properties']['parking']
    out, raw = claude(
        RESEARCH_PROMPT.format(today=today, brief=brief, facts=json.dumps(facts) if facts else 'none yet',
                               conids=json.dumps(dict(c.execute('SELECT symbol, conid FROM conids').fetchall())),
                               n=len(symbols), held=info(symbols), more=more),
        ['WebSearch', 'WebFetch'] + [p + 'search_contracts' for p in CONNECTORS.values()],
        schema, 'research', timeout=3000, effort='medium')
    remember_conids(c, [(x['symbol'], x['conid']) for x in out['stocks'] + out.get('candidates', [])])
    try:
        looks = lens_notes(c, symbols + pool, pages)
    except Exception:  # extra evidence: its failure must not lose the day's research
        import traceback
        traceback.print_exc()
        looks = {}
    opts = option_stats(c, [x['symbol'] for x in out['stocks'] + out.get('candidates', [])], today)
    for x in out['stocks'] + out.get('candidates', []):
        x['valuation'] = feeds.valuation(pages.get(x['symbol']))
        x['fundamentals'] = feeds.fundamentals(pages.get(x['symbol']))
        x['trend'] = trend_text(stats.get(x['symbol']))
        x.update(fh.get(x['symbol'], {}))  # earnings, analyst_trend, insiders: the app's numbers, not retyped
        if x['symbol'] in filings:
            x['filings'] = filings[x['symbol']]
        if x['symbol'] in opts:
            x['options'] = opts[x['symbol']]
        x.update({k: v for k, v in looks.get(x['symbol'], {}).items() if v})  # 'stage' feeds check_proposals
    for x in out.get('parking', []):
        x['symbol'] = (x.get('symbol') or '').upper()
    c.executemany('INSERT OR IGNORE INTO facts VALUES (?,?,?,?)',
                  [(x['symbol'], x['domicile'], x['withholding'], today) for x in out['stocks'] + out.get('candidates', [])
                   if 'unverified' not in (x['domicile'] + x['withholding']).lower()])
    if pack:
        pack['stocks'] += out['stocks']
    else:
        pack = out
        if fresh:
            pack['parking'], pack['parking_at'] = prev['parking'], prev['parking_at']
        else:
            pack['parking_at'] = today
    remember_conids(c, [(x['symbol'], x['conid']) for x in out['stocks'] + out.get('candidates', [])] +
                    [(x.get('symbol'), x.get('conid')) for x in out.get('parking', [])])
    c.execute('INSERT INTO reports (account_id, grp, run_date, body, at, usage_id) VALUES (NULL,?,?,?,?,?)',
              ('research', today, json.dumps(pack), now(), raw['_usage_id']))
    c.commit()
    return pack


def tax_context(c, states, today):
    """Per LIVE owner: this financial year's realised gains (by currency) and the open USD parcels now at a loss, with
    today's price, for tax-loss selling. states: {name: (account, state)}."""
    out = {}
    for a, s in states.values():
        if a['mode'] != 'LIVE' or not a['owner'] or not s:
            continue
        rs = tax.rows(c, a['owner'])
        fy_now = tax.fy(today)
        summ = tax.summary(rs)
        nets = tax.net(summ)
        done = {cur: {**{k: round(v, 2) for k, v in t.items() if k != 'n'},
                      **{k: round(v, 2) for k, v in nets[(cur, y)].items()}} for (cur, y), t in summ.items() if y == fy_now}
        if not done:  # nothing sold yet this year: losses carried in from earlier years still offset this year's gains
            last = [v for (cur, y), v in nets.items() if cur == 'USD']
            if last and last[-1]['losses_carried_out']:
                done = {'USD': {'losses_carried_in': round(last[-1]['losses_carried_out'], 2)}}
        price = {p['symbol']: p['last'] for p in s['positions']}
        losers = []
        for r in rs:
            if r['sale_date'] or r['currency'] != 'USD' or price.get(r['symbol']) is None:
                continue
            gain = r['buy_qty'] * price[r['symbol']] - r['cost']
            if gain < -200:
                losers.append(f'{r["symbol"]} bought {r["buy_date"]}: {r["buy_qty"]:g} shares, {usd(gain)}')
        out[a['owner'].title()] = {'financial_year': fy_now, 'realised_so_far': done or 'nothing sold yet',
                                   'parcels_at_a_loss': losers or 'none'}
    return json.dumps(out) if out else 'not applicable (no LIVE accounts here)'


MODELS = {'opus': 'Opus 5.5', 'sonnet': 'Sonnet 5'}  # CLI alias: name shown


def analysis_model(c):
    """The model for the household analysis, chosen on the home page (the brief and research stay on Sonnet)."""
    m = (c.execute('SELECT v FROM meta WHERE k="analysis_model"').fetchone() or ['sonnet'])[0]
    return m if m in MODELS else 'sonnet'


def risk_context(c, states, candidates, pack):
    """(figures for the prompt, {symbol: {'size_usd', 'results'}} for check_proposals) from 60 days of daily returns:
    portfolio beta, holdings that move together, and per stock beta, volatility and a risk-based position size. states:
    {name: (account, state)}."""
    pos = {}
    for _, s in states.values():
        for x in s['positions']:
            pos[x['symbol']] = pos.get(x['symbol'], 0) + (x['value'] or 0)
    total = sum(s['value'] for _, s in states.values()) or 1
    cl = feeds.closes(c, list(pos) + list(candidates) + ['SPY'])
    days = [d for d, _ in cl.get('SPY', [])][-61:]
    def rets(sym):
        px = dict(cl.get(sym, []))
        return {b: px[b] / px[a] - 1 for a, b in zip(days, days[1:]) if px.get(a) and px.get(b)}
    R = {sym: r for sym in set(pos) | set(candidates) | {'SPY'} if len(r := rets(sym)) >= 40}
    def corr(a, b):
        ks = sorted(set(a) & set(b))
        try:
            return statistics.correlation([a[k] for k in ks], [b[k] for k in ks]) if len(ks) >= 40 else None
        except statistics.StatisticsError:
            return None
    def beta(r):
        ks = sorted(set(r) & set(R.get('SPY', {})))
        if len(ks) < 40:
            return None
        m = [R['SPY'][k] for k in ks]
        return statistics.covariance([r[k] for k in ks], m) / statistics.variance(m)
    vol = lambda r: statistics.pstdev(list(r.values())) * 252 ** .5
    inv = {sym: v for sym, v in pos.items() if sym in R and v > 0}
    w = sum(inv.values()) or 1
    port = {k: sum(v / w * R[sym].get(k, 0) for sym, v in inv.items()) for k in days[1:]} if inv else {}
    typical = statistics.median(inv.values()) if inv else total / 10
    # An all-cash portfolio (a new sandbox) has no typical holding: a tenth of it, at a typical candidate's volatility
    # (not the index's: one stock moves far more than the S&P 500).
    cv = [vol(R[s_]) for s_ in candidates if s_ in R]
    avg_vol = sum(v * vol(R[sym]) for sym, v in inv.items()) / w if inv else statistics.median(cv) if cv else None
    results = {}
    for x in pack.get('stocks', []) + pack.get('candidates', []):
        if m := re.search(r'next results (\d{4}-\d\d-\d\d)', x.get('earnings') or ''):
            results[x['symbol']] = m[1]
    stocks, ctx = {}, {}
    for sym in sorted(set(pos) | set(candidates)):
        if sym not in R:
            continue
        b, v = beta(R[sym]), vol(R[sym])
        size = round(min(typical * avg_vol / v, MAX_WEIGHT * total), -2) if avg_vol and v else None  # T-bill funds barely move
        stocks[sym] = {'beta': round(b, 2) if b is not None else None, 'volatility_pct': round(100 * v), 'size_usd': size}
        if sym not in pos and port and (k := corr(R[sym], port)) is not None:
            stocks[sym]['corr_with_portfolio'] = round(k, 2)
        ctx[sym] = {'size_usd': size, 'results': results.get(sym)}
    for sym, d in results.items():
        ctx.setdefault(sym, {'size_usd': None, 'results': d})
    held = sorted(inv)
    pairs = sorted(((k, a, b) for i_, a in enumerate(held) for b in held[i_ + 1:] if (k := corr(R[a], R[b])) is not None and k >= 0.75),
                   reverse=True)
    pb = sum(v * (stocks.get(sym, {}).get('beta') or 0) for sym, v in inv.items()) / total
    out = {'portfolio_beta': round(pb, 2), 'typical_holding_usd': round(typical, -2),
           'typical_volatility_pct': round(100 * avg_vol) if avg_vol else None,
           'pairs_moving_together': [f'{a} and {b}: {k:.2f}' for k, a, b in pairs[:10]] or 'none', 'stocks': stocks}
    return json.dumps(out), ctx


def analyse_group(c, key, ms, brief, pack, today, deep=False):
    """One Claude run for a household: every member's state and the shared research in, proposals out per account."""
    states = {}
    for a in ms:
        a = c.execute('SELECT * FROM accounts WHERE id=?', (a['id'],)).fetchone()  # refreshed by the research stage
        states[a['name']] = (a, state(c, a))
    snapshot(c, states.values(), today)
    ids = [a['id'] for a in ms]
    q = ','.join('?' * len(ids))
    history = [dict(r) for r in c.execute(
        f'SELECT p.run_date, a.name AS account, p.side, p.qty, p.symbol, p.status, p.reject_reason, p.note FROM proposals p '
        f'JOIN accounts a ON a.id=p.account_id WHERE p.account_id IN ({q}) '
        f'AND p.status NOT IN ("pending","expired") ORDER BY p.id DESC LIMIT 20', ids)]
    total = sum(s['value'] for _, s in states.values())
    cash = sum(s['cash'] for _, s in states.values())
    schema = json.loads(json.dumps(PROPOSALS))
    item = schema['properties']['proposals']['items']
    item['properties']['account'] = {'enum': list(states)}
    item['required'].append('account')
    mine = sorted({p['symbol'] for _, s in states.values() for p in s['positions']})
    notes = {'stocks': [x for x in pack['stocks'] if x['symbol'] in mine],
             **{k: pack[k] for k in ('sectors', 'candidates', 'parking', 'gaps')}}
    try:
        risk, rctx = risk_context(c, states, [x['symbol'] for x in pack['candidates']], pack)
    except Exception as ex:  # extra evidence: its failure must not stop the analysis
        risk, rctx = f'not available today ({ex})', {}
    second = ''
    if deep:
        prev = c.execute(f'SELECT body FROM reports WHERE grp=? AND run_date=? AND body LIKE "{{%" ORDER BY id DESC LIMIT 1',
                         (key, today)).fetchone()
        props = [dict(r) for r in c.execute(
            f'SELECT a.name AS account, p.side, p.qty, p.symbol, p.order_type, p.limit_price, p.reason, p.status '
            f'FROM proposals p JOIN accounts a ON a.id=p.account_id WHERE p.account_id IN ({q}) AND p.run_date=?',
            ids + [today])]
        second = DEEP_PROMPT.format(portfolio=json.loads(prev['body']).get('portfolio', '') if prev else 'none',
                                    proposals=json.dumps(props) if props else 'none')
    out, raw = claude(
        ACCOUNT_PROMPT.format(mode=ms[0]['mode'], n=len(ms), names=', '.join(f'"{n}"' for n in states),
                              today=today, total=total, cash=cash, research=json.dumps(notes),
                              stats=json.dumps(price_stats(mine)), risk=risk,
                              conids=json.dumps(known_conids(c, mine + [x['symbol'] for x in pack['candidates']] +
                                                             [x.get('symbol') for x in pack['parking'] if x.get('symbol')])),
                              state=json.dumps({n: s for n, (_, s) in states.items()}, default=str),
                              cash_rule=(CASH_RULE if any(s['cash'] > PARK_ABOVE_USD for _, s in states.values())
                                         else NO_CASH_RULE).format(per_account=' '.join(
                                  f'"{n}" has ${s["cash"]:,.0f}, {"above" if s["cash"] > PARK_ABOVE_USD else "not above"} '
                                  f'the ${PARK_ABOVE_USD:,} threshold.' for n, (_, s) in states.items())),
                              brief=brief, history=json.dumps(history) if history else 'none yet',
                              plans=json.dumps(plans_context(c, key, states, today)[0]) or 'no holdings',
                              lessons=lessons_text(c), tax=tax_context(c, states, today)) + RULES + second,
        ['WebSearch', 'WebFetch'] + ALL_READ, schema, 'deep analysis' if deep else 'analysis', ms[0]['id'],
        timeout=2700, model='opus' if deep else (m := analysis_model(c)), effort='high' if deep or m == 'opus' else 'medium')
    # Old proposals were priced for a market that has moved, so the new run replaces them. Only
    # now, not when the run starts: otherwise the page shows nothing to decide for the whole run,
    # and a failed run would leave the accounts with no proposals at all.
    c.execute(f'UPDATE proposals SET status="expired", note=? WHERE status="pending" AND account_id IN ({q})',
              [f'superseded by the {today} run'] + ids)
    for x in pack.get('stocks', []) + pack.get('candidates', []):
        if x.get('stage'):
            rctx.setdefault(x['symbol'], {'size_usd': None, 'results': None})['stage'] = x['stage']
    shock = (c.execute('SELECT level FROM policy_days WHERE day=?', (today,)).fetchone() or [None])[0]
    for p, status, note in check_proposals(out['proposals'], {n: s for n, (_, s) in states.items()}, rctx, today, shock):
        a = states[p['account']][0] if p.get('account') in states else ms[0]
        c.execute('INSERT INTO proposals (account_id, run_date, symbol, conid, side, qty, order_type, '
                  'limit_price, ref_price, reason, confidence, status, note, proposed_at, plan) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  (a['id'], today, p['symbol'], p['conid'], p['side'], p['quantity'], p['order_type'],
                   p.get('limit_price'), p['ref_price'], p['reason'], p['confidence'], status, note, now(),
                   json.dumps(p['plan']) if p.get('plan') else None))
    # Plans the run wrote for holdings: new ones, and changes it gave a reason for.
    have = {r[0] for r in c.execute('SELECT symbol FROM plans WHERE grp=?', (key,))}
    for x in out['analysis']['holdings']:
        if x.get('plan') and (x['symbol'].upper() not in have or x['plan'].get('changed')):
            save_plan(c, key, x['symbol'], x['plan'], 'holding')
    # A plan outlives its stock only until the household no longer holds it (LIVE sales happen in the IBKR app, so
    # nothing else notices), so a later buy starts a fresh plan and a fresh holding period.
    c.execute(f'DELETE FROM plans WHERE grp=? AND symbol NOT IN ({",".join("?" * len(mine))})', [key] + mine)
    c.execute('INSERT INTO reports (account_id, grp, run_date, body, at, usage_id) VALUES (?,?,?,?,?,?)',
              (ms[0]['id'], key, today, json.dumps(out['analysis']), now(), raw['_usage_id']))
    c.commit()


# ---------------------------------------------------------------- learning loop
LESSON_KINDS = {'avoid': 'Avoid', 'favour': 'Lean towards', 'preference': 'Your preferences', 'timing': 'Timing'}
MAX_ACTIVE_LESSONS = 10
REJECT_REASONS = ['Too risky', 'Disagree with the reasoning', 'Tax timing', 'Wrong account', 'Other']


def lessons_text(c):
    rows = c.execute('SELECT kind, text FROM lessons WHERE status="active" ORDER BY kind, id').fetchall()
    return '\n'.join(f'- {LESSON_KINDS.get(r["kind"], r["kind"])}: {r["text"]}' for r in rows) or 'none yet'


REVIEW_PROMPT = '''You review how a stock advisor's proposals turned out, to improve its future proposals. The advisor
(Claude: Sonnet, or Opus when chosen on the home page, plus an occasional Opus deep review) proposes trades each weekday for a family's US stock portfolios; the owners approve or reject each one.
Today is {today}.

Scorecard, computed by the app from Yahoo closes. "excess" is the stock's return minus the S&P 500's (SPY) over 5, 20
and 60 trading days after the proposal, reversed for a SELL (a sale was right if the stock then lagged). Decision
classes: taken (approved), rejected (the owner said no, with a reason when one was picked), ignored (left until a
later run replaced it), hands-off (LIVE proposals the owners deliberately left alone while that household served as
the benchmark: they are NOT evidence of the owners' preferences, only of the advice itself).
Summary by class, confidence, side and horizon: {summary}
Every scored proposal (reason and exit plan as the advisor wrote them; judge the plans too: stops set too tight,
targets too low, holding periods that ignored the capital gains discount): {rows}
Ideas reversed within about 20 trading days: {churn}
Every verdict on the analysis cards (holdings and opportunities, not only trades), scored the same way: n, average
stock-minus-SPY return and the share that beat SPY, per verdict and horizon. BUY and ADD should beat SPY, SELL and TRIM
should lag it, HOLD should not lag it badly: {verdicts}
Exit plans of the BUYs that were taken: what each met first since, its target or its review level (stop), or neither
yet, and after how many days: {plan_outcomes}
Rejection reasons the owners picked: {reasons}
Friends' profiles, each an advised sandbox against their own fixed portfolio (no LIVE; their proposals are in the rows
above, under their own household keys): {friends}
The advisor's own household portfolio (sandbox: every proposal it makes is approved there) against the owners' fixed
buy-and-hold copy, their LIVE accounts and SPY: {bench}. Since live_follows_advice_since, the owners have also been
acting on the advice in LIVE, choosing which proposals to take: LIVE against the advisor shows what their choices
added or cost, and LIVE against fixed shows whether following the advice paid. "live_measure" says how LIVE is
measured: by account value it also moves with any deposit or withdrawal, so treat a sudden jump there as money.
Investor lenses (eight investors' checks the app runs weekly on every stock it researches): per lens and result (pass,
mixed, fail), n, average stock-minus-SPY return and the share that beat SPY, per horizon. A lens is useful when its
passes beat its fails: {lenses}
Candlestick and chart patterns on trial (seen on a stock's last bar, scored the same way, turned round for bearish
ones so positive means right), with each one's status (proven, on trial, failed; the app sets it, not you): {patterns}
Deep reviews by Opus in this period (where a second opinion disagreed with the first): {deep}
Lessons already in force (id: text): {active}

Write:
- "critique": markdown, at most 400 words, for the owners: what worked, what did not, and how sure you can be given
  how few proposals have matured. Say plainly when the data is too thin to conclude anything.
- "lessons": at most 8 NEW lessons for the advisor's prompt. Each is one sentence the advisor can act on, of kind
  "avoid" (a mistake to stop making), "favour" (an approach that worked), "preference" (what the owners keep choosing
  or rejecting) or "timing" (holding period, entry or exit timing). A lesson needs n >= 5 supporting proposals, or a
  rejection reason that repeats; say which in "evidence" and count them in "n". Never base one on 5-day moves alone.
  Lessons are about process or preference, never "buy X" or "avoid ticker X". Do not repeat a lesson already in
  force. Zero lessons is the right answer when the evidence is thin.
- "retire": ids of lessons in force that the evidence now contradicts.'''
REVIEW = {'type': 'object', 'required': ['critique', 'lessons', 'retire'], 'properties': {
    'critique': {'type': 'string'},
    'lessons': {'type': 'array', 'items': {'type': 'object', 'required': ['kind', 'text', 'evidence', 'n'], 'properties': {
        'kind': {'enum': list(LESSON_KINDS)}, 'text': {'type': 'string'}, 'evidence': {'type': 'string'},
        'n': {'type': 'integer'}}}},
    'retire': {'type': 'array', 'items': {'type': 'integer'}}}}


def bench_series(c):
    """(days, {group: [value per day]}, [SPY per day]) for the advisor, the frozen copy and LIVE, on the days every
    account in all three has a snapshot."""
    accts = c.execute(f'SELECT id, grp FROM accounts WHERE grp IN ({",".join("?" * len(BENCH))})', [g for g, *_ in BENCH]).fetchall()
    grp_of = {a['id']: a['grp'] for a in accts}
    by_day = {}
    for r in c.execute(f'SELECT * FROM snapshots WHERE account_id IN ({",".join("?" * len(grp_of))})', list(grp_of)):
        by_day.setdefault(r['day'], {})[r['account_id']] = (r['value_usd'], r['spy'])
    days = sorted(d for d, v in by_day.items() if set(v) == set(grp_of))
    vals = {g: [sum(v for i, (v, _) in by_day[d].items() if grp_of[i] == g) for d in days] for g, *_ in BENCH}
    twr = live_twr(c, days, {i: {d: by_day[d][i][0] for d in days} for i, g in grp_of.items() if g == 'family'})
    if twr:
        vals['family'] = [vals['family'][0] * x for x in twr]
    return days, vals, [next(iter(by_day[d].values()))[1] for d in days]


def live_twr(c, days, values):
    """The LIVE household's growth on each snapshot day as a multiple of the first, from IBKR's time-weighted daily
    returns (so a deposit or withdrawal is not counted as performance), converted to USD, each account weighted by its
    value at the previous snapshot. A snapshot on Sydney day d is taken 20 minutes into New York's session d, so it
    carries IBKR's returns for days before d. None unless IBKR's figures cover every account and day.
    ponytail: Yahoo's AUD/USD close nearest each day stands in for the exact rate at IBKR's valuation time."""
    if len(days) < 2 or not values:
        return None
    rows = {}
    for r in c.execute(f'SELECT * FROM perf WHERE account_id IN ({",".join("?" * len(values))})', list(values)):
        rows.setdefault(r['account_id'], {})[r['day']] = (r['ret'], r['currency'])
    if set(rows) != set(values) or any(min(r) > days[0] for r in rows.values()):
        return None
    fx = feeds.closes(c, ['AUDUSD=X']).get('AUDUSD=X', [])
    at = lambda d: next((x for x_d, x in reversed(fx) if x_d <= d), None)
    out = [1.0]
    for d0, d1 in zip(days, days[1:]):
        total = sum(v[d0] for v in values.values())
        step = 0.0
        for aid, v in values.items():
            g = 1.0
            for day, (ret, cur) in sorted(rows[aid].items()):
                if d0 <= day < d1:
                    g *= 1 + ret
            if rows[aid] and next(iter(rows[aid].values()))[1] == 'AUD':
                a, b = at(d0), at(d1)
                if not a or not b:
                    return None
                g *= b / a  # an AUD return, seen from USD
            step += v[d0] / total * (g - 1)
        out.append(out[-1] * (1 + step))
    return out


# group, chart label, short end label, colour token
BENCH = [('family-sandbox', 'Advisor (sandbox)', 'Advisor', '--sand'), ('family-benchmark', 'Your fixed portfolio', 'Fixed', '--frozen'),
         ('family', 'What you did (LIVE)', 'LIVE', '--live')]


def profile_series(c, prof):
    """(days, {'advisor': [...], 'fixed': [...]}, [SPY per day]) for a friend's profile: its advised sandboxes against its
    frozen ones, on the days every one of them has a value. Empty until the profile has both kinds."""
    kind = {r['id']: r['frozen'] for r in c.execute('SELECT id, frozen FROM accounts WHERE profile=? AND mode="SANDBOX"', (prof,))}
    if set(kind.values()) != {0, 1}:
        return [], {}, []
    by_day = {}
    for r in c.execute(f'SELECT * FROM snapshots WHERE account_id IN ({",".join("?" * len(kind))})', list(kind)):
        by_day.setdefault(r['day'], {})[r['account_id']] = (r['value_usd'], r['spy'])
    days = sorted(d for d, v in by_day.items() if set(v) == set(kind))
    vals = {k: [sum(v for i, (v, _) in by_day[d].items() if kind[i] == fz) for d in days] for k, fz in (('advisor', 0), ('fixed', 1))}
    if days and not (vals['advisor'][0] > 0 and vals['fixed'][0] > 0):
        return [], {}, []
    return days, vals, [next(iter(by_day[d].values()))[1] for d in days]


def friends_bench(c):
    """{profile: returns since its first common day} for every friend's profile with two days of values."""
    out = {}
    for p in profiles(c):
        days, vals, spy = profile_series(c, p) if p != 'family' else ([], {}, [])
        if len(days) >= 2:
            out[p] = {'since': days[0], **{k: v[-1] / v[0] - 1 for k, v in vals.items()}, 'spy': spy[-1] / spy[0] - 1}
    return out


def bench_text(c):
    days, vals, spy = bench_series(c)
    if len(days) < 2:
        return None
    r = {g: v[-1] / v[0] - 1 for g, v in vals.items()}
    # The first LIVE proposal the owners acted on after the hands-off spell: from then on LIVE follows the advice too,
    # picking which proposals to take, so LIVE against the advisor measures the owners' choices.
    acted = c.execute('SELECT MIN(p.run_date) FROM proposals p JOIN accounts a ON a.id=p.account_id WHERE a.grp="family" '
                      'AND p.status IN ("filled","drafted") AND p.run_date > ?', (learn.HANDS_OFF[1],)).fetchone()[0]
    return {'since': days[0], 'advisor': r['family-sandbox'], 'fixed': r['family-benchmark'], 'live': r['family'],
            'spy': spy[-1] / spy[0] - 1, 'live_follows_advice_since': acted,
            'live_measure': live_measure(c, days)}


def live_measure(c, days):
    """How bench_series measured LIVE: by IBKR's time-weighted returns when they reach back to the first day."""
    accts = [r['id'] for r in c.execute('SELECT id FROM accounts WHERE grp="family"')]
    n = c.execute(f'SELECT COUNT(DISTINCT account_id) FROM perf WHERE day <= ? AND account_id IN ({",".join("?" * len(accts))})',
                  [days[0]] + accts).fetchone()[0]
    return ('time-weighted (IBKR): deposits and withdrawals left out' if accts and n == len(accts)
            else 'account value: a deposit or withdrawal shows as a gain or loss')


def signals_for_review(c, kind):
    """The lens or pattern scorecard as short text per name and value, for the monthly review."""
    st = learn.statuses(c) if kind == 'pattern' else {}
    return {k + (f' ({st[k]})' if k in st else ''): {v: {f'{h} days': f'n={n}, avg {a:+.1%}, right {hit:.0%}' for h, (n, a, hit) in hs.items()}
                                                     for v, hs in vs.items()} for k, vs in learn.signal_summary(c, kind).items()}


def review():
    """The monthly review by Opus: stored data only, no tools. Its lessons wait for the owners' approval."""
    c = db()
    learn.score(c)
    today = date.today().isoformat()
    ps = [p for p in learn.rows(c) if p['scores']]
    pct = lambda x: f'{x:+.1%}'
    summary = {f'{h} days': {name: {str(k): f'n={n}, avg {pct(a)}, right {hit:.0%}' for k, (n, a, hit) in learn.summary(ps, h, key).items()}
                             for name, key in (('class', lambda p: p['cls']), ('confidence', lambda p: p['confidence']),
                                               ('side', lambda p: p['side']),
                                               ('policy shock that day', lambda p: p['policy'] or 'not tagged'))} for h in learn.HORIZONS}
    rows = [{'id': p['id'], 'day': p['run_date'], 'household': p['grp'], 'trade': f'{p["side"]} {p["symbol"]}', 'policy_shock': p['policy'],
             'class': p['cls'], 'confidence': p['confidence'], 'reject_reason': p['reject_reason'],
             'excess': {h: pct(x) for h, x in p['scores'].items()}, 'reason': (p['reason'] or '')[:400],
             'exit_plan': json.loads(p['plan']) if p.get('plan') else None} for p in ps]
    ch = [f'{a["grp"]} {a["symbol"]}: {a["side"]} {a["run_date"]}, then {b["side"]} {b["run_date"]}' for a, b in learn.churn(learn.rows(c))]
    reasons = [dict(r) for r in c.execute('SELECT reject_reason, COUNT(*) n FROM proposals WHERE reject_reason IS NOT NULL GROUP BY 1')]
    deep = [json.loads(r['body']).get('method', '') for r in c.execute(
        "SELECT r.body FROM reports r JOIN usage u ON u.id=r.usage_id WHERE u.kind='deep analysis' AND r.run_date >= ?",
        ((date.today() - timedelta(days=45)).isoformat(),)) if r['body'].startswith('{')]
    active = {r['id']: r['text'] for r in c.execute('SELECT id, text FROM lessons WHERE status="active"')}
    verdicts = {v: {f'{h} days': f'n={n}, avg {pct(a)}, beat SPY {hit:.0%}' for h, (n, a, hit) in hs.items()}
                for v, hs in learn.verdict_summary(c).items()}
    outcomes = []
    bought = [p for p in learn.rows(c) if p['side'] == 'BUY' and p['cls'] == 'taken' and p.get('plan')]
    cl = feeds.closes(c, sorted({p['symbol'] for p in bought}))
    for p in bought:
        what, when = learn.plan_outcome(json.loads(p['plan']), cl.get(p['symbol'], []), p['run_date'])
        outcomes.append(f'{p["grp"]} {p["symbol"]} bought {p["run_date"]}: ' + (
            f'{what} after {(date.fromisoformat(when) - date.fromisoformat(p["run_date"])).days} days' if what else 'neither yet'))
    out, raw = claude(REVIEW_PROMPT.format(today=today, summary=json.dumps(summary), rows=json.dumps(rows) if rows else 'none matured yet',
                                           churn=json.dumps(ch) if ch else 'none',
                                           verdicts=json.dumps(verdicts) if verdicts else 'none matured yet',
                                           plan_outcomes=json.dumps(outcomes) if outcomes else 'none yet', reasons=json.dumps(reasons) if reasons else 'none yet',
                                           bench=json.dumps(bench_text(c) or 'not enough history yet'),
                                           friends=json.dumps(friends_bench(c) or 'none with enough history yet'),
                                           lenses=json.dumps(signals_for_review(c, 'lens')) or 'none scored yet',
                                           patterns=json.dumps(signals_for_review(c, 'pattern')) or 'none scored yet',
                                           deep=json.dumps(deep) if deep else 'none', active=json.dumps(active) if active else 'none'),
                      [], REVIEW, 'review', timeout=1800, model='opus', effort='high')
    for x in out['lessons'][:8]:
        c.execute('INSERT INTO lessons (kind, text, evidence, n, created) VALUES (?,?,?,?,?)',
                  (x['kind'], x['text'], x['evidence'], x['n'], now()))
    for i in out['retire']:
        c.execute('UPDATE lessons SET status="retired", decided_by="review", decided_at=? WHERE id=? AND status="active"', (now(), i))
    c.execute('INSERT INTO reports (account_id, grp, run_date, body, at, usage_id) VALUES (NULL,"review",?,?,?,?)',
              (today, out['critique'], now(), raw['_usage_id']))
    c.commit()


# ---------------------------------------------------------------- web
CSS = '''
.sr{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
.pswitch .lbl{font-size:14px;color:var(--mute);font-weight:600}
/* by-hand trades: the stock search list and the buy/sell choice */
.ta{position:relative}
.ta ul{position:absolute;z-index:5;left:0;right:0;top:100%;margin:4px 0 0;padding:4px;list-style:none;background:var(--card);
border:1px solid var(--field);border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.18);max-height:360px;overflow:auto}
.ta li{display:grid;grid-template-columns:5.5em 1fr;gap:0 10px;align-items:baseline;padding:8px 10px;border-radius:6px;cursor:pointer;min-height:44px}
.ta li small{grid-column:2;color:var(--mute);font-size:13px}.ta li.none{display:block;color:var(--mute);cursor:default}
.ta li[aria-selected=true],.ta li:hover{background:color-mix(in srgb,var(--sand) 14%,var(--card))}
#t-pick{margin-top:6px;min-height:1.2em}
fieldset.side{border:0;padding:0;margin:10px 0 0;display:flex;flex-wrap:wrap;gap:0 20px}
fieldset.side legend{font-weight:600;padding:0;margin-bottom:2px}
fieldset.side label{display:inline-flex;align-items:center;gap:8px;font-weight:400;margin:0;min-height:44px}
fieldset.side input{width:auto;min-height:20px}
.pair{display:grid;grid-template-columns:1fr 1fr;gap:0 12px}
:root{--bg:#f6f7f9;--card:#fff;--ink:#16181d;--mute:#5d6470;--line:#dde1e7;--field:#7d8591;--up:#0a7d3b;--dn:#b3261e;
--live:#c62828;--sand:#00796b;--btn:#1f2937;--focus:#2563eb;--err-bg:#fde7e7;--err:#8a1c1c;--s1:#2a78d6;--s2:#eb6834;--ask:#a35a00;--frozen:#1565c0;--frozen-ink:#1565c0;--brand:#5b2fc9;--brand-ink:#5b2fc9}
@media (prefers-color-scheme:dark){:root{--bg:#111316;--card:#1b1e23;--ink:#e8eaed;--mute:#9aa1ab;
--line:#2c3139;--field:#6f7784;--up:#4cc27a;--dn:#ff6b61;--btn:#3a4150;--focus:#7aa7ff;--err-bg:#3b1717;--err:#ffb4ab;
--s1:#3987e5;--s2:#d95926;--frozen-ink:#8ab8ff;--brand:#6d45d9;--brand-ink:#b9a4ff}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:16px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}
main{max-width:860px;margin:0 auto;padding:12px 16px 64px}
a{color:inherit}h1{font-size:22px;margin:16px 0 4px}h2{font-size:18px;margin:24px 0 8px}
.bar{position:sticky;top:0;z-index:10;color:#fff;font-weight:800;letter-spacing:.04em;
text-align:center;padding:10px 16px;font-size:17px}
.bar.LIVE{background:var(--live)}.bar.SANDBOX{background:var(--sand)}.bar.HOME{background:var(--brand)}.bar.FROZEN{background:var(--frozen)}
.frame{position:fixed;inset:0;border:6px solid;pointer-events:none;z-index:9}
.frame.LIVE{border-color:var(--live)}.frame.SANDBOX{border-color:var(--sand)}.frame.FROZEN{border-color:var(--frozen)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;margin:10px 0}
.card.LIVE{border-left:8px solid var(--live)}.card.SANDBOX{border-left:8px solid var(--sand)}.card.FROZEN{border-left:8px solid var(--frozen)}
.tag{display:inline-block;font-size:12px;font-weight:800;color:#fff;border-radius:4px;padding:1px 6px}
.tag.LIVE{background:var(--live)}.tag.SANDBOX{background:var(--sand)}.tag.FROZEN{background:var(--frozen)}
.mute{color:var(--mute);font-size:14px}.up{color:var(--up)}.dn{color:var(--dn)}
.aud{white-space:nowrap}.aud i{font-style:normal}
/* households: the A$ figure on its own line under the big US$ one */
.hh .big .aud{display:block;font-size:15px}.hh .big .aud i{display:none}
/* a move as a tinted pill: the sign says up or down, the colour lets it be seen at a glance */
.chg{display:inline-block;font-size:13px;font-weight:700;padding:0 7px;border-radius:999px;font-variant-numeric:tabular-nums}
.chg.up{background:color-mix(in srgb,var(--up) 14%,transparent)}.chg.dn{background:color-mix(in srgb,var(--dn) 14%,transparent)}
.hrow{display:flex;flex-wrap:wrap;gap:4px 12px;align-items:center;justify-content:space-between}.hrow h1{margin-bottom:0}
.st{display:inline-block;font-size:12px;font-weight:700;padding:0 6px;border-radius:4px;margin-top:2px}
.st.announced{background:color-mix(in srgb,var(--up) 16%,var(--card));color:var(--up)}
.st.expected{border:1px dashed var(--line);color:var(--mute)}
.big{font-size:24px;font-weight:700}
table{width:100%;border-collapse:collapse;font-size:14px;font-variant-numeric:tabular-nums}
/* spreadsheet-style table: columns size to their content and never wrap, instead of squeezing
   to the container width. Used where many short columns read better as a grid than as prose. */
/* tables of words, not figures: every column reads from the left */
table.text th,table.text td{text-align:left}
/* positions: amount, then its percentage in a quieter column of its own */
table.pos .amt{padding-right:6px}table.pos .pc{padding-left:6px;padding-right:10px;color:var(--mute);font-size:13px}
table.pos th[colspan]{text-align:center}
table.grid{display:table;width:auto}table.grid th,table.grid td{white-space:nowrap}table.grid.wide{width:100%}
/* Wide view: the reader opts in (button below), so main's cap only lifts on request -- lifting
   it by default would make ordinary prose pages uncomfortably wide on a desktop monitor. */
html.wide main,html.wide .nav{max-width:none}
th,td{padding:6px 4px;border-bottom:1px solid var(--line);text-align:right}th:first-child,td:first-child{text-align:left}
.scroll{overflow-x:auto}
/* phones: the first column (symbol) stays put while the rest scrolls sideways */
.scroll tr>:first-child{position:sticky;left:0;background:var(--bg);z-index:1}
.card .scroll tr>:first-child,.card.scroll tr>:first-child{background:var(--card)}
/* links inside tables get a 44px-tall hit area without making the rows taller */
td a{display:inline-block;padding:12px 4px;margin:-12px -4px}
/* rows grouped by day: a heading row per day, alternate days tinted */
table.runs tr>:nth-child(2),table.runs tr>:nth-child(3){text-align:left}
tr.day th{text-align:left;padding-top:14px;border-bottom:2px solid var(--brand-ink)}
.scroll tr.band>*,.card.scroll tr.band>:first-child{background:color-mix(in srgb,var(--brand) 7%,var(--bg))}
button,.btn{font:inherit;font-weight:700;border:0;border-radius:8px;padding:10px 14px;min-height:44px;
color:#fff;background:var(--brand);cursor:pointer;text-decoration:none;display:inline-block;
transition:filter .15s,transform .15s}
button:hover,.btn:hover{filter:brightness(1.15)}button:active,.btn:active{transform:scale(.98)}
button[disabled]{opacity:.75;cursor:progress;filter:none;transform:none}
:focus-visible{outline:3px solid var(--focus);outline-offset:2px}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}
.nav{display:flex;flex-wrap:wrap;gap:0 4px;align-items:center;max-width:860px;margin:0 auto;padding:0 12px;
border-bottom:1px solid var(--line)}
.nav a,.nav button{display:inline-flex;align-items:center;min-height:44px;padding:0 8px;color:var(--ink);
background:none;font-weight:600;font-size:15px;text-decoration:none;border-radius:6px}
.nav form{margin-left:auto}.nav button{color:var(--mute)}
/* where you are: the current page's link is underlined in the brand colour (set by SCRIPT) */
.nav a[aria-current=page]{color:var(--brand-ink);box-shadow:inset 0 -3px 0 var(--brand-ink);border-radius:0}
/* phones: one row that scrolls sideways instead of three rows of links */
@media (max-width:699px){.nav{flex-wrap:nowrap;overflow-x:auto;scrollbar-width:none}.nav a,.nav form{flex:none}}
button.ok.LIVE{background:var(--live)}button.ok.SANDBOX{background:var(--sand)}
button.no{background:transparent;color:var(--ink);border:1px solid var(--line)}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.md{white-space:pre-wrap;font-size:15px}
input,textarea{font:inherit;width:100%;padding:8px;border:1px solid var(--field);border-radius:6px;
background:var(--card);color:var(--ink)}label{display:block;margin:10px 0 4px;font-weight:600}
.err{background:var(--err-bg);color:var(--err);padding:10px;border-radius:8px}
.alert{margin:14px 0;padding:14px 16px;border-radius:12px;background:var(--err-bg);color:var(--err);border:2px solid var(--err)}
.alert b{font-size:18px}.alert p{margin:6px 0}.alert ol{margin:6px 0 12px;padding-left:22px}.alert a{color:inherit;font-weight:700}
.alert button{background:var(--err);border-color:var(--err);color:#fff}
pre.md{margin:0;font:13px/1.45 ui-monospace,Menlo,monospace;overflow-x:auto}
.prose h2{font-size:19px;font-weight:700;margin:24px 0 8px;padding-left:10px;border-left:4px solid var(--focus)}
.prose h2:first-child{margin-top:4px;border:0;padding:0;font-size:21px}
.prose h3{font-size:17px;font-weight:700;margin:16px 0 4px;color:var(--focus)}
.prose li>b:first-child{color:var(--focus)}
.prose th{background:color-mix(in srgb,var(--focus) 8%,var(--card))}
.prose .warn{background:color-mix(in srgb,#b26a00 12%,var(--card));border-left:4px solid #b26a00;padding:8px 10px;border-radius:6px}
.prose ul,.prose ol{padding-left:22px}.prose li{margin:4px 0}.prose p{margin:8px 0}
.prose table{margin:6px 0 10px}.prose hr{border:0;border-top:1px solid var(--line)}
.opts{display:grid;gap:10px;margin-top:10px}
.opt{border:1px solid var(--line);border-radius:8px;padding:10px 12px;background:var(--card)}
.opt.rec{border:2px solid #1b7f45}
.opt .yield{font-size:22px;font-weight:700;margin:2px 0 4px}.opt p{margin:4px 0;font-size:15px}
.opt .lbl{display:block;font-size:12px;font-weight:800;letter-spacing:.06em;color:var(--mute);text-transform:uppercase}
.note{background:var(--card);border:2px dashed var(--line);padding:10px;border-radius:8px}
.grid{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(260px,1fr))}.grid .card{margin:0}
.v{font-size:13px;font-weight:800;color:#fff;border-radius:4px;padding:2px 8px;background:#6b7280}
.v.ADD,.v.BUY{background:#1b7f45}.v.WATCH{background:#1d5fa8}.v.TRIM{background:#9a5b00}.v.SELL{background:#b3261e}
summary{cursor:pointer;color:var(--mute);min-height:44px;display:flex;align-items:center}
.viz{position:relative;max-width:560px}.viz svg{display:block;width:100%;height:auto;touch-action:pan-y}
.viz .tip{position:absolute;top:0;pointer-events:none;background:var(--card);border:1px solid var(--line);
border-radius:6px;padding:4px 8px;font-size:13px;white-space:nowrap;display:none;font-variant-numeric:tabular-nums}
.prog{border-color:var(--focus)}
.track{height:10px;border-radius:5px;background:var(--line);overflow:hidden;margin:10px 0 8px}
.fill{height:100%;background:var(--focus);border-radius:5px;transition:width .6s linear}
/* households: one investment across accounts; bigger, tinted, a stripe on top, member chips */
.hh{border:1px solid var(--line);border-top:6px solid var(--btn);border-radius:12px;padding:16px;margin:10px 0 14px}
.hh.LIVE{border-top-color:var(--live);background:color-mix(in srgb,var(--live) 6%,var(--card))}
.hh.SANDBOX{border-top-color:var(--sand);background:color-mix(in srgb,var(--sand) 7%,var(--card))}
.hh.FROZEN{border-top-color:var(--frozen);background:color-mix(in srgb,var(--frozen) 7%,var(--card))}
.hh .big{font-size:30px}.kicker{font-size:14px;font-weight:600;color:var(--mute)}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
.chip{display:inline-flex;align-items:center;min-height:44px;padding:0 12px;border:1px solid var(--line);
border-radius:22px;background:var(--card);text-decoration:none;font-size:14px}
/* the chip used most, filled so it is found first */
.chip.hot{background:color-mix(in srgb,var(--brand) 12%,var(--card));border-color:var(--brand-ink);color:var(--brand-ink);font-weight:700}
/* two-way switch: the chosen option filled */
.seg{display:inline-flex;border:1px solid var(--brand-ink);border-radius:8px;overflow:hidden}
.seg button{border-radius:0;background:transparent;color:var(--brand-ink);min-height:40px;padding:8px 14px}
.seg button[aria-pressed=true]{background:var(--brand);color:#fff}
.seg a{display:inline-flex;align-items:center;min-height:44px;padding:0 14px;color:var(--brand-ink);font-weight:700;text-decoration:none}
.seg a+a,.seg button+button{border-left:1px solid var(--brand-ink)}
.seg a[aria-current=page]{background:var(--brand);color:#fff}
/* household members: one equal column each, name over amount, so every household card has the same shape */
.members{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-top:10px}
.member{display:flex;flex-direction:column;justify-content:center;min-height:52px;padding:6px 12px;border:1px solid var(--line);
border-radius:10px;background:var(--card);text-decoration:none;font-size:14px;line-height:1.3}
.member span{color:var(--mute);font-variant-numeric:tabular-nums}
/* label / value rows (the Analysis card) */
.facts{display:grid;grid-template-columns:auto 1fr;gap:8px 16px;align-items:baseline;margin:0}
.facts dt{color:var(--mute);font-size:14px}.facts dd{margin:0}.facts dd .mute{display:block}
.acct .big{font-size:20px}
/* card figures: label, amount, percentage, each in its own column so the amounts line up */
.stats{display:grid;grid-template-columns:1fr auto auto;gap:1px 10px;align-items:baseline;margin:8px 0 2px;
font-variant-numeric:tabular-nums;max-width:340px}
.stats dt{margin:0;white-space:nowrap}.stats dd{margin:0;text-align:right;white-space:nowrap}
.stats hr{grid-column:1/-1;border:0;border-top:1px solid var(--line);margin:5px 0;width:100%}
.stats .main{font-weight:700}
.stats .sub{font-size:14px;color:var(--mute)}.stats dt.sub{padding-left:12px}.acct{padding:12px 14px}
.key{display:inline-flex;align-items:center;gap:6px;margin-right:14px;font-size:14px}
.key i{width:14px;height:3px;border-radius:2px;display:inline-block}
/* approval status: amber is the one "your move" colour on the page (red and teal already mean
   LIVE/SANDBOX). Filled pill with a count = act; hollow pill = nothing to do. Shape and wording
   carry the meaning too, so it never relies on colour alone. */
.ask{display:inline-flex;align-items:center;gap:8px;margin-top:10px;padding:6px 14px 6px 6px;
border-radius:999px;font-size:15px;font-weight:700;background:var(--ask);color:#fff}
.ask .n{min-width:28px;height:28px;padding:0 6px;border-radius:14px;background:#fff;color:var(--ask);
display:inline-flex;align-items:center;justify-content:center;font-size:16px;font-weight:800}
.ask.none{background:none;color:var(--mute);border:1px dashed var(--line);padding:4px 12px;font-weight:600;font-size:14px}
.todo{display:block;margin:12px 0 4px;padding:12px 14px;border-radius:10px;background:var(--ask);color:#fff;
text-decoration:none;font-weight:700}
.todo .n{font-size:26px;font-weight:800;margin-right:6px;vertical-align:-2px}
.todo span.mute{color:#fff;opacity:.9;font-weight:500}
.card.drafts{border:2px dashed var(--ask)}
.plan{font-size:14px;margin:6px 0;padding:6px 8px;border-left:3px solid var(--focus);background:color-mix(in srgb,var(--focus) 6%,var(--card))}
.trig{display:inline-block;margin:2px 6px 2px 0;padding:2px 8px;border-radius:4px;font-size:13px;font-weight:700;color:#fff;background:var(--ask)}
.card.trigs{border:2px solid var(--ask)}
.ask.frozen{background:none;color:var(--frozen-ink);border:1px solid var(--frozen-ink);padding:4px 12px;font-size:14px}
/* accounts page: two cards a row on a desktop, one on a phone; every card in a row the same height */
.cards{display:grid;gap:12px;margin:10px 0 14px}
@media (min-width:700px){.cards{grid-template-columns:1fr 1fr}}
html.wide .cards{grid-template-columns:repeat(auto-fill,minmax(360px,1fr))}
.cards>a{display:block;text-decoration:none}.cards .card,.cards .hh{margin:0;height:100%}
/* learning page */
.lanes{display:grid;gap:12px}@media (min-width:700px){.lanes{grid-template-columns:1fr 1fr}}
.lane h3{margin:0 0 6px;font-size:16px}.lane ul{margin:0;padding-left:20px}.lane li{margin:6px 0}
.lane.avoid{border-top:4px solid var(--dn)}.lane.favour{border-top:4px solid var(--up)}
.lane.preference{border-top:4px solid var(--focus)}.lane.timing{border-top:4px solid var(--ask)}
.lesson.proposed{border:2px solid var(--ask)}
.bars{display:grid;grid-template-columns:auto minmax(80px,1fr) auto;gap:8px 10px;align-items:center;font-size:14px;
font-variant-numeric:tabular-nums;margin:6px 0 14px}
.bars .b{position:relative;height:14px;background:color-mix(in srgb,var(--line) 60%,transparent);border-radius:3px}
.bars .b i{position:absolute;top:0;bottom:0}
.bars .b i.up{background:var(--up)}.bars .b i.dn{background:var(--dn)}
.bars .b s{position:absolute;top:-3px;bottom:-3px;left:50%;border-left:2px solid var(--mute)}
.bars .num{text-align:right;white-space:nowrap}.bars .num b{display:inline-block;min-width:4.2em}
.pick{border-left:4px solid var(--line);padding:4px 0 4px 10px;margin:10px 0}.pick.up{border-color:var(--up)}.pick.dn{border-color:var(--dn)}
.pick q{display:block;color:var(--mute);font-size:14px;quotes:none}
.headline{font-size:20px;font-weight:700;line-height:1.35;margin:4px 0 2px}
/* dividends page */
.tiles{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));margin:10px 0}
.tiles .card{margin:0}.tiles .big{font-size:22px;color:var(--up)}
.ybars{display:grid;grid-template-columns:auto 1fr auto;gap:6px 10px;align-items:center;font-size:14px;font-variant-numeric:tabular-nums}
.ybars .b{display:flex;height:16px;border-radius:3px;overflow:hidden;background:color-mix(in srgb,var(--line) 50%,transparent)}
.ybars .b i{display:block;height:100%}
tr.you td{font-weight:700;background:color-mix(in srgb,var(--focus) 8%,var(--card))}
select{font:inherit;min-height:44px;padding:0 8px;border:1px solid var(--field);border-radius:8px;background:var(--card);color:var(--ink)}
/* job colours, the same on the How it works and Usage pages: brief orange, research blue, analysis purple,
   app jobs slate. Tinted pills, so the words stay readable; the colour only groups them. */
:root{--j-brief:#c2410c;--j-research:#0369a1;--j-analysis:var(--brand);--j-other:#56606e;--j-you:var(--ask)}
@media (prefers-color-scheme:dark){:root{--j-brief:#fb923c;--j-research:#38bdf8;--j-analysis:var(--brand-ink);--j-other:#a3acb9;--j-you:#f0a340}}
.job{display:inline-block;font-size:13px;font-weight:700;padding:1px 8px;border-radius:999px;white-space:nowrap;
color:var(--jc);background:color-mix(in srgb,var(--jc) 13%,transparent)}
.job.brief{--jc:var(--j-brief)}.job.research{--jc:var(--j-research)}.job.analysis{--jc:var(--j-analysis)}
.job.other{--jc:var(--j-other)}.job.you{--jc:var(--j-you)}td .job{margin:2px 4px 2px 0}
table.text td{vertical-align:top}
td a .job{text-decoration:underline;text-underline-offset:2px}
.job.failed{--jc:var(--dn)}
/* a table's header row tinted, so a long table is found and read by its columns */
.scroll table.tint tr:first-child>th,table.tint tr:first-child>th{background:color-mix(in srgb,var(--brand) 9%,var(--card));border-bottom:2px solid var(--brand-ink)}
/* Learning: investor lenses (passed minus failed as a bar centred on zero) and pattern cards */
table.lenses{width:100%}@media (max-width:600px){table.lenses .idea{display:none}.edge{width:48px}}table.lenses td{vertical-align:top}table.lenses td:last-child{white-space:nowrap}
.edge{display:inline-block;position:relative;width:90px;height:10px;margin-right:8px;vertical-align:middle;border-radius:3px;
background:color-mix(in srgb,var(--line) 60%,transparent)}
.edge::after{content:"";position:absolute;left:50%;top:-3px;bottom:-3px;border-left:2px solid var(--mute)}
.edge i{position:absolute;top:0;bottom:0;border-radius:2px}.edge i.up{background:var(--up)}.edge i.dn{background:var(--dn)}
.pats{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(240px,1fr))}.pats .card{margin:0}
.pat .side{font-size:13px;font-weight:700}.pat .side.bullish{color:var(--up)}.pat .side.bearish{color:var(--dn)}
.pat .track{height:6px;margin:4px 0}.pat .track i{display:block;height:100%;background:var(--j-other)}
.pill{font-size:13px;font-weight:700;padding:1px 9px;border-radius:999px;white-space:nowrap;color:var(--mute);border:1px dashed var(--field)}
.pill.proven{color:var(--up);border:1px solid var(--up);background:color-mix(in srgb,var(--up) 10%,var(--card))}
.pill.failed{color:var(--dn);border:1px solid var(--dn)}
.pat.proven{border-color:var(--up)}.pat.failed{opacity:.75}
/* How it works: the day at a glance, the run as numbered cards edged in the colour of who does it */
.glance .facts{margin:0}
ol.steps>li{padding-bottom:12px}ol.steps>li::before{top:12px}ol.steps>li:not(:last-child)::after{top:44px;bottom:-12px}
ol.steps .card{margin:0;border-left:4px solid var(--jc)}
.step-head{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:4px 10px}
.step-head h3{margin:0;font-size:17px}.gist{margin:6px 0 0;max-width:72ch}
ol.steps details{margin-top:4px}ol.steps summary{min-height:36px;font-size:14px}.more{max-width:72ch;padding-bottom:4px}
.after{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.after .card{margin:0;border-top:4px solid var(--jc)}.after h3{margin:0 0 6px;font-size:16px}.after p{margin:0}
.after .analysis{--jc:var(--j-analysis)}.after .other{--jc:var(--j-other)}
.lensgrid{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}
.lens{margin:0}.lens .idea{color:var(--mute);font-size:14px}.lens p{margin:8px 0 0;font-size:15px}
.lens.own{border-top:4px solid var(--frozen)}.lens.buy{border-top:4px solid var(--sand)}
.trial{border-left:4px dashed var(--j-other)}
/* the daily flow: a numbered timeline, each step's marker coloured by who does it */
ol.flow{list-style:none;padding:0;margin:8px 0;counter-reset:s}
ol.flow>li{counter-increment:s;position:relative;padding:0 0 14px 44px}
ol.flow>li::before{content:counter(s);position:absolute;left:0;top:0;width:30px;height:30px;border-radius:50%;
display:flex;align-items:center;justify-content:center;font-weight:800;font-size:14px;color:#fff;background:var(--jc)}
ol.flow>li:not(:last-child)::after{content:"";position:absolute;left:14px;top:32px;bottom:2px;border-left:2px solid var(--line)}
ol.flow>li>b:first-child{color:var(--jc)}
ol.flow>li.brief{--jc:var(--j-brief)}ol.flow>li.research{--jc:var(--j-research)}ol.flow>li.analysis{--jc:var(--j-analysis)}
ol.flow>li.other{--jc:var(--j-other)}ol.flow>li.you{--jc:var(--j-you)}
/* cost per job: one bar per row, in the job's colour */
.cost{display:block;height:6px;border-radius:3px;margin-top:3px;background:var(--jc);min-width:2px}
/* usage tiles: what the runs would have cost on the API */
.tiles.spend .big{color:var(--brand-ink)}
/* tax register: parcels still held, and the current financial year */
.st.held{background:color-mix(in srgb,var(--frozen) 13%,var(--card));color:var(--frozen-ink)}
.card.scroll tr.now>td,tr.now td{background:color-mix(in srgb,var(--brand) 7%,var(--card));font-weight:700}
.cur{display:inline-block;font-size:13px;font-weight:800;padding:1px 8px;border-radius:4px;margin-left:6px;vertical-align:2px;
color:#fff;background:var(--j-other)}
/* the Analysis card: last run, how far through the wait, next run */
.clock{display:grid;grid-template-columns:auto 1fr auto;gap:4px 14px;align-items:center}
.mkt{margin:0 0 16px}.mkt .dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px;vertical-align:2px;background:var(--mute)}
.mkt.on .dot{background:var(--up);box-shadow:0 0 0 4px color-mix(in srgb,var(--up) 22%,transparent)}
.mkt.on .lane2 i{background:var(--up)}.mkt.on .lane2::after{border-color:var(--up)}
.clock .end{display:flex;flex-direction:column;line-height:1.3}.clock .end:last-child{text-align:right}
.clock .end b{font-size:20px}.clock .lbl{font-size:13px;color:var(--mute);font-weight:600}
.clock .lane2{position:relative;height:8px;border-radius:4px;background:var(--line)}
.clock .lane2 i{position:absolute;left:0;top:0;bottom:0;border-radius:4px;background:var(--brand)}
.clock .lane2::after{content:"";position:absolute;right:-4px;top:-4px;width:12px;height:12px;border-radius:50%;
border:3px solid var(--brand);background:var(--card)}
@media (max-width:519px){.clock{grid-template-columns:1fr 1fr}.clock .lane2{grid-column:1/-1;order:3;margin:6px 4px 4px 0}}
.ctl{display:flex;flex-wrap:wrap;gap:10px 16px;align-items:center;justify-content:space-between;margin-top:16px;
padding-top:14px;border-top:1px solid var(--line)}
.ctl .lbl{font-size:14px;color:var(--mute);margin-right:8px}
'''

e = html.escape


def usd(x, sign=False):
    if x is None:
        return '–'
    return f'{"+" if sign and x > 0 else ""}{"-" if x < 0 else ""}${abs(x):,.2f}'


def both(x, rate):
    return f'{usd(x)} <span class="mute aud"><i>· </i>A{usd(x * rate) if rate else "$–"}</span>'


def pnl(x):
    return f'<span class="{"up" if (x or 0) >= 0 else "dn"}">{usd(x, True)}</span>'


def chg(pct, fmt='+.2%'):
    return f'<span class="chg {"up" if pct >= 0 else "dn"}">{pct:{fmt}}</span>'


def heat(pct, full):
    """A table cell's background: green or red, stronger with the size of the move, full strength at `full`."""
    if pct is None:
        return ''
    return (f' style="background:color-mix(in srgb,var(--{"up" if pct >= 0 else "dn"}) '
            f'{round(4 + 20 * min(abs(pct) / full, 1))}%,transparent)"')


ICON = ("data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'>"
        "<rect width='32' height='32' rx='7' fill='%23{bg}'/><polyline points='6,23 12,16 18,20 26,9' "
        "fill='none' stroke='white' stroke-width='3.5' stroke-linecap='round' stroke-linejoin='round'/></svg>")
ICON_BG = {'LIVE': 'c62828', 'SANDBOX': '00796b', 'FROZEN': '1565c0', 'HOME': '1f2937'}


def nav_html():
    fam = '<a href="/tax?owner=alex">Tax</a><a href="/usage">Usage</a>' if family() else ''
    return NAV.replace('{fam}', '<a href="/dividends">Dividends</a>' + fam)


NAV = ('<nav class=nav aria-label="Main"><a href="/">Accounts</a><a href="/learn">Learning</a>{fam}'
       '<a href="/how">How it works</a><a href="/password">Password</a>'
       # Desktop only, no scroll to save on a phone (hidden/shown by width, see SCRIPT). Lifts
       # main's and the nav's own max-width so a wide table fits without side-scrolling; the
       # choice is per-browser (localStorage) and applied pre-paint, so returning is flash-free.
       '<button type=button class=no id=wide-btn style="display:none" onclick="'
       + e("document.documentElement.classList.toggle('wide');"
           "localStorage.wideTables=document.documentElement.classList.contains('wide')?'1':'0';"
           "wideBtnLabel()") +
       '">Widen</button>'
       '<form method=post action=/logout><button data-busy="Signing out…">Sign out</button></form></nav>')
# Slow buttons (an IBKR draft or refresh runs Claude for 15-30 s) must show they are working,
# or they get tapped again. Runs after any confirm(): a cancelled confirm leaves them alone.
# pageshow undoes it when the browser's back button restores a page from its cache.
SCRIPT = '''<script>
document.addEventListener('submit', ev => {
  if (ev.defaultPrevented) return;
  const b = ev.target.querySelector('button');
  if (b) setTimeout(() => { b.dataset.label = b.textContent; b.textContent = b.dataset.busy || 'Working…'; b.disabled = true; });
});
addEventListener('pageshow', () => document.querySelectorAll('button[disabled]').forEach(b => {
  b.disabled = false; if (b.dataset.label) b.textContent = b.dataset.label; }));
function wideBtnLabel() {
  const b = document.getElementById('wide-btn');
  if (!b) return;
  b.textContent = document.documentElement.classList.contains('wide') ? 'Narrow' : 'Widen';
  b.style.display = matchMedia('(min-width:700px)').matches ? 'inline-block' : 'none';
}
wideBtnLabel();
addEventListener('resize', wideBtnLabel);
// Prices pages reload every 5 minutes while the US market is open (Mon-Fri 09:30-16:05 New York; holidays are
// not known here), which is when the 5-minute price cache has run out. Not while the reader is choosing or typing,
// while a button is working, or in a background tab; a tab brought back after 5 minutes catches up at once.
(function () {
  const q = location.pathname.split('/');  // '/' or '/a/<id>'; never a page that answered a form
  if (!(location.pathname === '/' || (q.length === 3 && q[1] === 'a'))) return;
  const loaded = Date.now();
  const open = () => {
    const p = Object.fromEntries(new Intl.DateTimeFormat('en-US', {timeZone: 'America/New_York', weekday: 'short',
      hour: 'numeric', minute: 'numeric', hourCycle: 'h23'}).formatToParts(new Date()).map(x => [x.type, x.value]));
    const m = +p.hour * 60 + +p.minute;
    return !['Sat', 'Sun'].includes(p.weekday) && m >= 570 && m < 965;
  };
  const busy = () => /INPUT|SELECT|TEXTAREA/.test((document.activeElement || {}).tagName || '') ||
    document.querySelector('button[disabled]') || document.querySelector('details[open]');
  const tick = () => { if (open() && !document.hidden && !busy() && Date.now() - loaded >= 295000) location.reload(); };
  setInterval(tick, 300000);
  document.addEventListener('visibilitychange', tick);
})();
// The market clock counts on between reloads, and reloads when the market opens or closes. dur() mirrors Python's.
(function () {
  const dur = m => { m = Math.round(m); return m < 1 ? 'under a minute' : m < 60 ? m + ' min' :
    m < 600 && m % 60 ? Math.floor(m / 60) + ' h ' + (m % 60) + ' min' : m < 2880 ? Math.round(m / 60) + ' h' : Math.round(m / 1440) + ' days'; };
  const tick = () => {
    const t = Date.now();
    document.querySelectorAll('[data-since]').forEach(x => x.textContent = dur((t - x.dataset.since) / 60000) + ' ago');
    document.querySelectorAll('[data-until]').forEach(x => {
      if (t >= +x.dataset.until) return location.reload();
      x.textContent = 'in ' + dur((x.dataset.until - t) / 60000); });
    document.querySelectorAll('.lane2[data-a]').forEach(x => x.firstElementChild.style.width =
      Math.min(Math.max((t - x.dataset.a) / (x.dataset.b - x.dataset.a), 0), 1) * 100 + '%');
  };
  if (document.querySelector('[data-until]')) setInterval(tick, 30000);
})();
// Stock search for the by-hand trade form: type a ticker or a name, pick from US-listed matches with their exchange.
(function () {
  const box = document.getElementById('t-sym');
  if (!box) return;
  const list = document.getElementById('t-list'), pick = document.getElementById('t-pick'), px = document.getElementById('t-px');
  let items = [], at = -1, timer, seq = 0;
  const esc = s => String(s).replace(/[&<>"]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c]));
  const show = on => { list.hidden = !on; box.setAttribute('aria-expanded', on); if (!on) box.removeAttribute('aria-activedescendant'); };
  const mark = i => {
    at = i;
    [...list.children].forEach((li, k) => li.setAttribute('aria-selected', k === i));
    if (i >= 0 && list.children[i]) { box.setAttribute('aria-activedescendant', 't-o' + i); list.children[i].scrollIntoView({block: 'nearest'}); }
  };
  const choose = i => {
    const x = items[i];
    if (!x) return;
    box.value = x.symbol;
    show(false);
    pick.textContent = x.name + ', ' + x.exchange + ' ' + x.type.toLowerCase();
    fetch('/quote?s=' + encodeURIComponent(x.symbol)).then(r => r.json()).then(q => {
      if (q.price == null || box.value !== x.symbol) return;
      pick.textContent += ', last $' + q.price.toFixed(2);
      px.placeholder = 'current: $' + q.price.toFixed(2);
    });
  };
  box.addEventListener('input', () => {
    clearTimeout(timer);
    pick.textContent = '';
    px.placeholder = 'current price';
    const q = box.value.trim();
    if (!q) return show(false);
    timer = setTimeout(() => {
      const n = ++seq;
      fetch('/search?q=' + encodeURIComponent(q)).then(r => r.json()).then(xs => {
        if (n !== seq) return;
        items = xs; at = -1;
        list.innerHTML = xs.length ? xs.map((x, i) => `<li id=t-o${i} role=option aria-selected=false><b>${esc(x.symbol)}</b>` +
          `<span>${esc(x.name)}</span><small>${esc(x.exchange)} · ${esc(x.type)}</small></li>`).join('')
          : '<li class=none>No US-listed stock or ETF matches</li>';
        show(true);
      });
    }, 200);
  });
  list.addEventListener('mousedown', ev => { const li = ev.target.closest('li[id]'); if (li) { ev.preventDefault(); choose(+li.id.slice(3)); } });
  box.addEventListener('keydown', ev => {
    if (list.hidden) return;
    if (ev.key === 'ArrowDown') { ev.preventDefault(); mark(Math.min(at + 1, items.length - 1)); }
    else if (ev.key === 'ArrowUp') { ev.preventDefault(); mark(Math.max(at - 1, 0)); }
    else if (ev.key === 'Enter' && at >= 0) { ev.preventDefault(); choose(at); }
    else if (ev.key === 'Escape') show(false);
  });
  box.addEventListener('blur', () => setTimeout(() => show(false), 150));
})();
document.querySelectorAll('.nav a').forEach(a => {
  const p = location.pathname, h = a.pathname;
  if (h === '/' ? (p === '/' || p.startsWith('/a/')) : p.startsWith(h)) a.setAttribute('aria-current', 'page');
});
</script>'''


def page(title, body, mode='HOME', label='US STOCK ADVISOR', nav=True):
    frame = f'<div class="frame {mode}"></div>' if mode != 'HOME' else ''
    return (f'<!doctype html><html lang=en><head><meta charset=utf-8>'
            f'<meta name=viewport content="width=device-width,initial-scale=1">'
            f'<link rel=icon href="{ICON.format(bg=ICON_BG[mode])}">'
            f'<title>{e(title)}</title><style>{CSS}</style>'
            '<script>try{if(localStorage.wideTables===\'1\')document.documentElement.classList.add(\'wide\')}catch(e){}</script>'
            '</head><body>'
            f'<div class="bar {mode}">{e(label)}</div>{nav_html() if nav else ""}{frame}<main>{body}</main>{SCRIPT}</body></html>')


def progress_html():
    """Progress bar for a running analysis; polls /progress and reloads the page when done."""
    v = progress_view()
    if not v['running']:
        return ''
    pct = v.get('pct')
    return (f'<div class="card prog" role=status><div class=row style="justify-content:space-between">'
            f'<b>Analysis in progress</b><span class=mute id=p-left>{e(v.get("left", ""))}</span></div>'
            f'<div class=track role=progressbar aria-label="Analysis progress" aria-valuemin=0 aria-valuemax=100'
            f'{f" aria-valuenow={pct}" if pct is not None else ""}><div class=fill id=p-fill style="width:{pct or 3}%"></div></div>'
            f'<div id=p-step>{e(v["step"])}</div><div class=mute id=p-detail>{e(v["detail"])}</div>'
            '<p class=mute style="margin:6px 0 0">This page updates by itself and reloads when the run finishes.</p></div>'
            """<script>
(function poll() {
  setTimeout(async () => {
    try {
      const v = await (await fetch('/progress', {cache: 'no-store'})).json();
      if (!v.running) { location.reload(); return; }
      const bar = document.querySelector('.track');
      if (v.pct != null) { document.getElementById('p-fill').style.width = v.pct + '%'; bar.setAttribute('aria-valuenow', v.pct); }
      document.getElementById('p-step').textContent = v.step;
      document.getElementById('p-detail').textContent = v.detail;
      document.getElementById('p-left').textContent = v.left || '';
    } catch (e) {}
    poll();
  }, 4000);
})();
</script>""")


def md_inline(t):
    """Escaped text with **bold**, *italic*, `code` and [links](https://...) rendered."""
    t = e(t)
    t = re.sub(r'\[([^\]]+)\]\((https?://[^)\s]+)\)', lambda m: f'<a href="{m[2]}" rel=noreferrer>{m[1]}</a>', t)
    t = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', t)
    t = re.sub(r'(?<![*\w])\*(?!\s)(.+?)(?<!\s)\*(?![*\w])', r'<i>\1</i>', t)
    t = re.sub(r'`([^`]+)`', r'<code>\1</code>', t)
    # Signed numbers green or red. Text only, never inside a tag (a URL can hold "-1"); the sign stays,
    # so colour is not the only signal. A range like "3.75-4.00%" is left alone: its "-" follows a digit.
    num = re.compile(r'(?<![\w.])([+\-−][\d][\d,]*(?:\.\d+)?%?)')
    color = lambda m: f'<span class={"up" if m[1][0] == "+" else "dn"}>{m[1]}</span>' if m[1][1:] != '0' else m[1]
    return ''.join(x if x.startswith('<') else num.sub(color, x) for x in re.split(r'(<[^>]+>)', t))


def md(text):
    """The markdown the market brief uses: headings, tables, bullet and numbered lists, paragraphs.
    ponytail: a small subset on purpose (no nesting, no code blocks); the stdlib has no renderer."""
    out, lines, i = [], text.splitlines(), 0
    while i < len(lines):
        l = lines[i]
        if not l.strip():
            i += 1
        elif m := re.match(r'(#{1,6})\s+(.*)', l):
            n = min(len(m[1]) + 1, 4)  # the page already has its h1
            out.append(f'<h{n}>{md_inline(m[2])}</h{n}>')
            i += 1
        elif l.lstrip().startswith('|'):
            rows = []
            while i < len(lines) and lines[i].lstrip().startswith('|'):
                cells = [x.strip() for x in lines[i].strip().strip('|').split('|')]
                if not all(re.fullmatch(r':?-{2,}:?', x) for x in cells if x):
                    rows.append(cells)
                i += 1
            head, body = rows[0], rows[1:]
            out.append('<div class=scroll><table><tr>' + ''.join(f'<th>{md_inline(x)}</th>' for x in head) + '</tr>' +
                       ''.join('<tr>' + ''.join(f'<td>{md_inline(x)}</td>' for x in r) + '</tr>' for r in body) +
                       '</table></div>')
        elif re.match(r'\s*([-*]|\d+\.)\s+', l):
            tag = 'ol' if re.match(r'\s*\d+\.', l) else 'ul'
            items = []
            while i < len(lines) and (m := re.match(r'\s*(?:[-*]|\d+\.)\s+(.*)', lines[i])):
                items.append(f'<li>{md_inline(m[1])}</li>')
                i += 1
            out.append(f'<{tag}>{"".join(items)}</{tag}>')
        elif l.strip() in ('---', '***'):
            out.append('<hr>')
            i += 1
        else:
            para = []
            while i < len(lines) and lines[i].strip() and not re.match(r'(#{1,6}\s|\s*\||\s*([-*]|\d+\.)\s)', lines[i]):
                para.append(lines[i].strip())
                i += 1
            text = " ".join(para)
            warn = re.match(r'\*\*(caveat|note|warning)\b', text, re.I)
            out.append(f'<p class=warn>{md_inline(text)}</p>' if warn else f'<p>{md_inline(text)}</p>')
    return ''.join(out)


def when(iso):
    """'25 Sep 07:40 (2 h ago)' from an ISO timestamp."""
    if not iso:
        return 'never'
    t = datetime.fromisoformat(iso)
    m = int((datetime.now() - t).total_seconds() // 60)
    ago = ('just now' if m < 1 else f'{m} min ago' if m < 60 else f'{m // 60} h ago' if m < 48 * 60 else f'{m // 1440} days ago')
    return f'{t.day} {t:%b %H:%M} ({ago})'


def warm(symbols):
    """Fetch Yahoo quotes in parallel, so a page listing many symbols waits once, not per symbol."""
    from concurrent.futures import ThreadPoolExecutor
    def one(sym):
        try:
            quote_full(sym)
        except Exception:
            pass
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(one, set(symbols)))


def today_fills(c, ids):
    """Filled trades in these accounts, each with the New York date it was made (decided_at is Sydney time).
    ponytail: only trades made through this app; a trade placed by hand in IBKR counts from the previous close."""
    q = ','.join('?' * len(ids))
    return [{'symbol': r['symbol'], 'side': r['side'], 'qty': r['qty'], 'price': r['fill_price'],
             'day': datetime.fromisoformat(r['decided_at']).replace(tzinfo=learn.SYD).astimezone(learn.NY).date().isoformat()}
            for r in c.execute(f'SELECT * FROM proposals WHERE status IN ("filled","manual") AND fill_price IS NOT NULL AND decided_at '
                               f'>= date("now", "-3 days") AND account_id IN ({q})', list(ids))] if ids else []


def day_move(qty, now_px, prev, fills):
    """(change $, the money it is measured against) for one symbol over the session. Shares held since the previous
    close count from that close; shares bought in the session from their fill price; shares sold in the session
    count from the previous close to their sale price."""
    bought = [(f['qty'], f['price']) for f in fills if f['side'] == 'BUY']
    sold = [(f['qty'], f['price']) for f in fills if f['side'] == 'SELL']
    held = max(qty - sum(q for q, _ in bought) + sum(q for q, _ in sold), 0)
    chg = held * (now_px - prev) + sum(q * (now_px - x) for q, x in bought) + sum(q * (y - prev) for q, y in sold)
    return chg, held * prev + sum(q * x for q, x in bought) + sum(q * prev for q, _ in sold)


def day_change(positions, fills=()):
    """{symbol: (change $, change %)} plus the total, over the latest session, from Yahoo; trades made in that
    session count from their fill price (see day_move). Both prices come from Yahoo so they are from the same
    moment: IBKR's pre-market price against Yahoo's previous close once counted two days of movement. IBKR itself
    reports daily P&L for only one of the two accounts."""
    qty = {}
    for p in positions:
        qty[p['symbol']] = qty.get(p['symbol'], 0) + p['qty']
    for f in fills:
        qty.setdefault(f['symbol'], 0)  # sold out in the session: no longer a position, still part of the day
    warm(qty)
    rows, day_tot, base_tot = {}, 0, 0
    for sym, q in qty.items():
        try:
            now_px, prev = quote_full(sym)
            day = session_day(sym)
        except Exception:
            continue
        if prev and now_px:
            chg, base = day_move(q, now_px, prev, [f for f in fills if f['symbol'] == sym and f['day'] == day])
            if base:
                rows[sym] = (chg, chg / base)
                day_tot += chg
                base_tot += base
    return rows, ((day_tot, day_tot / base_tot) if base_tot else None)


def value_chart(c, a, s):
    """The account's % return since its first snapshot, against the S&P 500 (SPY)."""
    pts = [(r['day'], r['value_usd'], r['spy']) for r in
           c.execute('SELECT day, value_usd, spy FROM snapshots WHERE account_id=? ORDER BY day', (a['id'],))]
    try:
        if s and pts and pts[-1][0] != date.today().isoformat():
            pts.append(('now', s['value'], quote('SPY')))
    except Exception:
        pass
    if len(pts) < 2:
        return ('<p class=mute>A chart of this account against the S&amp;P 500 appears once there are two days of '
                'history (a point is recorded at each analysis).</p>')
    return return_chart([d for d, _, _ in pts], [('This account', 'You', '--s1', [v for _, v, _ in pts]),
                                                 ('S&P 500 (SPY)', 'S&P', '--s2', [x for _, _, x in pts])])


def return_chart(days, series):
    """Inline SVG of each series' % return since the first day, on one axis. series: [(name, end label, colour
    token, values)]. Legend + direct end labels, crosshair tooltip, and the numbers as a table for anyone who
    cannot use the chart."""
    R_ = [[v / vals[0] - 1 for v in vals] for *_, vals in series]
    flat = [x for r in R_ for x in r] + [0]
    lo, hi = min(flat), max(flat)
    span = max(hi - lo, 0.002)
    step = next(st for st in (0.001, 0.0025, 0.005, 0.01, 0.02, 0.025, 0.05, 0.1, 0.2, 0.25, 0.5, 1) if span / st <= 5)
    lo, hi = step * (lo // step), step * -(-hi // step)
    W, H, L, R, T, Bm = 370, 190, 46, 100, 10, 24  # about phone width: 12-unit text stays ~12px
    X = lambda i: L + (W - L - R) * i / (len(days) - 1)
    Y = lambda v: T + (H - T - Bm) * (hi - v) / (hi - lo)
    lab = lambda d: 'now' if d == 'now' else f'{int(d[8:])} {date.fromisoformat(d):%b}'
    g = ''
    k = lo
    while k <= hi + 1e-9:
        g += (f'<line x1={L} x2={W - R} y1={Y(k):.1f} y2={Y(k):.1f} stroke="var(--line)" stroke-width=1 />'
              f'<text x={L - 6} y={Y(k) + 4:.1f} text-anchor=end font-size=12 fill="var(--mute)">{k:+.1%}</text>')
        k += step
    # End labels, spread so none sits closer than 14 units to the next.
    order = sorted(range(len(series)), key=lambda n: Y(R_[n][-1]))
    ly = {}
    for n in order:
        ly[n] = max(Y(R_[n][-1]), max((ly[m] for m in ly), default=-99) + 14)
    lines = ends = ''
    for n in reversed(range(len(series))):  # the first series is drawn last, on top
        name, short, col, _ = series[n]
        lines += (f'<polyline fill=none stroke="var({col})" stroke-width={2.5 if n == 0 else 2} stroke-linejoin=round '
                  f'stroke-linecap=round points="{" ".join(f"{X(i):.1f},{Y(v):.1f}" for i, v in enumerate(R_[n]))}" />')
        ends += (f'<circle cx={X(len(days) - 1):.1f} cy={Y(R_[n][-1]):.1f} r=4 fill="var({col})" stroke="var(--card)" stroke-width=2 />'
                 f'<text x={X(len(days) - 1) + 8:.1f} y={ly[n] + 4:.1f} font-size=12 fill="var(--ink)">{e(short)} {R_[n][-1]:+.1%}</text>')
    desc = ', '.join(f'{name} {R_[n][-1]:+.1%}' for n, (name, *_) in enumerate(series))
    svg = (f'<svg viewBox="0 0 {W} {H}" role=img aria-label="{e(f"Return since {lab(days[0])}: {desc}")}">{g}{lines}{ends}'
           f'<text x={L} y={H - 6} font-size=12 fill="var(--mute)">{lab(days[0])}</text>'
           f'<text x={W - R} y={H - 6} font-size=12 fill="var(--mute)" text-anchor=end>{lab(days[-1])}</text>'
           f'<line class=cross y1={T} y2={H - Bm} stroke="var(--mute)" stroke-width=1 visibility=hidden />'
           f'<rect x={L} y=0 width={W - L - R} height={H} fill=transparent /></svg>')
    data = json.dumps([{'x': round(X(i), 1), 'd': lab(d), 'v': [f'{r[i]:+.2%}' for r in R_]} for i, d in enumerate(days)])
    names = json.dumps([name for name, *_ in series])
    table = ''.join(f'<tr><td>{lab(d)}</td>' + ''.join(f'<td>{r[i]:+.2%}</td>' for r in R_) + '</tr>' for i, d in enumerate(days))
    return (f'<div class=mute style="margin-top:10px">' +
            ''.join(f'<span class=key><i style="background:var({col})"></i>{e(name)}</span>' for name, _, col, _ in series) +
            f'</div><div class=viz data-w={W} data-pts=\'{e(data)}\' data-names=\'{e(names)}\'>{svg}<div class=tip></div></div>'
            f'<details><summary>Chart as a table</summary><div class=scroll><table><tr><th>Day</th>' +
            ''.join(f'<th>{e(name)}</th>' for name, *_ in series) + f'</tr>{table}</table></div></details>'
            """<script>
document.querySelectorAll('.viz').forEach(v => {
  if (v.dataset.on) return; v.dataset.on = 1;
  const pts = JSON.parse(v.dataset.pts), names = JSON.parse(v.dataset.names), svg = v.querySelector('svg'),
        tip = v.querySelector('.tip'), cross = v.querySelector('.cross'), W = +v.dataset.w;
  const show = ev => {
    const r = svg.getBoundingClientRect(), x = (ev.clientX - r.left) * W / r.width;
    const p = pts.reduce((a, b) => Math.abs(b.x - x) < Math.abs(a.x - x) ? b : a);
    cross.setAttribute('x1', p.x); cross.setAttribute('x2', p.x); cross.setAttribute('visibility', 'visible');
    tip.replaceChildren();
    const d = document.createElement('div'); d.textContent = p.d; d.className = 'mute'; tip.append(d);
    names.forEach((n, i) => {
      const row = document.createElement('div'), b = document.createElement('b');
      b.textContent = p.v[i]; row.append(b, ' ' + n); tip.append(row);
    });
    tip.style.display = 'block';
    tip.style.left = Math.min(Math.max(p.x * r.width / W - tip.offsetWidth / 2, 0), r.width - tip.offsetWidth) + 'px';
  };
  svg.addEventListener('pointermove', show); svg.addEventListener('pointerdown', show);
  svg.addEventListener('pointerleave', () => { tip.style.display = 'none'; cross.setAttribute('visibility', 'hidden'); });
});
</script>""")


def banner(a):
    return (f'LIVE — {a["name"].upper()} — REAL MONEY' if a['mode'] == 'LIVE'
            else f'FROZEN BENCHMARK — {a["name"].upper()} — NO ADVICE' if a['frozen']
            else f'SANDBOX — {a["name"].upper()} — SIMULATED')


def running():
    """{account id or None (= all accounts): start time} for analysis jobs actually running.
    Services only: the timer is 'active' all day and must not count."""
    r = subprocess.run(['systemctl', 'list-units', '--no-legend', '--plain', '--state=active,activating',
                        'stock-analyse.service', 'stock-analyse-a*.service'], capture_output=True, text=True)
    out = {}
    for unit in (line.split()[0] for line in r.stdout.splitlines() if line.strip()):
        # InactiveExit, not ActiveEnter: a oneshot is 'activating' for its whole run and only
        # gets an ActiveEnter timestamp once it has finished.
        at = subprocess.run(['systemctl', 'show', '-p', 'InactiveExitTimestamp', '--value', unit],
                            capture_output=True, text=True).stdout.strip()
        m = re.fullmatch(r'stock-analyse-a(\d+)\.service', unit)
        out[int(m[1]) if m else None] = ' '.join(at.split()[1:3])[:-3] if at else ''
    return out


def sources_html(src):
    if not src:
        return ''
    li = ''
    for x in src:
        name = e(x.get('name', ''))
        if x.get('url'):
            name = f'<a href="{e(x["url"])}" rel=noreferrer>{name}</a>'
        li += f'<li>{name}{" · " + e(x["date"]) if x.get("date") else ""}</li>'
    return f'<p class=mute style="margin-bottom:0">Sources:</p><ul class=mute style="margin-top:4px">{li}</ul>'


def option_cards(options):
    """Cash-parking options as stacked cards: a table of long text columns is unreadable on a phone."""
    h = '<div class=opts>'
    for o in options:
        v = o.get('verdict')
        badge = f'<span class="v {"ADD" if v == "RECOMMENDED" else "HOLD"}">{e(v)}</span>' if v else ''
        sym = f' <span class=mute>{e(o["symbol"])}</span>' if o.get('symbol') and o['symbol'] not in o['name'] else ''
        rows = [('Risk', o.get('risk')), ('After tax', o.get('after_tax')),
                ('Tax', ' · '.join(x for x in (o.get('domicile'), o.get('withholding')) if x))]
        h += (f'<div class="opt{" rec" if v == "RECOMMENDED" else ""}"><div class=row style="justify-content:space-between">'
              f'<b>{e(o["name"])}{sym}</b>{badge}</div><div class=yield>{o["yield_pct"]:.2f}%<span class=mute> yield</span></div>'
              + ''.join(f'<p><span class=lbl>{k}</span>{e(t)}</p>' for k, t in rows if t) + '</div>')
    return h + '</div>'


def analysis_html(body, accounts, pend, notes=None, plans=None):
    """accounts: [(name, state)] of the household; cards show combined weight and who holds what."""
    positions, split = [], {}
    for name, st in accounts:
        for p in (st or {}).get('positions', []):
            positions.append(p)
            split.setdefault(p['symbol'], []).append(f'{name} {p["qty"]:g}')
    comb = {}
    for p in positions:
        x = comb.setdefault(p['symbol'], {'symbol': p['symbol'], 'value': 0, 'pnl': 0})
        x['value'] += p['value'] or 0
        x['pnl'] += p['pnl'] or 0
    positions = list(comb.values())
    many = len(accounts) > 1
    try:
        d = json.loads(body)
        d['holdings']
    except Exception:  # older plain-text reports, and failures
        return f'<div class="card md">{e(body)}</div>'
    pos = {p['symbol']: p for p in positions}
    total = sum(p['value'] or 0 for p in positions) or 1
    prop = {p['symbol']: f'{p["side"]} {p["qty"]:g}' + (f' in {p["account"]}' if many else '') for p in pend}
    h = f'<div class=card><b>Portfolio view</b><p class=md>{md_inline(d["portfolio"])}</p>'
    if d.get('method'):
        h += f'<details><summary>How these decisions were reached</summary><p class=md>{e(d["method"])}</p></details>'
    h += '</div>'
    cp = d.get('cash_plan')
    if cp:
        h += f'<div class=card><b>Cash plan</b><p class=md>{md_inline(cp["summary"])}</p>'
        if cp.get('accounts'):
            h += ('<div class=scroll><table><tr><th>Account</th><th>Cash</th><th>Keep ready</th><th>Park</th></tr>' +
                  ''.join(f'<tr><td>{e(x["account"])}</td><td>{usd(x["cash_usd"])}</td><td>{usd(x["reserve_usd"])}</td>'
                          f'<td>{usd(x["park_usd"])}</td></tr>' for x in cp['accounts']) + '</table></div>'
                  '<p class=mute>Cash stays in its own account: it can only buy there.</p>')
        elif 'reserve_usd' in cp:  # reports from before the per-account plan
            h += f'<p class=mute>keep ready {usd(cp["reserve_usd"])} · park {usd(cp["park_usd"])}</p>'
        if cp.get('options'):
            h += option_cards(cp['options'])
        h += '</div>'
    who = split if many else {}
    h += '<h3>Holdings</h3>' + cards(d['holdings'], pos, total, prop, who, notes, plans)
    if d.get('opportunities'):
        h += '<h3>Opportunities</h3>' + cards(d['opportunities'], pos, total, prop, who, notes)
    return h


def cards(items, pos, total, prop, who=None, notes=None, plans=None):
    h = '<div class=grid>'
    order = {'SELL': 0, 'TRIM': 1, 'BUY': 2, 'ADD': 3, 'WATCH': 4, 'HOLD': 5}
    for x in sorted(items, key=lambda x: (order.get(x['verdict'], 6), -((pos.get(x['symbol']) or {}).get('value') or 0))):
        p = pos.get(x['symbol'])
        facts = (f'{(p["value"] or 0) / total:.1%} of holdings · {pnl(p["pnl"])}' if p else 'not held')
        if who and x['symbol'] in who:
            facts += ' · ' + e(', '.join(who[x['symbol']]))
        tag = f' <span class=tag style="background:var(--btn)">proposal: {e(prop[x["symbol"]])}</span>' if x['symbol'] in prop else ''
        h += (f'<div class=card><div class=row style="justify-content:space-between">'
              f'<span class=big>{e(x["symbol"])}</span><span class="v {e(x["verdict"])}">{e(x["verdict"])}</span></div>'
              f'<div class=mute>{facts}{tag}</div><p><b>{e(x["headline"])}</b></p>'
              + plan_html((plans or {}).get(x['symbol'])) +
              f'<details><summary>Details</summary>{card_detail(x, (notes or {}).get(x["symbol"]))}</details></div>')
    return h + '</div>'


def card_detail(x, note):
    """The card's own text (this portfolio), then the shared research note's facts. Reports from before
    2026-09-26 carry a full "detail" of their own instead."""
    h = ''.join(f'<p class=md>{e(x[k])}</p>' for k in ('fit', 'detail') if x.get(k))
    if note and not x.get('detail'):
        h += ('<p class=mute style="margin-bottom:0">From the research notes:</p>' +
              ''.join(f'<p class=md><b>{label}:</b> {e(note[k])}</p>'
                      for k, label in (('trend', 'Trend'), ('summary', 'Summary'), ('news', 'News'),
                                       ('analysts', 'Analysts'), ('analyst_trend', 'Analyst counts'), ('earnings', 'Earnings'),
                                       ('insiders', 'Insiders'), ('valuation', 'Valuation'), ('fundamentals', 'Fundamentals'),
                                       ('options', 'Options'), ('filings', 'SEC filings'), ('policy', 'Policy exposure'),
                                       ('lenses', 'Investor lenses'), ('patterns', 'Patterns'),
                                       ('domicile', 'Domicile'), ('withholding', 'Withholding')) if note.get(k)))
    return h + sources_html((x.get('sources') or []) + ((note or {}).get('sources') or [] if not x.get('detail') else []))


def taken(c, a):
    """[(day, realised gain in USD)] for one account. LIVE: every USD sale in the owner's tax register (ASX parcels are
    left out, as the account views are US-only). SANDBOX and FROZEN: their own sales, plus their LIVE twin's sales
    before the day they were cloned, so every household's "all time" covers the same history.
    ponytail: the twin is the LIVE account whose name starts the clone's ("Sam Sandbox" -> "Sam")."""
    if a['mode'] == 'LIVE':
        return [(r['sale_date'], r['pnl']) for r in tax.rows(c, a['owner']) if r['sale_date'] and r['currency'] == 'USD']
    own = [(r['decided_at'][:10], r['realised']) for r in c.execute(
        'SELECT decided_at, realised FROM proposals WHERE account_id=? AND side="SELL" AND status IN ("filled","manual") '
        'AND realised IS NOT NULL', (a['id'],))]
    twin = next((t for t in c.execute('SELECT * FROM accounts WHERE mode="LIVE"')
                 if a['name'].split()[0] == t['name']), None)
    return own + ([(d, g) for d, g in taken(c, twin) if d < a['created'][:10]] if twin else [])


def stats_html(c, accts, positions, day=None, today=None, cash=None):
    """The card's figures as one aligned table: today's move, then the overall gain and what it is made of (the gain on
    shares held plus every gain already taken by selling; this financial year's share of the taken gains below it).
    Whole dollars: cents are noise at this size."""
    today = today or date.today().isoformat()
    rows = [x for a in accts for x in taken(c, a)]
    fy_now = tax.fy(today)
    fy = sum(g for d, g in rows if tax.fy(d) == fy_now)
    ever = sum(g for _, g in rows)
    held = sum(p['pnl'] or 0 for p in positions)
    cost = sum((p['avg'] or 0) * p['qty'] for p in positions if p['pnl'] is not None)
    money = lambda x: (f'<span class="{"up" if x > 0.5 else "dn" if x < -0.5 else "mute"}">'
                       f'{"+" if x > 0.5 else "-" if x < -0.5 else ""}${abs(x):,.0f}</span>')
    row = lambda label, x, pct='', cls='': f'<dt class="{cls}">{label}</dt><dd class="{cls}">{money(x)}</dd><dd>{pct}</dd>'
    return ('<dl class=stats>'
            + (row('Today', day[0], chg(day[1])) + '<hr>' if day else '')
            + row('Overall gain', held + ever, '', 'main')
            + row('on shares held', held, chg(held / cost, '+.1%') if cost else '', 'sub')
            + row('taken, all time', ever, '', 'sub')
            + row(f'taken in {fy_now}', fy, '', 'sub')
            + (f'<hr><dt>Cash <span class=mute>{cash[1]}</span></dt><dd>${cash[0]:,.0f}</dd><dd></dd>' if cash else '')
            + '</dl>')


def drafts_html(c, aid=None):
    """Approved LIVE drafts the app has not yet seen filled, with the button that checks IBKR."""
    rows = c.execute('SELECT p.symbol, p.side, p.qty, p.note, a.name FROM proposals p JOIN accounts a ON a.id=p.account_id '
                     'WHERE p.status="drafted"' + (' AND a.id=?' if aid else '') + ' ORDER BY p.id', (aid,) if aid else ()).fetchall()
    if not rows:
        return ''
    items = ''.join(f'<li>{e(r["side"])} {r["qty"]:g} {e(r["symbol"])} in {e(r["name"])}\'s account'
                    f'{f" <span class=mute>({e(r[3])})</span>" if r["note"] and "partly" in r["note"] else ""}</li>' for r in rows)
    return (f'<div class="card drafts"><b>{len(rows)} draft{"s" if len(rows) != 1 else ""} sent to IBKR</b>'
            f'<ul style="margin:6px 0 10px;padding-left:20px">{items}</ul>'
            '<form method=post action=/refresh class=row><button class=no data-busy="Checking IBKR… about 20 s">'
            'Submitted them? Check IBKR</button><span class=mute>updates positions, cash and these drafts</span></form></div>')


PRICES_BTN = ('<form method=post action=/prices class=row><button class=no data-busy="Getting prices…">Refresh prices</button>'
              '<span class=mute>fetches the latest prices now; while the US market is open this page also does it '
              'every 5 minutes</span></form>')
PRICES_SMALL = ('<form method=post action=/prices><button class=no data-busy="Getting prices…" '
                'title="Fetch the latest prices and recalculate Today">Refresh prices</button></form>')


def ask_html(n, frozen=False):
    if frozen:
        return '<div><span class="ask frozen">Benchmark: no advice, trades by hand only</span></div>'
    if not n:
        return '<div><span class="ask none">Nothing to approve</span></div>'
    return (f'<div><span class=ask><span class=n>{n}</span>proposal{"s" if n != 1 else ""} '
            'awaiting approval</span></div>')


# NYSE full-day closures and 1 pm closes. ponytail: written out by hand; add each year's from nyse.com/markets/hours-calendars.
HOLIDAYS = {'2026-11-26': 'Thanksgiving', '2026-12-25': 'Christmas', '2027-01-01': "New Year's Day", '2027-01-18': 'Martin Luther King Day',
            '2027-02-15': "Washington's Birthday", '2027-03-26': 'Good Friday', '2027-05-31': 'Memorial Day', '2027-06-18': 'Juneteenth',
            '2027-07-05': 'Independence Day', '2027-09-06': 'Labor Day', '2027-11-25': 'Thanksgiving', '2027-12-24': 'Christmas'}
EARLY_CLOSE = {'2026-11-27', '2026-12-24', '2027-11-26'}


def session(d):
    """(open, close) of the regular New York session on date d, aware datetimes, or None on a weekend or holiday."""
    if d.weekday() >= 5 or d.isoformat() in HOLIDAYS:
        return None
    at = lambda h, m: datetime(d.year, d.month, d.day, h, m, tzinfo=learn.NY)
    return at(9, 30), at(13, 0) if d.isoformat() in EARLY_CLOSE else at(16, 0)


def market_clock(now=None):
    """Where the US market is: {'open', 'state', 'start', 'end'}. Open: start and end are today's open and close.
    Closed: from the last close to the next open, and state says why (pre-market, after hours, weekend, a holiday)."""
    now = (now or datetime.now(learn.NY)).astimezone(learn.NY)
    today = session(now.date())
    if today and today[0] <= now < today[1]:
        return {'open': True, 'state': 'Open' + (' (closes early)' if now.date().isoformat() in EARLY_CLOSE else ''),
                'start': today[0], 'end': today[1]}
    last = next(x[1] for k in range(0, 10) if (x := session(now.date() - timedelta(days=k))) and x[1] <= now)
    nxt = next(x[0] for k in range(0, 10) if (x := session(now.date() + timedelta(days=k))) and x[0] > now)
    hol = HOLIDAYS.get(now.date().isoformat())
    state = (f'Closed for {hol}' if hol else 'Closed for the weekend' if now.weekday() >= 5 else
             'Pre-market' if today and 4 <= now.hour and now < today[0] else
             'After hours' if today and now >= today[1] and now.hour < 20 else 'Closed overnight')
    return {'open': False, 'state': state, 'start': last, 'end': nxt}


def dur(minutes):
    """'3 h 20 min', '14 h', '2 days': as precise as is useful at that distance. Mirrored by dur() in SCRIPT."""
    m = round(minutes)
    return ('under a minute' if m < 1 else f'{m} min' if m < 60 else f'{m // 60} h {m % 60} min' if m < 600 and m % 60
            else f'{round(m / 60)} h' if m < 2880 else f'{round(m / 1440)} days')


def market_html(now=None):
    """The top-of-page clock: open or closed, how long since, how long until, in New York and Sydney time."""
    k = market_clock(now)
    now = (now or datetime.now(learn.NY)).astimezone(learn.NY)
    a, b = k['start'], k['end']
    done = min(max((now - a) / (b - a), 0), 1)
    when = lambda t: (f'{t:%H:%M} New York · {t.astimezone(learn.SYD):%H:%M} Sydney' if t.date() == now.date()
                      else f'{t:%a %H:%M} New York · {t.astimezone(learn.SYD):%a %H:%M} Sydney')
    ms = lambda t: int(t.timestamp() * 1000)
    return (f'<div class="card mkt {"on" if k["open"] else "off"}"><div class=clock>'
            f'<div class=end><span class=lbl>US market</span><b><span class=dot aria-hidden=true></span>{e(k["state"])}</b>'
            f'<span class=mute>{"Opened" if k["open"] else "Closed"} <span data-since={ms(a)}>{dur((now - a).total_seconds() / 60)} ago</span></span></div>'
            f'<div class=lane2 data-a={ms(a)} data-b={ms(b)} role=img aria-label="{done:.0%} of the way to the {"close" if k["open"] else "open"}">'
            f'<i style="width:{100 * done:.0f}%"></i></div>'
            f'<div class=end><span class=lbl>{"Closes" if k["open"] else "Opens"}</span>'
            f'<b data-until={ms(b)}>in {dur((b - now).total_seconds() / 60)}</b><span class=mute>{when(b)}</span></div></div></div>')


def profile_switch(c, prof):
    """Alex's way between the family's accounts and a friend's: a cookie, checked against ADMINS on every request."""
    return ('<form method=post action=/profile class="row pswitch" style="margin:12px 0"><span class=lbl>Profile</span>'
            '<span class=seg role=group aria-label="Whose accounts to show">'
            + ''.join(f'<button name=p value="{e(p)}" aria-pressed={"true" if p == prof else "false"}>{e(p.title())}</button>'
                      for p in profiles(c)) + '</span></form>')


def home(c, user):
    prof = getattr(CTX, 'profile', profile_of(user))
    is_fam = prof == 'family'
    h = (profile_switch(c, prof) if user in ADMINS else '') + (ibkr_alert(c) if is_fam else '') + market_html() + progress_html()
    rows = c.execute('SELECT * FROM accounts WHERE profile=? ORDER BY mode, frozen, id', (prof,)).fetchall()
    ids = [a['id'] for a in rows]
    warm([r['symbol'] for r in c.execute(f'SELECT symbol FROM positions WHERE account_id IN ({",".join("?" * len(ids))})', ids)]
         + ['AUDUSD=X'] + [p['contract_description'] for a in rows if a['snapshot']
                           for p in json.loads(a['snapshot'])['positions']['positions']])
    accts = [(a, state(c, a)) for a in rows]
    fam = {}
    for a, s in accts:
        if a['grp'] and s:
            fam.setdefault(a['grp'], []).append((a, s))
    fam = {k: v for k, v in fam.items() if len(v) > 1}
    # The one thing to act on goes first, above every balance, so it is seen without looking for it.
    waiting = c.execute('SELECT a.id, a.name, a.mode, COUNT(*) n FROM proposals p JOIN accounts a ON a.id=p.account_id '
                        'WHERE p.status="pending" AND a.profile=? GROUP BY a.id ORDER BY a.mode, a.id', (prof,)).fetchall()
    h += drafts_html(c) if is_fam else ''
    for w in waiting:
        h += (f'<a class=todo href="/a/{w["id"]}"><span class=n>{w["n"]}</span>'
              f'proposal{"s" if w["n"] != 1 else ""} to approve in {e(w["name"])} '
              f'<span class=mute>({w["mode"].lower()}) · review</span></a>')
    if fam:
        h += f'<div class=hrow><h1>Households</h1>{PRICES_SMALL}</div><p class=mute style="margin-top:0">Accounts treated as one investment.</p><div class=cards>'
    for key, lst in fam.items():
        mode = look(lst[0][0])
        tot = sum(s['value'] for _, s in lst)
        rate = lst[0][1]['aud_per_usd']
        day = day_change([p for _, s in lst for p in s['positions']], today_fills(c, [a['id'] for a, _ in lst]))[1]
        pend = c.execute(f'SELECT COUNT(*) FROM proposals WHERE status="pending" AND account_id IN '
                         f'({",".join("?" * len(lst))})', [a['id'] for a, _ in lst]).fetchone()[0]
        # The household's tag already says SANDBOX or FROZEN: the person's name is enough ("Alex Sandbox" -> "Alex").
        chips = ''.join(f'<a class=member href="/a/{a["id"]}" aria-label="{e(a["name"])}"><b>{e(a["name"].split()[0])}</b>'
                        f'<span>{usd(s["value"])[:-3]}</span></a>'
                        for a, s in lst)
        h += (f'<section class="hh {mode}"><div class=kicker><span class="tag {mode}">{mode}</span> household</div>'
              f'<div class=big>{both(tot, rate)}</div>'
              f'{stats_html(c, [a for a, _ in lst], [p for _, s in lst for p in s["positions"]], day, cash=(sum(s["cash"] for _, s in lst), f"({len(lst)} accounts)"))}'
              f'{ask_html(pend, mode == "FROZEN")}<div class=members>{chips}</div></section>')
    h += ('</div>' if fam else '') + '<h1 style="margin-top:28px">Individual accounts</h1><div class=cards>'
    if not accts:
        h += ('<p class=mute>No accounts yet. Start with a sandbox the advisor manages, and a frozen copy to measure it '
              'against: <a href="/new">new sandbox</a>.</p>')
    for a, s in accts:
        pend = c.execute('SELECT COUNT(*) FROM proposals WHERE account_id=? AND status="pending"',
                         (a['id'],)).fetchone()[0]
        val = both(s['value'], s['aud_per_usd']) if s else '<span class=mute>not loaded yet</span>'
        day = day_change(s['positions'], today_fills(c, [a['id']]))[1] if s else None
        h += (f'<a href="/a/{a["id"]}" style="text-decoration:none"><div class="card acct {look(a)}">'
              f'<span class="tag {look(a)}">{look(a)}</span> <b>{e(a["name"])}</b>'
              f'<div class=big>{val}</div>{stats_html(c, [a], s["positions"], day) if s else ""}'
              f'{ask_html(pend, a["frozen"])}</div></a>')
    h += '</div>'
    b = c.execute('SELECT run_date FROM reports WHERE account_id IS NULL AND grp IS NULL ORDER BY id DESC LIMIT 1').fetchone()
    r = running()
    last = c.execute('SELECT MAX(at) FROM reports').fetchone()[0]
    nxt = subprocess.run(['systemctl', 'show', '-p', 'NextElapseUSecRealtime', '--value', 'stock-analyse.timer'],
                         capture_output=True, text=True).stdout.strip()
    at = datetime.strptime(' '.join(nxt.split()[1:3]), '%Y-%m-%d %H:%M:%S') if nxt else None
    t0 = datetime.fromisoformat(last) if last else None
    span = lambda m: 'just now' if m < 1 else f'{m:.0f} min' if m < 60 else f'{m / 60:.0f} h' if m < 48 * 60 else f'{m / 1440:.0f} days'
    now_ = datetime.now()
    ago = (span((now_ - t0).total_seconds() / 60) + (' ago' if (now_ - t0).total_seconds() >= 60 else '')) if t0 else 'never'
    until = f'in {span((at - now_).total_seconds() / 60)}' if at else 'not scheduled'
    # how far through the wait between the last run and the next one
    done = min(max((now_ - t0) / (at - t0), 0), 1) if t0 and at and at > t0 else 0
    cur = analysis_model(c)
    toggle = ('<form method=post action=/model style="display:inline-flex;align-items:center"><span class=lbl>Model</span>'
              '<span class=seg role=group aria-label="Model for the household analysis">'
              + ''.join(f'<button name=m value={k} aria-pressed={"true" if k == cur else "false"} data-busy="Saving…">{v}</button>'
                        for k, v in MODELS.items()) + '</span></form>')
    h += ('<h1 style="margin-top:28px">Analysis</h1><div class=card><div class=clock>'
          f'<div class=end><span class=lbl>Last run</span><b>{ago}</b>'
          f'<span class=mute>{f"{t0:%-d %b %H:%M}" if t0 else ""}</span></div>'
          f'<div class=lane2 role=img aria-label="{done:.0%} of the way to the next run"><i style="width:{100 * done:.0f}%"></i></div>'
          f'<div class=end><span class=lbl>Next run</span><b>{until}</b>'
          f'<span class=mute>{f"{at:%-d %b %H:%M}" if at else ""}, weekdays</span></div></div>'
          + (f'<div class=ctl>{toggle}'
             + ('<span class=mute>Running now: see the progress bar at the top.</span>' if r else
                '<form method=post action=/run><button data-busy="Starting…">Run analysis now</button></form>')
             + '</div>' if family() else '<p class=mute>Every account is analysed in the same daily run.</p>')
          + '<div class=chips style="margin-top:14px">'
          + (f'<a class="chip hot" href="/brief">Market brief, {date.fromisoformat(b["run_date"]):%-d %b}</a>' if b else
             '<span class=chip style="opacity:.6">Market brief: none yet</span>')
          + '<a class=chip href="/research">Research notes</a><a class=chip href="/how">How it works</a></div></div>')

    cost = c.execute('SELECT COALESCE(SUM(cost_usd),0), COUNT(*) FROM usage WHERE at >= ?', (ny_start(29),)).fetchone()
    h += ('<h1 style="margin-top:28px">Manage</h1><div class=card><div class=chips>'
          '<a class=chip href="/new">+ New sandbox</a>'
          + ('<a class=chip href="/tax?owner=alex">Alex capital gains</a>'
             '<a class=chip href="/tax?owner=sam">Sam capital gains</a>'
             f'<a class=chip href="/usage">Claude usage: {usd(cost[0])}/30d</a>' if family() else '') + '</div></div>')
    h += f'<p class=mute style="margin:24px 0 0">Signed in as {e(user)}.</p>'
    return page('Stock advisor', h)


def bars(stats, labels=None):
    """Rows of average excess return against SPY, centred on zero, with the numbers printed beside each bar.
    stats: {key: (n, average, hit rate)}."""
    if not stats:
        return '<p class=mute>Nothing scored yet.</p>'
    top = max(abs(a) for _, a, _ in stats.values()) or 0.01
    out = ''
    for k, (n, a, hit) in stats.items():
        w = 50 * abs(a) / top
        pos = f'left:50%;width:{w:.1f}%' if a >= 0 else f'left:{50 - w:.1f}%;width:{w:.1f}%'
        out += (f'<div>{e((labels or {}).get(k, str(k)))}</div><div class=b role=img aria-label="{a:+.1%} against the S&amp;P 500">'
                f'<s></s><i class={"up" if a >= 0 else "dn"} style="{pos}"></i></div>'
                f'<div class=num><b class={"up" if a >= 0 else "dn"}>{a:+.1%}</b> <span class=mute>right {hit:.0%} · {n}</span></div>')
    return f'<div class=bars>{out}</div>'


def pct_pts(x):
    return f'<b class={"up" if x >= 0 else "dn"}>{x:+.1%}</b>'


def lens_section(c):
    """Learning: does passing each investor's lens pick stocks that beat the S&P 500, against failing it?"""
    st = learn.signal_summary(c, 'lens')
    first = c.execute('SELECT MIN(day), COUNT(*) FROM signals WHERE kind="lens"').fetchone()
    h = ('<h2 id=lenses>Investor lenses</h2><p class=mute style="margin-top:0">Every stock researched is checked each week '
         'against eight investors\' rules (<a href="/how#lenses">what each checks</a>). If a lens is worth listening to, '
         'stocks that pass it should beat the S&amp;P 500 by more than stocks that fail it.</p>')
    if not st:
        return h + (f'<div class=card><p class=mute style="margin:0">{first[1]} checks recorded since '
                    f'{date.fromisoformat(first[0]):%-d %b}. The first scores come 5 trading days later, on a Saturday.</p></div>'
                    if first[1] else '<div class=card><p class=mute style="margin:0">Recording starts with the next daily run; '
                    'the first scores come 5 trading days after that.</p></div>')
    H = 20 if any(st[k].get(v, {}).get(20, (0,))[0] >= 10 for k in st for v in st[k]) else 5
    cell = lambda x: f'{pct_pts(x[1])}<div class=mute>n={x[0]}</div>' if x else '<span class=mute>–</span>'
    rows, edges = [], {}
    for k, name, idea, _, _ in lenses.LENSES:
        ps, fs = st.get(k, {}).get('pass', {}).get(H), st.get(k, {}).get('fail', {}).get(H)
        if ps and fs:
            edges[k] = ps[1] - fs[1]
        rows.append((k, name, idea, ps, fs))
    top = max([abs(x) for x in edges.values()] or [0.01]) or 0.01
    def edge(k):
        if k not in edges:
            return '<span class=mute>needs both</span>'
        x, w = edges[k], 50 * abs(edges[k]) / top
        return (f'<span class=edge role=img aria-label="{x:+.1%}"><i class={"up" if x >= 0 else "dn"} style="'
                f'{"left:50%" if x >= 0 else f"left:{50 - w:.1f}%"};width:{w:.1f}%"></i></span>{pct_pts(x)}')
    return h + ('<div class="card scroll"><table class="grid lenses"><tr><th>Lens</th><th>Passed</th><th>Failed</th>'
                '<th>Passed minus failed</th></tr>'
                + ''.join(f'<tr><td><b>{e(name)}</b><div class="mute idea">{e(idea)}</div></td><td>{cell(ps)}</td><td>{cell(fs)}</td>'
                          f'<td>{edge(k)}</td></tr>' for k, name, idea, ps, fs in rows) +
                f'</table><p class=mute style="margin:8px 0 0">Average return against the S&amp;P 500 over {H} trading days after '
                'the check; n counts stock-weeks. A wide gap to the right means the lens separated winners from losers. '
                '"Mixed" results are left out.</p></div>')


def pattern_section(c):
    """Learning: each candlestick and chart pattern's record, and whether it has earned a say in the advice."""
    st = learn.signal_summary(c, 'pattern')
    status = learn.statuses(c)
    recent = c.execute('SELECT day, symbol, name FROM signals WHERE kind="pattern" ORDER BY day DESC, symbol LIMIT 12').fetchall()
    h = ('<h2 id=patterns>Patterns on trial</h2><p class=mute style="margin-top:0">Candlesticks and chart shapes have weak '
         'support in research, so none counts as evidence until it proves itself here. A pattern is <b>proven</b> after '
         f'{learn.PROVE_N} cases that beat the S&amp;P 500 in its own direction by {learn.PROVE_EDGE:.0%} or more on average over '
         f'{learn.PROVE_H} trading days, right {learn.PROVE_HIT:.0%} of the time; it has <b>failed</b> with {learn.PROVE_N} cases '
         'and no edge. Until then the advisor must set it aside.</p>')
    def card(k):
        name, side, what = lenses.PATTERNS[k]
        x = next(iter(st.get(k, {}).values()), {})
        n, avg, hit = x.get(learn.PROVE_H) or x.get(5) or (0, 0, 0)
        hz = learn.PROVE_H if x.get(learn.PROVE_H) else 5
        seen = c.execute('SELECT COUNT(*) FROM signals WHERE kind="pattern" AND name=?', (k,)).fetchone()[0]
        stt = status.get(k, 'on trial')
        scored = x.get(learn.PROVE_H, (0,))[0]
        return (f'<div class="card pat {stt.replace(" ", "-")}"><div class=row style="justify-content:space-between;gap:6px">'
                f'<b>{e(name)}</b><span class="pill {stt.replace(" ", "-")}">{stt}</span></div>'
                f'<div class="side {side}">{side}</div><p class=mute style="margin:4px 0 8px">{e(what)}</p>'
                f'<div class=track role=img aria-label="{scored} of {learn.PROVE_N} cases scored at {learn.PROVE_H} days">'
                f'<i style="width:{min(100, 100 * scored / learn.PROVE_N):.0f}%"></i></div>'
                + (f'<div class=mute>{scored} of {learn.PROVE_N} cases scored at {learn.PROVE_H} days' if scored < learn.PROVE_N else
                   f'<div class=mute>{scored} cases scored at {learn.PROVE_H} days ({learn.PROVE_N} needed)')
                + (f', seen {seen} time{"s" if seen != 1 else ""}</div>' if seen else ', not seen yet</div>')
                + (f'<div>{hz} days: {pct_pts(avg)} against the S&amp;P 500, right {hit:.0%} (n={n})</div>' if n else '') + '</div>')
    h += ('<h3>Candlesticks</h3><div class=pats>' + ''.join(card(k) for k in lenses.CANDLES) + '</div>'
          '<h3>Chart shapes</h3><div class=pats>' + ''.join(card(k) for k in lenses.PATTERNS if k not in lenses.CANDLES) + '</div>')
    if recent:
        h += ('<details><summary>Latest sightings</summary><div class=scroll><table><tr><th>Day</th><th>Stock</th><th>Pattern</th></tr>'
              + ''.join(f'<tr><td>{r["day"]}</td><td>{e(r["symbol"])}</td><td>{e(lenses.PATTERNS.get(r["name"], (r["name"],))[0])}</td></tr>'
                        for r in recent) + '</table></div></details>')
    return h


def learn_page(c, err=None):
    ps = learn.rows(c)
    scored = [p for p in ps if p['scores']]
    h = ('<p><a href="/">← all accounts</a></p><h1>Learning</h1>'
         '<p class=mute style="margin-top:0">How the advisor\'s proposals turned out, and what it takes from that. The app '
         'scores every proposal against the S&amp;P 500 each Saturday; once a month Opus reviews the scores and your '
         'decisions and suggests lessons. A lesson changes the advice only after you keep it. '
         '<a href="#lenses">Investor lenses</a> and <a href="#patterns">patterns on trial</a> are scored the same way.</p>')
    if err:
        h += f'<p class=err>{e(err)}</p>'

    h += ('<h2>Can the advisor beat your portfolio?</h2>' if family() else '<h2>Family: can the advisor beat their portfolio?</h2>') + '<div class=card>'
    days, vals, spy = bench_series(c)
    if len(days) >= 2:
        b = bench_text(c)
        gap = (b['advisor'] - b['fixed']) * 100
        h += (f'<p class=headline>The advisor is {"ahead of" if gap >= 0 else "behind"} your fixed portfolio by '
              f'{abs(gap):.1f} point{"s" if abs(gap) >= 1.05 or abs(gap) < 0.95 else ""} since '
              f'{date.fromisoformat(days[0]):%-d %b}.</p>'
              + (f'<p class=headline style="margin-top:0">Your LIVE accounts, where you have been following the advice '
                 f'since {date.fromisoformat(b["live_follows_advice_since"]):%-d %b}, are '
                 f'{abs(lv := (b["live"] - b["fixed"]) * 100):.1f} points {"ahead of" if lv >= 0 else "behind"} the fixed portfolio, '
                 f'and {abs(la := (b["live"] - b["advisor"]) * 100):.1f} points {"ahead of" if la >= 0 else "behind"} the advisor.</p>'
                 if b['live_follows_advice_since'] else '') +
              f'<p class=mute style="margin:0">Advisor {b["advisor"]:+.2%} · fixed {b["fixed"]:+.2%} · LIVE '
              f'{b["live"]:+.2%} · S&amp;P 500 {b["spy"]:+.2%}</p>'
              + return_chart(days, [(name, short, col, vals[g]) for g, name, short, col in BENCH] +
                             [('S&P 500 (SPY)', 'S&P', '--mute', spy)]) +
              '<p class=mute>The advisor and the fixed portfolio both started from the LIVE holdings on '
              f'{date.fromisoformat(days[0]):%-d %b}, and neither ever gets new money, so they compare directly. '
              'LIVE started from the same holdings, and since you began acting on the advice there, LIVE against the advisor '
              'shows what your choices of which proposals to take added or cost. LIVE also moves with any deposit or '
              'withdrawal: a sudden jump there is money, not performance.</p>')
    else:
        h += '<p class=mute>Appears after two days of values for the advisor, the fixed portfolio and LIVE.</p>'
    h += '</div>'
    for p in profiles(c):
        days, vals, spy = profile_series(c, p) if p != 'family' else ([], {}, [])
        if not vals:
            continue
        h += f'<h2>{e(p.title())}: can the advisor beat the fixed portfolio?</h2><div class=card>'
        if len(days) >= 2:
            r = {k: v[-1] / v[0] - 1 for k, v in vals.items()}
            gap = (r['advisor'] - r['fixed']) * 100
            h += (f'<p class=headline>The advisor is {"ahead of" if gap >= 0 else "behind"} {e(p.title())}\'s fixed portfolio by '
                  f'{abs(gap):.1f} point{"s" if abs(gap) >= 1.05 or abs(gap) < 0.95 else ""} since {date.fromisoformat(days[0]):%-d %b}.</p>'
                  f'<p class=mute style="margin:0">Advisor {r["advisor"]:+.2%} · fixed {r["fixed"]:+.2%} · S&amp;P 500 '
                  f'{spy[-1] / spy[0] - 1:+.2%}</p>'
                  + return_chart(days, [('Advisor (sandbox)', 'Advisor', '--sand', vals['advisor']),
                                        ('Fixed portfolio', 'Fixed', '--frozen', vals['fixed']), ('S&P 500 (SPY)', 'S&P', '--mute', spy)]) +
                  '<p class=mute>The sandbox follows the advice; the fixed portfolio changes only by hand. Trades entered by hand '
                  'count in both; cash top-ups do not count as gains.</p>')
        else:
            h += '<p class=mute>Appears after two days of values for both the sandbox and the fixed portfolio (one is recorded at each daily analysis).</p>'
        h += '</div>'

    waiting = c.execute('SELECT * FROM lessons WHERE status="proposed" ORDER BY id').fetchall()
    if waiting:
        h += f'<h2>Lessons waiting for you ({len(waiting)})</h2>'
        for l in waiting:
            h += (f'<div class="card lesson proposed"><span class=tag style="background:var(--ask)">'
                  f'{e(LESSON_KINDS.get(l["kind"], l["kind"]))}</span><p style="margin:8px 0 4px;font-weight:600">{e(l["text"])}</p>'
                  f'<p class=mute style="margin:0 0 10px">From {l["n"]} proposals: {e(l["evidence"] or "")}</p>'
                  + (f'<div class=row><form method=post action="/lesson/{l["id"]}/keep"><button class="ok" '
                     f'style="background:var(--ask)">Keep, use in advice</button></form>'
                     f'<form method=post action="/lesson/{l["id"]}/discard"><button class=no>Discard</button></form></div>'
                     if family() else '') + '</div>')

    active = c.execute('SELECT * FROM lessons WHERE status="active" ORDER BY id').fetchall()
    h += f'<h2>What the advisor has learned</h2><p class=mute style="margin-top:0">{len(active)} of {MAX_ACTIVE_LESSONS} lessons in use.</p><div class=lanes>'
    for kind, title in LESSON_KINDS.items():
        mine = [l for l in active if l['kind'] == kind]
        items = ''.join(
            f'<li>{e(l["text"])}<div class="mute row" style="gap:4px 10px">Kept {date.fromisoformat(l["decided_at"][:10]):%-d %b} · '
            f'from {l["n"]} proposals' + (f'<form method=post action="/lesson/{l["id"]}/retire"><button class=no '
            f'style="min-height:36px;padding:4px 10px;font-size:13px">Retire</button></form>' if family() else '')
            + '</div></li>' for l in mine)
        h += (f'<div class="card lane {kind}"><h3>{title}</h3>'
              + (f'<ul>{items}</ul>' if items else '<p class=mute style="margin:0">Nothing yet.</p>') + '</div>')
    h += '</div>'

    h += '<h2>How the advice is doing</h2><div class=card>'
    if not scored:
        h += ('<p class=mute style="margin:0">Nothing scored yet. A proposal is first scored 5 trading days after it was made; '
              'the scores run every Saturday.</p>')
    else:
        cnt = {k: learn.summary(scored, k, lambda p: 0).get(0, (0,))[0] for k in learn.HORIZONS}
        H = max([k for k in learn.HORIZONS if cnt[k] >= 10] or [5])
        h += ('<h3 style="margin-top:0">Against the S&amp;P 500, by how long the stock was held</h3>'
              + bars({k: v for k in learn.HORIZONS if (v := learn.summary(scored, k, lambda p: 0).get(0))},
                     {k: f'{k} days' for k in learn.HORIZONS}) +
              f'<p class=mute>Below: {H}-trading-day results. Right = share of proposals that beat the S&amp;P 500; '
              'the last number is how many.' + (' Fewer than 10 per group so far: read these as early signs only.' if cnt[H] < 10 else '') + '</p>'
              '<h3>By the advisor\'s confidence</h3>' + bars(learn.summary(scored, H, lambda p: p['confidence'] or 'unknown')) +
              '<h3>Buys and sells</h3>' + bars(learn.summary(scored, H, lambda p: p['side']), {'BUY': 'Buys', 'SELL': 'Sells'}) +
              '<h3>By your decision</h3>' + bars(learn.summary(scored, H, lambda p: p['cls']),
                                                 {'taken': 'Approved', 'rejected': 'Rejected', 'ignored': 'Left to expire',
                                                  'hands-off': 'LIVE, hands off'}) +
              '<h3>Approved, LIVE against sandbox</h3>' + bars(learn.summary([p for p in scored if p['cls'] == 'taken'], H,
                                                                       lambda p: 'LIVE' if p['grp'] == 'family' else 'Sandbox')) +
              '<h3>Made on a policy-shock day</h3>' + bars(learn.summary([p for p in scored if p['policy']], H, lambda p: p['policy']),
                                                          {'none': 'Calm days', 'minor': 'Minor policy shock', 'major': 'Major policy shock'}) +
              '<p class=mute>As the morning brief judged the day: a major shock is a policy announcement that moved the S&amp;P 500 '
              'or a whole sector by 2% or more. Proposals from before 6 Oct 2026 are not tagged.</p>' +
              '<p class=mute style="margin:0">"LIVE, hands off": the LIVE proposals of 24-26 Sep, left alone on purpose. They '
              'count for how good the advice was, never for what you prefer.</p>')
        H2 = 20 if any(20 in p['scores'] for p in scored) else H
        ranked = sorted([p for p in scored if H2 in p['scores']], key=lambda p: p['scores'][H2])
        pick = lambda p: (f'<div class="pick {"up" if p["scores"][H2] >= 0 else "dn"}"><b>{e(p["side"])} {e(p["symbol"])}</b> '
                          f'<span class=mute>{e(p["run_date"])} · {e(p["grp"])} · {e(p["cls"])}</span> '
                          f'<b class={"up" if p["scores"][H2] >= 0 else "dn"}>{p["scores"][H2]:+.1%}</b>'
                          f'<q>{e((p["reason"] or "")[:220])}{"…" if len(p["reason"] or "") > 220 else ""}</q></div>')
        h += (f'</div><h2>Biggest misses and best calls</h2><p class=mute style="margin-top:0">Against the S&amp;P 500 over '
              f'{H2} trading days, with the reason the advisor gave at the time.</p><div class=lanes><div class=card><h3>Misses</h3>'
              + ''.join(pick(p) for p in ranked[:5] if p['scores'][H2] < 0) + '</div><div class=card><h3>Best calls</h3>'
              + ''.join(pick(p) for p in ranked[::-1][:5] if p['scores'][H2] >= 0) + '</div></div><div>')
    vs = learn.verdict_summary(c)
    if vs:
        hv = max([k for k in learn.HORIZONS if sum(hs.get(k, (0,))[0] for hs in vs.values()) >= 30] or [5])
        # Bars read "right" for every verdict: a SELL or TRIM is right when the stock lags the S&P 500.
        flip = lambda v, x: (x[0], -x[1], 1 - x[2]) if v in learn.BEARISH else x
        order = ['BUY', 'ADD', 'HOLD', 'WATCH', 'TRIM', 'SELL']
        h += (f'<h3>Every verdict on the analysis cards, after {hv} trading days</h3>'
              + bars({v: flip(v, vs[v][hv]) for v in order if v in vs and hv in vs[v]},
                     {'BUY': 'Buy', 'ADD': 'Add', 'HOLD': 'Hold', 'WATCH': 'Watch', 'TRIM': 'Trim (reversed)', 'SELL': 'Sell (reversed)'}) +
              '<p class=mute>Not just the trades: every card the analysis writes, held stocks and opportunities. Trim and Sell '
              'are reversed, so a bar to the right always means the verdict was right. Hold and Watch simply show how those '
              'stocks did against the S&amp;P 500.</p>')
    ch = learn.churn(ps)
    if ch:
        h += ('<h3>Changed its mind within about 20 trading days</h3><ul>' +
              ''.join(f'<li>{e(a["symbol"])} ({e(a["grp"])}): {e(a["side"])} {e(a["run_date"])}, then {e(b["side"])} {e(b["run_date"])}</li>'
                      for a, b in ch) + '</ul>')
    h += '</div>'

    h += lens_section(c) + pattern_section(c)
    rv = c.execute('SELECT * FROM reports WHERE grp="review" ORDER BY id DESC LIMIT 1').fetchone()
    at = (c.execute('SELECT v FROM meta WHERE k="scored_at"').fetchone() or [None])[0]
    busy = subprocess.run(['systemctl', 'is-active', 'stock-review-now.service'], capture_output=True, text=True).stdout.strip() in ('active', 'activating')
    h += ('<h2>Monthly review</h2><div class="card prose">' + (md(rv['body']) if rv else
          '<p class=mute>No review yet. The first runs on the first Saturday of the month.</p>') + '</div>'
          f'<p class=mute>Scores last updated {e(when(at))}. {f"Review written {e(when(rv["at"]))}." if rv else ""}</p>'
          + ('' if not family() else '<p class=mute>A review is running; reload in a few minutes.</p>' if busy else
             '<form method=post action="/learn/review" onsubmit="return confirm(\'Run a review with Opus now? About $1.\')">'
             '<button class=no data-busy="Starting…">Run review now</button></form>'))
    return page('Learning', h)


def friend_lots(c, prof):
    """(lots, dividends paid) for a friend's profile, shaped like the tax register's: one parcel per holding of each
    sandbox or frozen account, the account's name as the owner, and the dividends credited to its cash, gross.
    ponytail: a holding's parcel is dated by its first buy (or the account's start), so a stock sold out and bought
    again counts from the first time; track parcels per fill if that matters."""
    lots, pays = [], []
    for a in c.execute('SELECT * FROM accounts WHERE profile=? AND mode="SANDBOX" ORDER BY frozen, id', (prof,)).fetchall():
        for p in c.execute('SELECT * FROM positions WHERE account_id=?', (a['id'],)):
            first = c.execute('SELECT MIN(decided_at) FROM proposals WHERE account_id=? AND symbol=? AND side="BUY" '
                              'AND status IN ("filled","manual")', (a['id'], p['symbol'])).fetchone()[0]
            lots.append({'id': f'{a["id"]}:{p["symbol"]}', 'owner': a['name'], 'symbol': p['symbol'], 'currency': 'USD',
                         'buy_date': (first or a['created'])[:10], 'buy_qty': p['qty'], 'buy_price': p['avg_price'],
                         'buy_fee': 0, 'sale_date': None, 'sale_qty': None})
        pays += [{'owner': a['name'], 'symbol': r['symbol'], 'day': r['day'], 'qty': r['qty'], 'per_share': r['per_share'],
                  'amount': r['qty'] * r['per_share'], 'currency': 'USD'}
                 for r in c.execute('SELECT * FROM sandbox_divs WHERE account_id=?', (a['id'],))]
    return lots, pays


def dividends_page(c, prof='family'):
    today = date.today().isoformat()
    if prof == 'family':
        lots, pays = [dict(r) for r in c.execute('SELECT * FROM lots ORDER BY buy_date')], None
    else:
        lots, pays = friend_lots(c, prof)
    syms = sorted({l['symbol'] for l in lots} | {p['symbol'] for p in pays or []})
    H = dividends.histories(c, syms)
    C = dividends.histories(c, [s for s, _ in dividends.COMPARE], daily=True)
    divs = {s: h['divs'] for s, h in H.items()}
    try:
        rate = aud_per_usd()
    except Exception:
        rate = None
    cur_of = {l['symbol']: l['currency'] for l in lots} | {p['symbol']: p['currency'] for p in pays or []}
    to_usd = lambda amt, cur: amt if cur == 'USD' else (amt / rate if rate else 0)
    pays = dividends.received(lots, divs) if pays is None else pays
    for p in pays:
        p['usd'] = to_usd(p['amount'], p['currency'])
    owners = sorted({l['owner'] for l in lots} | {p['owner'] for p in pays})
    col = {o: ('--s1', '--s2', '--sand', '--frozen')[i % 4] for i, o in enumerate(owners)}
    total = lambda f: sum(p['usd'] for p in pays if f(p))
    fy_now = dividends.fy(today)
    open_pos = {}
    for l in lots:
        if not l['sale_date']:
            o = open_pos.setdefault((l['owner'], l['symbol']), {'qty': 0, 'cost': 0, 'lots': []})
            o['qty'] += l['buy_qty']
            o['cost'] += l['buy_qty'] * l['buy_price'] + (l['buy_fee'] or 0)
            o['lots'].append(l)
    fwd = sum(to_usd(o['qty'] * dividends.ttm(divs.get(sym, []), today), cur_of[sym]) for (_, sym), o in open_pos.items())

    h = '<p><a href="/">← all accounts</a></p><h1>Dividends</h1>'
    h += ('<p class=mute style="margin-top:0">The dividends paid into your sandbox and frozen accounts\' cash, shown gross '
          '(before the withholding tax taken when they are paid in), dated by ex-dividend date, and what your holdings '
          'should pay next.</p>' if prof != 'family' else
          '<p class=mute style="margin-top:0">Worked out from the tax register\'s parcels (who held what, and when) and each '
         'company\'s published dividends. Gross amounts, before withholding tax (15% on US companies with a W-8BEN), dated by '
         'ex-dividend date; the cash arrives a few weeks later. LIVE accounts only: sandboxes are paid theirs into cash on their own pages.'
         + (f' Australian-dollar dividends are converted at today\'s A${rate:.4f} per US$.'
            if rate and any(v != 'USD' for v in cur_of.values()) else '') + '</p>')
    if not lots and not pays:
        return page('Dividends', h + '<p class=mute>Nothing yet: buy some shares in a sandbox or frozen account first.</p>')
    tile = lambda label, v, sub='': f'<div class=card><div class=mute>{label}</div><div class=big>{usd(v)}</div>{sub}</div>'
    per = lambda f: ('<div class=mute>' + ' · '.join(f'{e(o.title())} {usd(total(lambda p, o=o: p["owner"] == o and f(p)))}'
                                                     for o in owners) + '</div>')
    this_year = lambda p: p['day'][:4] == today[:4]
    this_fy = lambda p: dividends.fy(p['day']) == fy_now
    h += ('<div class=tiles>' + tile('All time', total(lambda p: True), per(lambda p: True))
          + tile(f'{today[:4]} so far', total(this_year), per(this_year))
          + tile(f'Financial year {fy_now} so far', total(this_fy), per(this_fy))
          + tile('Next 12 months, if unchanged', fwd, '<div class=mute>shares held now × the last year\'s dividends</div>')
          + '</div>')

    years = sorted({p['day'][:4] for p in pays})
    if years:
        by = {y: {o: total(lambda p, y=y, o=o: p['day'][:4] == y and p['owner'] == o) for o in owners} for y in years}
        top = max(sum(v.values()) for v in by.values()) or 1
        h += ('<h2>By year</h2><div class=card><div class=mute style="margin-bottom:8px">'
              + ''.join(f'<span class=key><i style="background:var({col[o]})"></i>{e(o.title())}</span>' for o in owners)
              + '</div><div class=ybars>'
              + ''.join(f'<div>{y}{" so far" if y == today[:4] else ""}</div>'
                        f'<div class=b role=img aria-label="{y}: {usd(sum(v.values()))}">'
                        + ''.join(f'<i style="width:{100 * v[o] / top:.1f}%;background:var({col[o]})"></i>' for o in owners if v[o])
                        + f'</div><div><b>{usd(sum(v.values()))}</b></div>' for y, v in by.items()) + '</div></div>')

    fv = feeds.quote_pages(c, [sym for (_, sym) in open_pos if cur_of[sym] == 'USD'])
    def announced(sym):
        try:
            return datetime.strptime((fv.get(sym) or {}).get('Dividend Ex-Date', ''), '%b %d, %Y').date().isoformat()
        except ValueError:
            return None
    up = []
    for (owner, sym), o in open_pos.items():
        n = dividends.next_ex(divs.get(sym, []), announced(sym), today)
        if n:
            up.append((n[0], owner, sym, o['qty'], n[1], n[2], cur_of[sym]))
    h += '<h2>Coming up</h2>'
    if up:
        h += ('<div class=card><div class=scroll><table><tr><th>Ex-date</th><th style="text-align:left">Holding</th><th>Shares</th><th>Per share</th><th>Expected</th></tr>'
              + ''.join(f'<tr><td>{date.fromisoformat(d):%a %-d %b}<div><span class="st {st}">{st}</span></div></td>'
                        f'<td style="text-align:left">{e(sym)}<div class=mute>{e(o.title())}</div></td><td>{q:g}</td>'
                        f'<td>{usd(a) if cur == "USD" else f"A${a:,.2f}"}</td><td><b>{usd(to_usd(q * a, cur))}</b></td></tr>'
                        for d, o, sym, q, a, st, cur in sorted(up))
              + '</table></div><p class=mute style="margin:8px 0 0">Own the shares before the ex-date to receive the dividend. '
              '"Announced": the company has declared it. "Expected": projected from its usual pattern and last amount.</p></div>')
    else:
        h += '<p class=mute>No ex-dividend dates expected in the next 75 days.</p>'

    bench = C.get(dividends.BENCH)
    rows = []
    for owner, sym in sorted({(p['owner'], p['symbol']) for p in pays} | set(open_pos)):
        d, o, price = divs.get(sym, []), open_pos.get((owner, sym)), (H.get(sym) or {}).get('price')
        got = total(lambda p: p['owner'] == owner and p['symbol'] == sym)
        t = dividends.ttm(d, today)
        if not got and not t:
            continue
        ret = b = None
        if o and price and o['cost']:
            held = {l['id'] for l in o['lots']}
            lot_divs = sum(p['amount'] for p in dividends.received([l for l in lots if l['id'] in held], divs))
            ret = (price * o['qty'] + lot_divs) / o['cost'] - 1
            if bench:
                bs = [(l['buy_qty'] * l['buy_price'], dividends.total_return(bench['closes'], bench['divs'], l['buy_date']))
                      for l in o['lots']]
                bs = [(w, r) for w, r in bs if r is not None]
                b = sum(w * r for w, r in bs) / sum(w for w, _ in bs) if bs else None
        rows.append({'owner': owner, 'sym': sym, 'qty': o['qty'] if o else 0, 'yield': t / price if price and o else None,
                     'yoc': t * o['qty'] / o['cost'] if o and o['cost'] else None, 'got': got, 'g5': dividends.growth(d, today),
                     'ret': ret, 'bench': b, 'cur': cur_of[sym]})
    pct = lambda x: '–' if x is None else f'{x:.1%}'
    sgn = lambda x: '–' if x is None else f'<span class={"up" if x >= 0 else "dn"}>{x:+.1%}</span>'
    h += ('<h2>Your dividend payers</h2><div class=card><div class=scroll><table class=grid><tr><th>Holding</th><th>Yield now</th>'
          '<th>Yield on cost</th><th>Received</th><th>Dividend growth<br>per year, 5-yr average</th><th>Return<br>since bought</th>'
          '<th>SCHD<br>same dates</th></tr>'
          + ''.join(f'<tr><td><b>{e(r["sym"])}</b> <span class=mute>{e(r["owner"].title())}{"" if r["qty"] else ", sold"}</span></td>'
                    f'<td>{pct(r["yield"])}</td><td>{pct(r["yoc"])}</td><td>{usd(r["got"])}</td><td>{sgn(r["g5"])}</td>'
                    f'<td>{sgn(r["ret"])}</td><td>{sgn(r["bench"])}</td></tr>'
                    for r in sorted(rows, key=lambda r: (-(r['yield'] or 0), -r['got'])))
          + '</table></div><p class=mute style="margin:8px 0 0">Yield now: the last year\'s dividends over today\'s price. Yield on cost: '
          'the same dividends over what you paid. Return since bought: price change plus dividends received, for the shares still '
          'held. SCHD, same dates: what the same money in the Schwab US dividend ETF would have returned from the same purchase '
          'dates.</p></div>')

    # The family's dividend picks as a group, against the dividend funds and the long-running dividend growers.
    picks = [r for r in rows if r['qty'] and (r['yield'] or 0) >= dividends.INCOME_YIELD and r['cur'] == 'USD'
             and r['sym'] not in dividends.CASH_LIKE]
    pos = lambda r: open_pos[(r['owner'], r['sym'])]
    # Yield and dividend growth describe the portfolio as it is now: weighted by value today. Returns are weighted by
    # what was paid, so a holding that has risen does not count for more in its own return.
    val = lambda r: pos(r)['qty'] * (H[r['sym']]['price'] or 0)
    def wavg(k, w):
        xs = [(w(r), r[k]) for r in picks if r[k] is not None]
        return sum(a * x for a, x in xs) / sum(a for a, _ in xs) if xs and sum(a for a, _ in xs) else None
    lots_held = [l for r in picks for l in pos(r)['lots']]
    def same_dates(x):
        """What the money in the picks would have returned in x, bought on the same dates."""
        xs = [(l['buy_qty'] * l['buy_price'], dividends.total_return(x['closes'], x['divs'], l['buy_date'])) for l in lots_held]
        xs = [(w, r) for w, r in xs if r is not None]
        return sum(w * r for w, r in xs) / sum(w for w, _ in xs) if xs else None
    ago = lambda n: (date.fromisoformat(today) - timedelta(days=365 * n)).isoformat()
    cmp_rows = ''.join(
        f'<tr><td><b>{sym}</b> <span class=mute>{e(name)}</span></td><td>{pct(dividends.ttm(x["divs"], today) / x["price"])}</td>'
        f'<td>{sgn(dividends.growth(x["divs"], today))}</td>' + (f'<td>{sgn(same_dates(x))}</td>' if picks else '')
        + f'<td>{sgn(dividends.total_return(x["closes"], x["divs"], ago(1)))}</td>'
        f'<td>{sgn(dividends.total_return(x["closes"], x["divs"], ago(5)))}</td></tr>'
        for sym, name in dividends.COMPARE if (x := C.get(sym)) and x['price'])
    head = ('<tr><th></th><th>Yield now</th><th>Dividend growth<br>per year, 5-yr average</th>'
            + ('<th>Return<br>on your dates</th>' if picks else '') + '<th>Return<br>last year</th><th>Return<br>last 5 years</th></tr>')
    h += '<h2>How your picks compare</h2>'
    if picks:
        r_you, r_b = wavg('ret', lambda r: pos(r)['cost']), (same_dates(C[dividends.BENCH]) if dividends.BENCH in C else None)
        verdict = ('' if r_you is None or r_b is None else
                   f'<p class=headline>Your dividend picks have returned {r_you:+.1%} since you bought them. The same money in '
                   f'SCHD would have returned {r_b:+.1%}.</p>')
        h += (f'<div class=card>{verdict}<p class=mute style="margin-top:0">Your dividend picks: the {len(picks)} US holdings '
              f'you still hold that yield {dividends.INCOME_YIELD:.1%} or more ({e(", ".join(sorted({r["sym"] for r in picks})))}). '
              f'Sold holdings and T-bill funds (cash parked for interest) are not included.</p><div class=scroll><table class=grid>{head}'
              f'<tr class=you><td>Your dividend picks</td><td>{pct(wavg("yield", val))}</td><td>{sgn(wavg("g5", val))}</td>'
              f'<td>{sgn(r_you)}</td><td>–</td><td>–</td></tr>{cmp_rows}</table></div>'
              '<p class=mute style="margin:8px 0 0">Dividend growth: how fast the company has raised its dividend per share, '
              'on average each year over the last 5 years. Return on your dates: price change plus dividends, from the days you '
              'bought your picks to today; for the funds and companies, the same money bought on those same days. Your picks '
              'have no last-year or 5-year return because you bought them at different times. Yield and dividend growth are '
              'weighted by value today; returns by what you paid. Returns include dividends, not reinvested. The first five '
              'are the usual US dividend funds; the companies below them have raised their dividends for 25 years or more.</p></div>')
    else:
        h += (f'<div class=card><p class=mute style="margin-top:0">None of your US holdings yields {dividends.INCOME_YIELD:.1%} or '
              f'more yet. For reference:</p><div class=scroll><table class=grid>{head}{cmp_rows}</table></div></div>')
    return page('Dividends', h)


def account_page(c, a, err=None):
    s = state(c, a)
    live = a['mode'] == 'LIVE'
    sibs = members(c, a)
    # The other accounts of the household, one tap away; the current one is marked, not linked.
    switch = ('<span class=seg role=navigation aria-label="Accounts in this household">'
              + ''.join(f'<a aria-current=page>{e(m["name"])}</a>' if m['id'] == a['id'] else
                        f'<a href="/a/{m["id"]}">{e(m["name"])}</a>' for m in sibs) + '</span>') if len(sibs) > 1 else ''
    h = (ibkr_alert(c) if live else '') + (f'<div class=row style="margin:12px 0"><a href="/">← all accounts</a>{switch}</div>'
         f'<h1><span class="tag {look(a)}">{look(a)}</span> {e(a["name"])}</h1>')
    if a['frozen']:
        h += ('<p class=note>Fixed benchmark: the positions and cash it was given, held without advice and never '
              'analysed; only trades entered by hand below change it. The <a href="/learn">Learning</a> page compares the advisor with it.</p>')
    if err:
        h += f'<p class=err>{e(err)}</p>'
    if s:
        rate = s['aud_per_usd']
        first = c.execute('SELECT value_usd, spy, day FROM snapshots WHERE account_id=? ORDER BY day LIMIT 1',
                          (a['id'],)).fetchone()
        h += f'<div class=card><div class=mute>Total value</div><div class=big>{both(s["value"], rate)}</div>'
        # Cash in the table; for LIVE the part already in US dollars (the rest is A$, converted).
        h += stats_html(c, [a], s['positions'], day_change(s['positions'], today_fills(c, [a['id']]))[1],
                        cash=(s['cash'], f'(US$ {usd(s.get("cash_usd_only"))[:-3]})' if live and s.get('cash_usd_only') is not None else ''))
        if first:
            try:
                you, spy = s['value'] / first['value_usd'] - 1, quote('SPY') / first['spy'] - 1
                h += (f'<div class=mute style="margin-top:6px">Since {date.fromisoformat(first["day"]):%-d %b} the account '
                      f'is {pnl(s["value"] - first["value_usd"])} ({you:+.2%}); the S&amp;P 500 {spy:+.2%}.</div>')
            except Exception:
                pass
        h += f'<div class=mute>Prices as of {e(when(s["at"]))}</div>' + value_chart(c, a, s) + '</div>'
        if live and s['at'] and (datetime.now() - datetime.fromisoformat(s['at'])).total_seconds() > 86400:
            h += '<p class=err>These IBKR numbers are more than a day old. Refresh from IBKR below.</p>'
    else:
        h += '<p class=mute>Not loaded from IBKR yet.</p>'
    if live:
        h += (f'<form method=post action="/a/{a["id"]}/refresh"><button data-busy="Refreshing from IBKR… about 20 s">'
              'Refresh from IBKR</button> <span class=mute>takes about 20 s</span></form>')
    ms = members(c, a)
    together = len(ms) > 1
    if together and not a['frozen']:
        others = ', '.join(e(m['name']) for m in ms if m['id'] != a['id'])
        h += (f'<p class=note>Analysed as one investment with {others}. Proposals for every account in it are '
              'listed here, each marked with the account it goes in.</p>')
    ids = [m['id'] for m in ms]
    pend = c.execute(f'SELECT p.*, a.name AS account FROM proposals p JOIN accounts a ON a.id=p.account_id '
                     f'WHERE p.account_id IN ({",".join("?" * len(ids))}) AND p.status="pending" ORDER BY p.id', ids).fetchall()
    h += drafts_html(c, a['id']) if live else ''
    if not a['frozen']:
        h += f'<h2>Awaiting your approval ({len(pend)})</h2>'
    if not pend and not a['frozen']:
        h += '<p class=mute>Nothing to decide.</p>'
    for p in pend:
        est = p['qty'] * (p['limit_price'] or p['ref_price'] or 0)
        what = f'{p["side"]} {p["qty"]:g} {p["symbol"]}'
        lim = f' limit {usd(p["limit_price"])}' if p['order_type'] == 'LIMIT' else ' at market'
        act = 'creates an IBKR draft you then submit in the IBKR app' if live else 'fills now at the current price'
        conf = f'{a["mode"]} — {p["account"].upper()}\'S ACCOUNT: {what}{lim}, about {usd(est)}. This {act}. Continue?'
        where = f'<div><span class="tag {a["mode"]}">in {e(p["account"])}\'s account</span></div>' if together else ''
        h += (f'<div class="card {a["mode"]}">{where}<div class=big>{e(what)}</div>'
              f'<div>{"Limit " + usd(p["limit_price"]) if p["order_type"] == "LIMIT" else "Market order"} · ~{usd(est)} · ref {usd(p["ref_price"])} · '
              f'confidence {e(p["confidence"] or "")} · proposed {e(p["run_date"])}</div>'
              + (f'<p class=err>{e(p["note"])}</p>' if p['note'] else '') +
              f'<p class=md>{e(p["reason"] or "")}</p>'
              + (f'<p class=plan><b>Exit plan:</b> {e(plan_line(json.loads(p["plan"])))}</p>' if p['plan'] else '') +
              f'<div class=row><form method=post action="/p/{p["id"]}/approve" '
              f'onsubmit="return confirm({e(json.dumps(conf))})"><button class="ok {a["mode"]}" '
              f'data-busy="{"Creating IBKR draft… up to 30 s" if live else "Filling…"}">'
              f'{"Approve — draft in IBKR" if live else "Approve — fill in sandbox"}</button></form>'
              f'<form method=post action="/p/{p["id"]}/reject" class=row><button class=no data-busy="Rejecting…">Reject</button>'
              f'<select name=reason aria-label="Why reject (optional)"><option value="">Why? (optional)</option>'
              + ''.join(f'<option>{r}</option>' for r in REJECT_REASONS) + '</select></form></div></div>')
    if s and s['positions']:
        days, total_day = day_change(s['positions'], today_fills(c, [a['id']]))
        rows = ''
        for r in sorted(s['positions'], key=lambda r: -(r['value'] or 0)):
            day, pct = days.get(r['symbol'], (None, None))
            tp = r['pnl'] / (r['avg'] * r['qty']) if r['pnl'] is not None and r['avg'] else None
            # Amount and percentage in their own columns, each lined up; the tint spans both, so they read as a pair.
            pair = lambda amt, x, fmt, full: (f'<td class=amt{heat(x, full)}>{pnl(amt) if amt is not None else "–"}</td>'
                                              f'<td class=pc{heat(x, full)}>{"" if x is None else format(x, fmt)}</td>')
            rows += (f'<tr><td>{e(r["symbol"])}</td><td>{r["qty"]:g}</td><td>{usd(r["avg"])}</td>'
                     f'<td>{usd(r["last"])}</td><td>{usd(r["value"])}</td>{pair(day, pct, "+.2%", .03)}'
                     f'{pair(r["pnl"], tp, "+.1%", .3)}</tr>')
        held = sum(r['pnl'] or 0 for r in s['positions'])
        cost = sum((r['avg'] or 0) * r['qty'] for r in s['positions'] if r['pnl'] is not None)
        h += ('<h2>Positions</h2><div class=scroll><table class="grid wide pos"><tr><th>Symbol</th><th>Qty</th><th>Avg</th>'
              '<th>Last</th><th>Value</th><th colspan=2>Today</th><th colspan=2>Total P&amp;L</th></tr>' + rows +
              f'<tr><th>Total</th><td></td><td></td><td></td><th>{usd(sum(r["value"] or 0 for r in s["positions"]))}</th>'
              f'<th class=amt>{pnl(total_day[0]) if total_day else "–"}</th>'
              f'<th class=pc>{f"{total_day[1]:+.2%}" if total_day else ""}</th>'
              f'<th class=amt>{pnl(held)}</th><th class=pc>{f"{held / cost:+.1%}" if cost else ""}</th></tr></table></div>'
              '<p class=mute>Today: the latest regular-session price against the previous close (Yahoo); shares '
              'bought or sold in the session count from their fill price instead. Before the US open it shows the last '
              'full session. Last, Value and Total P&amp;L on LIVE accounts use the latest IBKR refresh.</p>' + PRICES_BTN)
    r = running()
    busy = next((k for k in [None] + ids if k in r), 0)
    names = ' + '.join(m['name'] for m in ms)
    if a['frozen']:
        pass
    elif busy != 0:
        h += ('<h2>Analysis</h2>' + progress_html() +
              '<p class=mute>Proposals above stay until the new ones arrive.</p>')
    elif r:
        h += '<h2>Analysis</h2><p class=mute>Another analysis is running; this button returns when it finishes.</p>'
    else:
        h += (f'<h2>Analysis</h2><form method=post action="/a/{a["id"]}/run"><button data-busy="Starting…">'
              f'Analyse {e(names)} now</button></form>'
              '<p class=mute>A fresh look using today\'s market brief. Its proposals replace any still awaiting approval.</p>')
        if family() and todays_research(c, date.today().isoformat()) and c.execute(
                'SELECT 1 FROM reports WHERE grp=? AND run_date=? AND usage_id IS NOT NULL',
                (grp_key(a), date.today().isoformat())).fetchone():
            h += (f'<form method=post action="/a/{a["id"]}/deep" onsubmit="return confirm(\'Run a deep review with Opus? '
                  'It reuses today\\\'s research and costs several times a normal analysis.\')">'
                  '<button class=no data-busy="Starting…">Deep review with Opus</button></form>'
                  '<p class=mute>Opus re-judges today\'s analysis from the same research: no new searching. '
                  'Its proposals replace the ones above.</p>')
    rep = c.execute('SELECT * FROM reports WHERE grp=? OR (account_id=? AND grp IS NULL) ORDER BY id DESC LIMIT 1',
                    (grp_key(a), a['id'])).fetchone()
    if rep:
        how = (f' · <a href="/trace/{rep["usage_id"]}">How this was made</a>' if rep['usage_id'] else '')
        kind = c.execute('SELECT kind FROM usage WHERE id=?', (rep['usage_id'],)).fetchone() if rep['usage_id'] else None
        deep = ' <span class=tag style="background:var(--btn)">DEEP REVIEW · OPUS</span>' if kind and kind[0] == 'deep analysis' else ''
        res = c.execute("SELECT body FROM reports WHERE grp='research' AND run_date<=? ORDER BY id DESC LIMIT 1",
                        (rep['run_date'],)).fetchone()
        res = json.loads(res['body']) if res else {}
        notes = {x['symbol']: x for x in res.get('stocks', []) + res.get('candidates', [])}
        sts = {m['name']: (m, state(c, m)) for m in ms}
        ctx, trig = plans_context(c, grp_key(a), sts, date.today().isoformat())
        hits = [(sym, x) for sym, xs in trig.items() for x in xs]
        if hits:
            h += ('<div class="card trigs"><b>Exit plan checks</b><ul style="margin:6px 0 0;padding-left:20px">'
                  + ''.join(f'<li><b>{e(sym)}</b> {e(x)}</li>' for sym, x in hits) +
                  '</ul><p class=mute style="margin:6px 0 0">The next analysis must propose a sale or say why it holds.</p></div>')
        h += (f'<h3>Latest: {e(when(rep["at"]))}{deep}{how} · <a href="/how">How it works</a></h3>' +
              analysis_html(rep['body'], [(n, st) for n, (_, st) in sts.items()], pend, notes, ctx))
    done = c.execute('SELECT * FROM proposals WHERE account_id=? AND status!="pending" ORDER BY id DESC LIMIT 30',
                     (a['id'],)).fetchall()
    if done:
        h += '<h2>History</h2><div class=scroll><table class=text><tr><th>Date</th><th>Trade</th><th>Status</th><th>Detail</th></tr>'
        status_v = {'filled': 'ADD', 'drafted': 'ADD', 'manual': 'WATCH', 'rejected': 'SELL', 'invalid': 'SELL',
                    'expired': 'HOLD', 'working': 'WATCH'}
        for p in done:
            det = (f'filled {usd(p["fill_price"])}, fee {usd(p["fee"])}' + (' in IBKR' if p['link'] else '') if p['fill_price'] else
                   f'<a href="{e(p["link"])}">open in IBKR</a>' + (f' <span class=mute>{e(p["note"])}</span>' if p['note'] and 'partly' in p['note'] else '')
                   if p['link'] else e(p['note'] or ''))
            who = f' by {e(p["decided_by"])}' if p['decided_by'] else ''
            side = (f'<span class="up" style="font-weight:700">{e(p["side"])}</span>' if p['side'] == 'BUY'
                    else f'<span class="dn" style="font-weight:700">{e(p["side"])}</span>')
            status = f'<span class="v {status_v.get(p["status"], "HOLD")}">{e(p["status"])}</span>{who}'
            h += (f'<tr><td>{p["run_date"]}</td><td>{side} {p["qty"]:g} {e(p["symbol"])}</td>'
                  f'<td>{status}</td><td>{det}</td></tr>')
        h += '</table></div>'
    got = c.execute('SELECT * FROM sandbox_divs WHERE account_id=? ORDER BY day DESC', (a['id'],)).fetchall()
    if got:
        h += (f'<h2>Dividends credited</h2><p class=mute style="margin-top:0">{usd(sum(r["net_usd"] for r in got))} so far, '
              f'after {DIV_WITHHOLDING:.0%} withholding, paid into cash on each ex-dividend date.</p><details><summary>All '
              f'{len(got)}</summary><div class=scroll><table><tr><th>Ex-date</th><th>Holding</th><th>Shares</th><th>Per share</th>'
              '<th>Credited</th></tr>' + ''.join(f'<tr><td>{r["day"]}</td><td>{e(r["symbol"])}</td><td>{r["qty"]:g}</td>'
                                                 f'<td>{usd(r["per_share"])}</td><td>{usd(r["net_usd"])}</td></tr>' for r in got)
              + '</table></div></details>')
    if not live:
        h += trade_html(a, s)
    if a['frozen']:
        h += (f'<h2>Benchmark</h2><form method=post action="/a/{a["id"]}/delete" onsubmit="return confirm(\'Delete this '
              f'benchmark and its history?\')"><button class=no>Delete</button></form>')
    elif not live:
        h += (f'<h2>Sandbox</h2><div class=row><a class=btn href="/a/{a["id"]}/edit">Edit starting state</a>'
              f'<form method=post action="/a/{a["id"]}/delete" onsubmit="return confirm(\'Delete this sandbox?\')">'
              f'<button class=no>Delete</button></form></div>')
    return page(a['name'], h, look(a), banner(a))


def sandbox_form(c, a=None, err=None):
    lives = c.execute('SELECT * FROM accounts WHERE mode="LIVE"').fetchall() if family() else []
    pos = ''
    if a:
        pos = '\n'.join(f'{p["symbol"]} {p["qty"]:g} {p["avg_price"]:.2f}' for p in
                        c.execute('SELECT * FROM positions WHERE account_id=? ORDER BY symbol', (a['id'],)))
    copy = ''.join(f'<label><input type=radio name=copy value={l["id"]} style="width:auto;min-height:24px"> '
                   f'copy positions AND cash from LIVE {e(l["name"])}</label>' for l in lives)
    name, cash = (a['name'], f'{a["cash"]:.2f}') if a else ('', '')
    h = (f'<p><a href="/">← all accounts</a></p><h1>{"Edit" if a else "New"} sandbox</h1>'
         + (f'<p class=err>{e(err)}</p>' if err else '') +
         f'<form method=post><label for=f-name>Name</label><input id=f-name name=name required value="{e(name)}">'
         f'<label for=f-cash>Cash (USD)</label><input id=f-cash name=cash inputmode=decimal placeholder=10000 value="{cash}">'
         '<p class=mute>Ignored when copying from a live account: its cash is used, converted to USD.</p>')
    h += ('<label for=f-pos>Positions: one per line, <code>SYMBOL QTY AVG_PRICE</code> (average price optional, '
          f'defaults to today\'s price)</label><textarea id=f-pos name=positions rows=8>{e(pos)}</textarea>'
          f'<label><input type=radio name=copy value="" checked style="width:auto"> use the positions above</label>{copy}'
          f'<label><input type=checkbox name=frozen value=1 style="width:auto"{" checked" if a and a["frozen"] else ""}> '
          'Benchmark: hold these positions as they are, never analysed and never given proposals</label>'
          f'<p><button class="ok SANDBOX">Save sandbox</button></p></form>'
          '<p class=mute>Saving replaces the positions and cash. History and proposals are kept.</p>')
    return page('Sandbox', h, 'SANDBOX', 'SANDBOX — SIMULATED')


def save_sandbox(c, f, aid=None, user='alex', profile='family'):
    name = f.get('name', '').strip()
    if not name:
        raise ValueError('a name is required')
    if f.get('copy') and user in CONNECTORS:
        src = c.execute('SELECT * FROM accounts WHERE id=? AND mode="LIVE"', (int(f['copy']),)).fetchone()
        s = state(c, src) if src else None
        if not s:
            raise ValueError('that live account has not been loaded from IBKR yet')
        rows = [(r['symbol'], r['qty'], r['avg']) for r in s['positions']]
        cash = s['cash']
    else:
        cash = float(f.get('cash', '').replace(',', '').replace('$', '') or 'nan')
        if not cash >= 0:
            raise ValueError('enter a cash amount of 0 or more')
        rows = []
        for line in f.get('positions', '').splitlines():
            bits = line.replace(',', ' ').split()
            if not bits:
                continue
            if len(bits) not in (2, 3) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9.]{0,9}', bits[0]):
                raise ValueError(f'cannot read line: {line!r}')
            sym = bits[0].upper()
            rows.append((sym, float(bits[1]), float(bits[2]) if len(bits) == 3 else quote(sym)))
    if aid is None:
        aid = c.execute('INSERT INTO accounts (name, mode, owner, cash, created, frozen, profile) VALUES (?,?,?,?,?,?,?)',
                        (name, 'SANDBOX', user if profile == 'family' else profile, cash, now(), int(bool(f.get('frozen'))),
                         profile)).lastrowid
    else:
        c.execute('UPDATE accounts SET name=?, cash=?, frozen=? WHERE id=? AND mode="SANDBOX"',
                  (name, cash, int(bool(f.get('frozen'))), aid))
        c.execute('DELETE FROM positions WHERE account_id=?', (aid,))
        c.execute('DELETE FROM snapshots WHERE account_id=?', (aid,))  # a new start resets the benchmark
    c.executemany('INSERT OR REPLACE INTO positions VALUES (?,?,?,?)', [(aid, *r) for r in rows if r[1] > 0])
    c.commit()
    return aid


US_EXCHANGES = {'NMS', 'NGM', 'NCM', 'NAS', 'NYQ', 'ASE', 'PCX', 'BTS', 'NIM'}  # Yahoo's codes: Nasdaq, NYSE, Arca, Cboe


def search_symbols(q):
    """US-listed stocks and ETFs matching a ticker or company name, from Yahoo's search: [{symbol, name, exchange, type}]."""
    req = urllib.request.Request('https://query2.finance.yahoo.com/v1/finance/search?' + urllib.parse.urlencode(
        {'q': q, 'quotesCount': 12, 'newsCount': 0, 'listsCount': 0}), headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as r:
        found = json.load(r).get('quotes', [])
    return [{'symbol': x['symbol'].replace('-', '.'), 'name': x.get('longname') or x.get('shortname') or '',
             'exchange': x.get('exchDisp') or x['exchange'], 'type': x.get('typeDisp') or x.get('quoteType', '').title()}
            for x in found if x.get('exchange') in US_EXCHANGES and x.get('quoteType') in ('EQUITY', 'ETF')][:8]


def manual_trade(c, a, f, user):
    """A buy or sell typed in by hand on a sandbox or frozen account, filled at once at the price given or the current
    one, with IBKR's fee. Stored as a proposal with status "manual": shown in the history, never scored as advice."""
    sym = f.get('symbol', '').strip().upper().replace('-', '.')
    if not re.fullmatch(r'[A-Z][A-Z0-9.]{0,9}', sym):
        raise ValueError('pick a stock from the list')
    side = f.get('side')
    if side not in ('BUY', 'SELL'):
        raise ValueError('choose buy or sell')
    qty = float(f.get('qty', '').replace(',', '') or 'nan')
    if not qty > 0:
        raise ValueError('enter a number of shares above 0')
    QUOTES.pop(sym, None)
    try:
        last = quote(sym)  # also proves the symbol exists
    except Exception:
        raise ValueError(f'no price found for {sym}')
    typed = f.get('price', '').replace(',', '').replace('$', '').strip()
    price = float(typed) if typed else last
    if not price > 0:
        raise ValueError('enter a price above 0, or leave it empty for the current price')
    p = {'account_id': a['id'], 'symbol': sym, 'qty': qty, 'side': side, 'order_type': 'MARKET', 'limit_price': None, 'plan': None}
    fee, gain = fill(c, p, price)
    plan_on_fill(c, a, p)
    c.execute('INSERT INTO proposals (account_id, run_date, symbol, side, qty, order_type, ref_price, reason, status, '
              'decided_by, decided_at, fill_price, fee, realised, proposed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
              (a['id'], date.today().isoformat(), sym, side, qty, 'MARKET', last, 'Entered by hand', 'manual', user, now(),
               price, fee, gain, now()))
    c.commit()


def top_up(c, a, amount):
    """Add cash to a sandbox. The past daily values are scaled up by the same proportion, so the charts and the
    comparisons show returns, not the deposit.
    ponytail: rescales history rather than tracking deposits; past dollar figures read as if the money had been there
    from the start. Track cash flows if the dollar history itself matters."""
    if not amount > 0:
        raise ValueError('enter an amount above 0')
    before = state(c, c.execute('SELECT * FROM accounts WHERE id=?', (a['id'],)).fetchone())['value']
    if before > 0:
        c.execute('UPDATE snapshots SET value_usd=value_usd*? WHERE account_id=?', ((before + amount) / before, a['id']))
    else:
        c.execute('DELETE FROM snapshots WHERE account_id=?', (a['id'],))  # nothing to scale: measure from here
    c.execute('UPDATE accounts SET cash=cash+? WHERE id=?', (amount, a['id']))
    c.commit()


def trade_html(a, s):
    """The by-hand buy and sell form, and the top-up, for a sandbox or frozen account."""
    return (f'<h2>Buy or sell by hand</h2><div class=card><form method=post action="/a/{a["id"]}/trade" class=trade>'
            '<label for=t-sym>Stock</label><div class=ta>'
            '<input id=t-sym name=symbol required autocomplete=off autocapitalize=characters spellcheck=false role=combobox '
            'aria-autocomplete=list aria-expanded=false aria-controls=t-list placeholder="Ticker or company name">'
            '<ul id=t-list role=listbox hidden></ul></div><div id=t-pick class=mute aria-live=polite></div>'
            '<fieldset class=side><legend>Buy or sell</legend>'
            '<label><input type=radio name=side value=BUY checked> Buy</label>'
            '<label><input type=radio name=side value=SELL> Sell</label></fieldset>'
            '<div class=pair><div><label for=t-qty>Shares</label><input id=t-qty name=qty inputmode=decimal required></div>'
            '<div><label for=t-px>Price per share (USD)</label><input id=t-px name=price inputmode=decimal '
            'placeholder="current price"></div></div>'
            f'<p class=mute>Fills at once. Leave the price empty for the current price, or enter what you paid. IBKR\'s fee '
            f'is charged, as on any sandbox fill. Cash: {usd((s or {}).get("cash", a["cash"]))}.</p>'
            '<p><button class="ok SANDBOX" data-busy="Filling…">Fill now</button></p></form></div>'
            f'<h2>Add cash</h2><form method=post action="/a/{a["id"]}/cash" class=row>'
            '<label for=c-amt class=sr>Amount to add (USD)</label>'
            '<input id=c-amt name=amount inputmode=decimal required placeholder="Amount, USD" style="max-width:200px">'
            '<button class=no data-busy="Adding…">Add cash</button></form>'
            '<p class=mute>A top-up is not a gain: past values are scaled by the same proportion, so the charts keep showing '
            'returns only.</p>')


DRIFT = 0.03  # a market order whose price has moved more than this since it was proposed asks before going ahead
DRIFT_OK = 'Approving again accepts the new price'


def decide(c, pid, approve, user, reason=None):
    """Returns (account, error). Claims the proposal first, so a double tap cannot act twice."""
    p = c.execute('SELECT * FROM proposals WHERE id=?', (pid,)).fetchone()
    if not p:
        return None, 'no such proposal'
    a = c.execute('SELECT * FROM accounts WHERE id=?', (p['account_id'],)).fetchone()
    if not approve:
        c.execute('UPDATE proposals SET status="rejected", decided_by=?, decided_at=?, reject_reason=? '
                  'WHERE id=? AND status="pending"', (user, now(), reason if reason in REJECT_REASONS else None, pid))
        c.commit()
        return a, None
    if c.execute('UPDATE proposals SET status="working" WHERE id=? AND status="pending"', (pid,)).rowcount != 1:
        c.commit()
        return a, 'already decided'
    c.commit()
    try:
        QUOTES.pop(p['symbol'], None)
        try:
            price = quote(p['symbol'])
        except Exception:
            if a['mode'] == 'SANDBOX':
                raise
            price = None  # LIVE: Yahoo being down must not block a draft
        # A market order approved hours later trades wherever the price is by then: say so once, and a second
        # approval accepts it. Limit orders carry their own protection.
        moved = price / p['ref_price'] - 1 if price and p['order_type'] == 'MARKET' and p['ref_price'] else 0
        if abs(moved) > DRIFT and DRIFT_OK not in (p['note'] or ''):
            c.execute('UPDATE proposals SET status="pending", note=? WHERE id=?', (' '.join(filter(None, [
                p['note'], f'Price moved {moved:+.1%} since it was proposed ({usd(p["ref_price"])} to {usd(price)}). '
                           f'{DRIFT_OK}.'])), pid))
            c.commit()
            return a, f'{p["symbol"]} has moved {moved:+.1%} since this was proposed: check the reason still holds, then approve again.'
        if a['mode'] == 'SANDBOX':
            fee, gain = fill(c, p, price)
            c.execute('UPDATE proposals SET status="filled", fill_price=?, fee=?, realised=?, decided_by=?, decided_at=? '
                      'WHERE id=?', (price, fee, gain, user, now(), pid))
            plan_on_fill(c, a, p)
        else:
            link = draft_order(c, a, p)
            c.execute('UPDATE proposals SET status="drafted", link=?, decided_by=?, decided_at=?, '
                      'note="submit it in the IBKR app" WHERE id=?', (link, user, now(), pid))
        c.commit()
        return a, None
    except Exception as ex:
        c.rollback()
        c.execute('UPDATE proposals SET status="pending" WHERE id=?', (pid,))
        c.commit()
        return a, str(ex)


# ---------------------------------------------------------------- tax register pages
def money(x, cur):
    return f'{"-" if x < 0 else ""}{"A$" if cur == "AUD" else "$"}{abs(x):,.2f}'


def tax_page(c, owner):
    rs = tax.rows(c, owner)
    other = 'sam' if owner == 'alex' else 'alex'
    h = (f'<p><a href="/">← all accounts</a> · <a href="/tax?owner={other}">{other.title()}</a></p>'
         f'<h1>{owner.title()}: capital gains register</h1>'
         '<p class=mute>One row per parcel. Dates are trade dates on the exchange\'s calendar: New York for US shares, Sydney for ASX. USD rows are '
         'in USD, ASX rows in AUD; nothing is converted. '
         'IBKR trades are added on every refresh '
         'and matched to the parcels that add the least tax (losses first, then gains past the 12-month discount, oldest '
         'first among equals). Check the matching and edit any row.</p>')
    live = c.execute('SELECT * FROM accounts WHERE mode="LIVE" AND owner=?', (owner,)).fetchone()
    s = state(c, live) if live else None
    if s:
        diff = tax.reconcile(rs, s['positions'])
        if diff:
            h += ('<div class=err><b>Register does not match IBKR.</b> Unsold shares: '
                  + '; '.join(f'{e(x)} register {r:g}, IBKR {i:g}' for x, r, i in diff) +
                  '. A trade, split or share grant is missing or wrong.</div>')
    summ = tax.summary(rs)
    if summ:
        h += ('<h2>By financial year</h2><div class="card scroll"><table class="grid tint"><tr><th>FY</th><th>Sales</th><th>Total</th>'
              '<th>Gains held 12m+</th><th>Gains under 12m</th><th>Losses</th><th>Net gain (est.)</th><th>Losses carried on</th>'
              '<th>CSV</th></tr>')
        now_fy = tax.fy(date.today().isoformat())
        nets = tax.net(summ)
        for (cur, y), t in summ.items():
            n_ = nets[(cur, y)]
            h += (f'<tr{" class=now" if y == now_fy else ""}><td>{y} {cur}{" (this year)" if y == now_fy else ""}</td><td>{t["n"]}</td><td>{pnl_cur(t["total"], cur)}</td>'
                  f'<td>{pnl_cur(t["long"], cur)}</td><td>{pnl_cur(t["short"], cur)}</td><td>{pnl_cur(t["loss"], cur)}</td>'
                  f'<td><b>{money(n_["net_gain"], cur)}</b></td><td>{money(n_["losses_carried_out"], cur) if n_["losses_carried_out"] else ""}</td>'
                  f'<td><a href="/tax.csv?owner={owner}&fy={y}">download</a></td></tr>')
        h += ('</table></div><p class=mute>Net gain is an estimate of what is taxed: losses (and losses carried on from earlier years '
              'in this register) come off gains held under 12 months first, then off the 12m+ gains, which are then halved by '
              'the 50% CGT discount. Per currency and not converted to AUD; the accountant\'s figures are the real ones.</p>')
    h += (f'<p class=row><a class=btn href="/tax.csv?owner={owner}">Download everything (CSV)</a>'
          f'<a class=btn href="/tax/new?owner={owner}">Add a row</a></p>')
    for cur in ('USD', 'AUD'):
        sub = [r for r in rs if r['currency'] == cur]
        if not sub:
            continue
        h += (f'<h2>{"US shares" if cur == "USD" else "ASX shares"}<span class=cur>{cur}</span></h2><div class="card scroll"><table class="grid tint"><tr><th>Share</th><th>Sale date</th><th>Qty</th>'
              '<th>Price</th><th>Fees</th><th>Proceeds</th><th>Bought</th><th>Qty</th><th>Cost/sh</th><th>Fees</th>'
              '<th>Total cost</th><th>P&amp;L</th><th>Held</th><th></th></tr>')
        for r in sub:
            sold = bool(r['sale_date'])
            flag = ' <span class=tag style="background:var(--dn)">!</span>' if r['note'] and 'NO PURCHASE' in r['note'] else ''
            h += (f'<tr><td><b>{e(r["symbol"])}</b>{flag}</td><td>{r["sale_date"] or "<span class=\'st held\'>held</span>"}</td>'
                  f'<td>{format(r["sale_qty"], "g") if sold else ""}</td>'
                  f'<td>{money(r["sale_price"], cur) if sold else ""}</td><td>{money(r["sale_fee"], cur) if sold else ""}</td>'
                  f'<td>{money(r["proceeds"], cur) if sold else ""}</td><td>{r["buy_date"]}</td><td>{r["buy_qty"]:g}</td>'
                  f'<td>{money(r["buy_price"], cur)}</td><td>{money(r["buy_fee"], cur)}</td><td>{money(r["cost"], cur)}</td>'
                  f'<td{heat(r["pnl"] / r["cost"] if sold and r["cost"] else None, 0.5)}>{pnl_cur(r["pnl"], cur) if sold else ""}</td>'
                  f'<td>{held_html(r) if sold else ""}</td>'
                  f'<td><a href="/tax/lot/{r["id"]}">edit</a></td></tr>')
        h += '</table></div><p class=mute><span class=up>✓</span> held 12 months or more (discount-eligible) · '
        h += '<span class=dn>✗</span> under 12 months.</p>'
    return page(f'Tax {owner}', h, 'HOME', f'TAX REGISTER — {owner.upper()}')


def pnl_cur(x, cur):
    return f'<span class="{"up" if x >= 0 else "dn"}">{money(x, cur)}</span>'


def held_html(r):
    mark = '<span class=up>✓</span>' if r['discount'] else '<span class=dn>✗</span>'
    return f'{mark} {e(r["held"])}'


def lot_form(c, owner, lot=None, err=None):
    v = dict(lot) if lot else {'currency': 'USD'}
    h = (f'<p><a href="/tax?owner={owner}">← register</a></p><h1>{"Edit" if lot else "Add"} parcel</h1>'
         + (f'<p class=err>{e(err)}</p>' if err else '') + '<form method=post>')
    for f in tax.FIELDS:
        typ = 'date' if f.endswith('date') else 'text'
        h += (f'<label for=f-{f}>{f.replace("_", " ")}</label><input id=f-{f} type={typ} name={f} '
              f'value="{e("" if v.get(f) is None else str(v[f]))}">')
    h += ('<p class=mute>Leave the four sale fields empty for a parcel still held.</p>'
          '<p class=row><button>Save</button></p></form>')
    if lot and not lot['sale_date']:
        h += (f'<h2>Split</h2><form method=post action="/tax/lot/{lot["id"]}/split" class=row>'
              '<input name=qty inputmode=decimal placeholder="shares to split off" style="width:12em">'
              '<button>Split parcel</button></form><p class=mute>Makes two parcels with the same date and price, '
              'so part of it can be matched to a sale.</p>')
    if lot:
        h += (f'<h2>Delete</h2><form method=post action="/tax/lot/{lot["id"]}/delete" '
              'onsubmit="return confirm(\'Delete this parcel from the register?\')"><button class=no>Delete row</button></form>')
    return page('Parcel', h, 'HOME', f'TAX REGISTER — {owner.upper()}')


def save_lot(c, owner, f, lid=None):
    v = {k: (f.get(k) or '').strip() or None for k in tax.FIELDS}
    for k in ('buy_qty', 'buy_price', 'buy_fee', 'sale_qty', 'sale_price', 'sale_fee'):
        v[k] = float(v[k].replace(',', '').replace('$', '')) if v[k] else None
    for k in ('buy_date', 'sale_date'):
        if v[k]:
            date.fromisoformat(v[k])
    if not (v['symbol'] and v['buy_date'] and v['buy_qty'] is not None and v['buy_price'] is not None):
        raise ValueError('symbol, purchase date, quantity and price are required')
    if v['currency'] not in ('USD', 'AUD'):
        raise ValueError('currency must be USD or AUD')
    sale = [v['sale_date'], v['sale_qty'], v['sale_price']]
    if any(x is not None for x in sale) and not all(x is not None for x in sale):
        raise ValueError('fill in sale date, quantity and price together, or none of them')
    v['buy_fee'] = v['buy_fee'] or 0
    if v['sale_date']:
        v['sale_fee'] = v['sale_fee'] or 0
    v['symbol'] = v['symbol'].upper()
    if lid:
        c.execute(f'UPDATE lots SET {", ".join(k + "=?" for k in tax.FIELDS)} WHERE id=? AND owner=?',
                  [v[k] for k in tax.FIELDS] + [lid, owner])
    else:
        c.execute(f'INSERT INTO lots (owner, source, {", ".join(tax.FIELDS)}) VALUES (?,?,{",".join("?" * len(tax.FIELDS))})',
                  [owner, 'manual'] + [v[k] for k in tax.FIELDS])
    c.commit()


JOBS = {'brief': 'brief', 'market brief': 'brief', 'research': 'research', 'facts': 'research', 'options': 'research',
        'analysis': 'analysis', 'deep analysis': 'analysis', 'exit plans': 'analysis'}


def job(kind):
    """A job's name as a pill in its colour (see .job in CSS)."""
    return f'<span class="job {JOBS.get(kind, "other")}">{e(kind)}</span>'


def ny_start(days_ago=0):
    """Midnight New York time, `days_ago` days back, as a Sydney-local timestamp like usage.at. Days are New York
    days so a run at 23:50 Sydney and its follow-ups after midnight count as one trading day."""
    d = datetime.now(learn.NY).date() - timedelta(days=days_ago)
    return datetime(d.year, d.month, d.day, tzinfo=learn.NY).astimezone(learn.SYD).replace(tzinfo=None).isoformat()


def ny_time(at):
    """'09-30 09:50' in New York time from a Sydney-local timestamp."""
    return datetime.fromisoformat(at).replace(tzinfo=learn.SYD).astimezone(learn.NY).strftime('%m-%d %H:%M')


def usage_page(c):
    names = {a['id']: a['name'] for a in c.execute('SELECT id, name FROM accounts')}
    h = ('<p><a href="/">← all accounts</a></p><h1>Claude usage</h1>'
         '<p class=mute>Runs on the Claude subscription, so nothing here is billed per call. "Cost" is what the '
         'same tokens would cost on the Claude API: a yardstick for how heavy each job is. Most input is '
         '"cache read", which the API charges at a tenth of the normal input price. Days and times are New York '
         'time, so one trading day\'s runs count as one day.</p>')
    tot = lambda where, args=(): c.execute(
        'SELECT COUNT(*), COALESCE(SUM(cost_usd),0), COALESCE(SUM(input_tokens+cache_read_tokens+cache_write_tokens),0), '
        f'COALESCE(SUM(output_tokens),0) FROM usage WHERE {where}', args).fetchone()
    h += '<div class=tiles style="margin-top:14px">'
    for label, where, args in (('Today', 'at >= ?', (ny_start(),)), ('Last 7 days', 'at >= ?', (ny_start(6),)),
                               ('Last 30 days', 'at >= ?', (ny_start(29),)), ('All time', '1', ())):
        n, cost, tin, tout = tot(where, args)
        h += (f'<div class="card"><div class=mute>{label}</div><div class=big style="color:var(--brand-ink)">{usd(cost)}</div>'
              f'<div class=mute>{n} run{"s" if n != 1 else ""}, {tin / 1e6:.1f}M tokens</div></div>')
    kinds = c.execute("SELECT kind, COUNT(*), AVG(cost_usd), AVG(seconds), SUM(cost_usd) FROM usage "
                      "WHERE at >= ? GROUP BY kind ORDER BY SUM(cost_usd) DESC", (ny_start(29),)).fetchall()
    top = max([r[4] or 0 for r in kinds] + [0.01])
    h += ('</div><h2>By job, last 30 days</h2><div class="card scroll"><table class=tint><tr><th>Job</th><th>Runs</th>'
          '<th>Avg cost</th><th>Avg time</th><th style="min-width:110px">Total</th></tr>')
    for r in kinds:
        h += (f'<tr><td>{job(r[0])}</td><td>{r[1]}</td><td>{usd(r[2])}</td><td>{(r[3] or 0) / 60:.1f} min</td>'
              f'<td>{usd(r[4])}<span class="cost job {JOBS.get(r[0], "other")}" style="width:{100 * (r[4] or 0) / top:.0f}%;padding:0"></span></td></tr>')
    h += '</table></div><h2>Recent runs</h2><div class="card scroll"><table class="runs tint"><tr><th>Time (New York)</th><th>Job</th><th>Account</th>' \
         '<th>Cost</th><th>Tokens in</th><th>Out</th><th>Turns</th><th>Time</th></tr>'
    recent = c.execute('SELECT * FROM usage ORDER BY id DESC LIMIT 60').fetchall()
    # One New York day per group: a heading row with the day's totals, and every other day's rows tinted.
    day = lambda r: datetime.fromisoformat(r['at']).replace(tzinfo=learn.SYD).astimezone(learn.NY).date()
    per = {}
    for r in recent:
        n, cost = per.get(day(r), (0, 0))
        per[day(r)] = (n + 1, cost + (r['cost_usd'] or 0))
    order = list(per)
    for r in recent:
        d = day(r)
        band = ' class=band' if order.index(d) % 2 else ''
        if r is next(x for x in recent if day(x) == d):
            h += (f'<tr class="day{" band" if band else ""}"><th colspan=8>{d:%a %-d %b} <span class=mute>{per[d][0]} '
                  f'run{"s" if per[d][0] != 1 else ""} · {usd(per[d][1])}</span></th></tr>')
        tin = (r['input_tokens'] or 0) + (r['cache_read_tokens'] or 0) + (r['cache_write_tokens'] or 0)
        h += (f'<tr{band}><td>{ny_time(r["at"])[6:]}</td><td><a href="/trace/{r["id"]}">{job(r["kind"])}</a>{"" if r["ok"] else ' <span class="job failed">failed</span>'}</td>'
              f'<td>{e(names.get(r["account_id"], "–"))}</td><td>{usd(r["cost_usd"])}</td><td>{tin:,}</td>'
              f'<td>{r["output_tokens"] or 0:,}</td><td>{r["turns"] or ""}</td><td>{(r["seconds"] or 0) / 60:.1f} min</td></tr>')
    return page('Usage', h + '</table></div>')



# ---------------------------------------------------------------- transparency
def domain(url):
    return urllib.parse.urlsplit(url).netloc.removeprefix('www.')


def how_step(job_cls, who, title, summary, more=''):
    """One step of the daily run: a card edged in the colour of who does it, the gist always shown, the detail folded."""
    return (f'<li class={job_cls}><div class=card><div class=step-head><h3>{title}</h3><span class="job {job_cls}">{who}</span></div>'
            f'<p class=gist>{summary}</p>' + (f'<details><summary>How it works</summary><div class=more>{more}</div></details>' if more else '')
            + '</div></li>')


def how_flow(c, model):
    """The top of How it works: the day at a glance, the run step by step, what happens afterwards, the investor lenses."""
    nxt = subprocess.run(['systemctl', 'show', '-p', 'NextElapseUSecRealtime', '--value', 'stock-analyse.timer'],
                         capture_output=True, text=True).stdout.strip()
    at = datetime.strptime(' '.join(nxt.split()[1:3]), '%Y-%m-%d %H:%M:%S') if nxt else None
    groups = c.execute('SELECT COUNT(DISTINCT COALESCE(grp, "a" || id)) FROM accounts WHERE frozen=0').fetchone()[0]
    h = ('<p><a href="/">← all accounts</a></p><h1>How the analysis works</h1>'
         '<p class=mute style="margin-top:0">Generated from the running code, so it always matches what happens. Every analysis '
         'also links to its own record of what it searched, opened and fetched.</p>'
         '<div class="card glance"><dl class=facts>'
         f'<dt>When</dt><dd>Weekdays at 09:50 New York, 20 minutes after the open{f" (next: {at:%a %-d %b %H:%M} Sydney)" if at else ""}, '
         'or from a button</dd>'
         f'<dt>Claude runs</dt><dd>A market brief and the research, shared by everyone, then one analysis per portfolio ({groups} today)</dd>'
         '<dt>Who trades</dt><dd>Only you. A sandbox fills when you approve; a LIVE approval makes an IBKR draft you submit yourself</dd>'
         '<dt>Measured</dt><dd>Every Saturday against the S&amp;P 500: proposals, verdicts, investor lenses and chart patterns</dd></dl></div>'
         '<h2>The daily run</h2><p class=mute style="margin:0 0 12px">In order. Each step is coloured by who does it: '
         '<span class="job brief">Claude reads the market</span> <span class="job research">Claude researches</span> '
         '<span class="job analysis">Claude decides</span> <span class="job other">the app, by script</span> '
         '<span class="job you">you</span></p><ol class="flow steps">'
         + how_step('brief', 'Claude, Sonnet', 'Market brief',
                    'The market as a whole: indexes, rates, the Fed, policy announcements and what they moved, markets breaking '
                    'their usual pattern, geopolitics, sectors and earnings. Once a day, shared.',
                    'The app computes the index table (day, week, month, year to date) from Yahoo closes, reads the latest '
                    'macro figures from FRED and the Finviz sector table. Claude adds the news from a few searches, citing each '
                    'source: what President Trump and his administration announced in the last 3 days (formally or on Truth '
                    'Social) and what it moved, whether earlier announcements were reversed, and the policy dates ahead. '
                    '<br><br>The app also checks seven cross-market links from a year of daily changes (gold against the dollar, '
                    'real yields and stocks; stocks against bonds and the VIX; oil against energy stocks; the A$ against stocks) '
                    'and marks any that broke; the brief must explain each one. It rates the day\'s policy shock as none, minor or '
                    'major, which the Learning page uses to compare advice given on shock days.')
         + how_step('other', 'the app', 'Accounts',
                    'LIVE accounts are read from IBKR; sandboxes are priced from Yahoo. Each account\'s value is recorded for the charts.',
                    'LIVE: one short Haiku run fetches positions, balances, cash and the trades since the last refresh, which '
                    'also feed the tax register. SANDBOX and FROZEN: priced from Yahoo, and paid their dividends into cash on each '
                    'ex-dividend date, less withholding. The account value and the S&amp;P 500 price are recorded daily.')
         + how_step('research', 'Claude, Sonnet', 'Research',
                    'Notes on every stock held in any portfolio, plus 5 to 8 candidates, from data the app gathers first. '
                    'Shared by every portfolio. <a href="/research">Read today\'s notes</a>.',
                    'By script: each stock\'s Finviz page (valuation, quality, growth, analyst rating changes, dated headlines), '
                    'trend figures from a year of Yahoo closes, Finnhub\'s results date, earnings surprises, analyst counts and '
                    'insider trades, SEC filings, IBKR option statistics, the sector table, a screen of the 3 strongest sectors '
                    'and the ones the portfolios lack, and the day\'s analyst upgrades. Claude summarises each stock, including its '
                    'exposure to US government policy (tariffs, China, drug pricing, defence spending), picks the candidates, '
                    'and searches only to fill gaps. Facts that rarely change are stored: domicile and dividend withholding, '
                    'IBKR contract ids, and the cash-parking options (re-researched weekly).')
         + how_step('other', 'the app', 'Investor lenses and patterns',
                    'Every stock researched is checked against <a href="#lenses">eight investors\' rules</a>, and scanned for '
                    '<a href="#patterns">13 candlestick and chart patterns</a>. All by script, and all recorded for scoring.',
                    'The results go on each stock\'s research note. Lenses are recorded once a week per stock and patterns each '
                    'time one appears, then scored every Saturday against the S&amp;P 500 on the <a href="/learn#lenses">Learning '
                    'page</a>. Patterns stay on trial, and may not be a reason for a trade, until they prove themselves there.')
         + how_step('analysis', f'Claude, {MODELS[analysis_model(c)]}', 'Portfolio analysis',
                    'One run per portfolio, deciding from the notes: a verdict on every holding, the candidates that suit it, a '
                    'cash plan, and up to 5 proposed trades.',
                    'Accounts in one household (Alex and Sam\'s LIVE accounts; their sandboxes) are one investment. The app '
                    'adds risk figures from 60 days of prices: the portfolio\'s beta to the S&amp;P 500, holdings that move '
                    'together, and a position size per stock that gives it the same risk as a typical holding. The rules ask it '
                    'to prefer stocks passing both a quality lens and a trend lens, never to buy in Weinstein stage 4, to judge '
                    'each holding by its Lynch kind\'s sell rule, to state a margin of safety, and not to trade on a policy '
                    'announcement alone. Each trade names its account (sales where the shares are, buys where the cash is) and '
                    f'whose tax a sale affects. A cash plan is written for any account holding more than ${PARK_ABOVE_USD:,}. '
                    'The model is set on the home page; proposals still waiting are replaced by the new ones.')
         + how_step('other', 'the app', 'Checks',
                    'Every proposal is checked by script. One that fails is kept but marked invalid, never shown as something to approve.',
                    'Invalid: selling more shares than the account holds, a buy the account\'s US dollar cash cannot pay for, or '
                    'a buy taking one stock above 25% of the portfolio. Warnings: one stock in two accounts, a buy more than 1.5 '
                    'times its risk-based size, a buy within a week of results.')
         + how_step('you', 'you', 'You decide',
                    'Approve or reject, saying why if you like. Your reasons feed the next analysis and the monthly review.',
                    'SANDBOX fills at the current Yahoo price with IBKR\'s fees. LIVE creates an IBKR order draft that you submit '
                    'in the IBKR app. A market order whose price has moved more than 3% since it was proposed asks once more.')
         + '</ol>'
         '<h2>After the run</h2><div class=after>'
         '<div class="card analysis"><h3>Exit plans</h3><p>Every BUY comes with a plan: how long to hold, a target, a review '
         'level that is not an automatic sale, what would break the thesis, and a review date. The app checks the triggers '
         'daily and shows when each owner\'s shares qualify for the capital gains discount.</p></div>'
         '<div class="card analysis"><h3>Deep review</h3><p>A button on the account page: Opus re-judges the day\'s analysis '
         'from the same brief and research, without new searching. Its proposals replace the first ones.</p></div>'
         '<div class="card other"><h3>Learning</h3><p>Every Saturday the app scores each proposal, every verdict, every lens and '
         'every pattern against the S&amp;P 500 over 5, 20 and 60 trading days. On the first Saturday of the month Opus reads '
         'the scores and your decisions and suggests lessons; a lesson changes the advice only after you keep it. '
         '<a href="/learn">Learning page</a>.</p></div>'
         '<div class="card other"><h3>Benchmarks</h3><p>A FROZEN copy of each portfolio is never analysed, so the advisor\'s '
         'sandbox can be measured against doing nothing. LIVE is measured by IBKR\'s time-weighted return, so deposits and '
         'withdrawals do not count as performance.</p></div></div>'
         f'<p class=mute>Most recent run used: {e(model)}, through Claude Code on the subscription.</p>'
         '<h2 id=lenses>The eight investor lenses</h2><p class=mute style="margin-top:0">What each investor would check, '
         'computed from Finviz figures and a year of prices. Each gives pass, mixed or fail with the numbers behind it; ETFs '
         'get only the trend lenses. Recorded weekly and scored on the <a href="/learn#lenses">Learning page</a>.</p>')
    for kind, title in (('own', 'What to own'), ('buy', 'When to buy')):
        h += (f'<h3>{title}</h3><div class=lensgrid>' + ''.join(
            f'<div class="card lens {kind}"><b>{e(name)}</b><div class=idea>{e(idea)}</div><p>Passes with {e(rule)}.</p></div>'
            for k, name, idea, rule, kk in lenses.LENSES if kk == kind) + '</div>')
    h += ('<h3>Lynch\'s kinds of stock, and when to sell each</h3><div class="card lynch"><dl class=facts>' + ''.join(
        f'<dt>{k.capitalize()}</dt><dd>{e(v[0].upper() + v[1:])}</dd>' for k, v in lenses.LYNCH_SELL.items()) + '</dl></div>'
        '<p class=mute>O\'Neil\'s automatic 7-8% stop-loss is not used: a review level stays a prompt to re-check, not a sale.</p>'
        '<h2 id=patterns>Patterns on trial</h2><div class="card trial"><p style="margin-top:0">Most studies find candlesticks '
        'and classic chart shapes add little once trading costs are counted, and they describe moves of days while this '
        'advisor holds for weeks or months. So they are recorded, not trusted: each one seen on a stock\'s last whole bar '
        'goes on its research note marked <b>on trial</b>, and the advisor may not use it as a reason.</p>'
        f'<p>A pattern is <b>proven</b> once {learn.PROVE_N} cases beat the S&amp;P 500 in its own direction by '
        f'{learn.PROVE_EDGE:.0%} or more on average over {learn.PROVE_H} trading days, right {learn.PROVE_HIT:.0%} of the '
        'time; only then can it count, alongside other evidence. With that many cases and no edge it has <b>failed</b>.</p>'
        '<p class=mute style="margin-bottom:0">Candlesticks: ' + ', '.join(e(lenses.PATTERNS[k][0]) for k in lenses.CANDLES)
        + '. Chart shapes: ' + ', '.join(e(v[0]) for k, v in lenses.PATTERNS.items() if k not in lenses.CANDLES)
        + '. <a href="/learn#patterns">Their record so far</a>.</p></div>')
    return h


def how_page(c):
    model = (c.execute('SELECT model FROM usage ORDER BY id DESC LIMIT 1').fetchone() or ['unknown'])[0]
    h = how_flow(c, model) + (
         '<h2>Sources</h2><div class="card scroll"><table class="text tint"><tr><th>Source</th><th>Used for</th><th>Used by</th></tr>'
         f'<tr><td>IBKR connector<div class=mute style="font-size:13px">read-only tools: {e(", ".join(READ_TOOLS))}</div></td><td>Your positions, cash and trades; '
         'live quotes, price history, performance, volatility, company themes and connections</td><td>brief, analysis, refresh</td></tr>'
         '<tr><td>Web search and web page fetch</td><td>Macro and geopolitical news for the brief; filling gaps in the research. '
         'Claude chooses the searches and sites itself; see "What was consulted" below</td><td>brief, research, analysis</td></tr>'
         '<tr><td>Finviz quote pages (by the app)</td><td>Each stock\'s valuation, quality (returns, margins, debt), growth, fund and '
         'insider ownership, short interest, analyst rating changes and dated headlines; the figures behind the investor lenses</td><td>research</td></tr>'
         f'<tr><td>Finviz <a href="{SECTORS_URL}">sector performance</a> and <a href="{SCREEN_URL}">stock screen</a>; '
         f'MarketBeat <a href="{UPGRADES_URL}">analyst upgrades</a> (by the app)</td><td>The sector picture and the candidate pool. Fixed pages, so every run starts from the same funnel</td><td>brief, research</td></tr>'
         '<tr><td>Finnhub (by the app, free plan)</td><td>Next results date, the last four earnings surprises, analyst buy/hold/sell '
         'counts now and 3 months ago, and insiders\' open-market buys and sales in 90 days</td><td>research, analysis</td></tr>'
         '<tr><td>FRED, St. Louis Fed (by the app)</td><td>Inflation, jobs, the Fed funds rate, the yield curve, credit spreads '
         'and the A$/US$ rate, against 3 and 12 months earlier; the 10-year real yield for the cross-market check</td><td>brief</td></tr>'
         '<tr><td>SEC EDGAR (by the app)</td><td>Each company\'s current-event filings in 30 days: results, executive changes, '
         'deals, restructuring, restatements, auditor changes, activist stakes</td><td>research</td></tr>'
         '<tr><td>IBKR option statistics (one short Haiku run)</td><td>Implied volatility, how high it is against the past year, '
         'realised volatility and put/call volume: the move the market prices in, for sizing, review levels and results risk'
         '</td><td>research, analysis</td></tr>'
         '<tr><td>The chart, read by the app</td><td>From a year of Yahoo closes: trend against the 50- and 200-day averages, '
         '12-1 momentum, golden and death crosses, 55-day breakouts, RSI, volatility, support and resistance; the trend lenses '
         '(Minervini, Weinstein); and the candlestick and chart patterns on trial, from daily open, high, low and close</td>'
         '<td>research, analysis</td></tr>'
         '<tr><td>Yahoo Finance (by the app, not Claude)</td><td>Index table, trend figures, sandbox prices and fills, SPY benchmark, AUD/USD for sandbox totals</td><td>app</td></tr>'
         '<tr><td>Your decisions</td><td>The last 20 approvals and rejections for the account</td><td>analysis</td></tr></table></div>'
         '<h2>Guard rails</h2><ul class=md style="white-space:normal">'
         f'<li>Claude is refused these tools in every analysis: {e(", ".join(sorted({d.rsplit("__", 1)[-1] for d in DENY})))}. '
         'It cannot place, change or delete orders, alerts or watchlists, run commands or write files.</li>'
         '<li>Only the approve button on a LIVE proposal creates an IBKR order instruction, and that is a draft: IBKR sends nothing to market until you submit it in the IBKR app.</li>'
         '<li>The app rejects any proposal with a malformed symbol, a sale of more shares than held, or a limit order without a positive price. Sandbox fills also refuse buys beyond the cash and limits that are not reached.</li></ul>'
         '<h2>Exact instructions</h2>'
         f'<details><summary>Market brief prompt</summary><pre class=md>{e(BRIEF_PROMPT)}</pre></details>'
         f'<details><summary>Research prompt</summary><pre class=md>{e(RESEARCH_PROMPT + RESEARCH_MORE + PARKING_PROMPT)}</pre></details>'
         f'<details><summary>Account analysis prompt</summary><pre class=md>{e(ACCOUNT_PROMPT + RULES)}</pre></details>'
         f'<details><summary>Deep review addition</summary><pre class=md>{e(DEEP_PROMPT)}</pre></details>'
         f'<details><summary>Monthly review prompt</summary><pre class=md>{e(REVIEW_PROMPT)}</pre></details>'
         f'<details><summary>Required answer structure</summary><pre class=md>{e(json.dumps(PROPOSALS, indent=1))}</pre></details>')
    since = "at >= datetime('now','localtime','-30 days')"
    uids = f'SELECT id FROM usage WHERE {since}'
    runs = c.execute(f"SELECT COUNT(*) FROM usage WHERE {since} AND kind IN ('analysis','deep analysis','research','market brief')").fetchone()[0]
    h += f'<h2>What was consulted, last 30 days</h2><p class=mute>Across {runs} brief, research and analysis runs.</p>'
    doms, found = {}, {}
    for (u,) in c.execute(f"SELECT detail FROM trace WHERE kind='fetch' AND usage_id IN ({uids})"):
        doms[domain(u)] = doms.get(domain(u), 0) + 1
    for (j,) in c.execute(f"SELECT detail FROM trace WHERE kind='found' AND usage_id IN ({uids})"):
        for l in json.loads(j):
            found[domain(l['url'])] = found.get(domain(l['url']), 0) + 1
    if found:
        h += ('<h3>Sites that searches returned</h3><p class=mute>Claude reads the search result summaries; '
              'only pages it opens are listed under "Websites opened".</p><div class=scroll><table><tr><th>Site</th>'
              '<th>Results</th></tr>' + ''.join(f'<tr><td>{e(d)}</td><td>{n}</td></tr>'
              for d, n in sorted(found.items(), key=lambda kv: -kv[1])[:30]) + '</table></div>')
    if doms:
        h += '<h3>Websites opened</h3><div class=scroll><table><tr><th>Site</th><th>Pages</th></tr>' + ''.join(
            f'<tr><td>{e(d)}</td><td>{n}</td></tr>' for d, n in sorted(doms.items(), key=lambda kv: -kv[1])[:30]) + '</table></div>'
    ib = c.execute(f"SELECT tool, COUNT(*) FROM trace WHERE kind='ibkr' AND usage_id IN ({uids}) GROUP BY tool ORDER BY 2 DESC").fetchall()
    if ib:
        h += '<h3>IBKR data requests</h3><div class=scroll><table><tr><th>Tool</th><th>Calls</th></tr>' + ''.join(
            f'<tr><td>{e(t)}</td><td>{n}</td></tr>' for t, n in ib) + '</table></div>'
    q = c.execute(f"SELECT detail FROM trace WHERE kind='search' AND usage_id IN ({uids}) ORDER BY usage_id DESC, seq LIMIT 40").fetchall()
    if q:
        h += '<h3>Recent web searches</h3><ul>' + ''.join(f'<li>{e(x[0])}</li>' for x in q) + '</ul>'
    if not (doms or ib or q or found):
        h += '<p class=mute>Nothing recorded yet: recording started on 24 Sep 2026.</p>'
    # the Sources table's "Used by" cells: each job as a pill in its colour
    h = re.sub(r'<td>((?:brief|research|analysis|refresh|app)(?:, (?:brief|research|analysis|refresh|app))*)</td></tr>',
               lambda m: '<td>' + ''.join(job(k) for k in m[1].split(', ')) + '</td></tr>', h)
    return page('How it works', h)


def research_page(c):
    r = c.execute("SELECT * FROM reports WHERE grp='research' ORDER BY id DESC LIMIT 1").fetchone()
    h = '<p><a href="/">← all accounts</a> · <a href="/how">How it works</a></p><h1>Research notes</h1>'
    if not r:
        return page('Research', h + '<p class=mute>None yet: the first analysis run writes them.</p>')
    d = json.loads(r['body'])
    how = f' · <a href="/trace/{r["usage_id"]}">How this was made</a>' if r['usage_id'] else ''
    h += (f'<p class=mute>{e(when(r["at"]))}{how}. Shared by every portfolio; facts, not decisions. '
          'Each household\'s analysis decides from these.</p>')
    stats = price_stats([x['symbol'] for x in d['stocks']])
    note = lambda x, extra='': (
        f'<div class=card><div class=row style="justify-content:space-between"><span class=big>{e(x["symbol"])}</span>'
        f'<span class=mute>{e(x["domicile"])} · {e(x["withholding"])}</span></div>{extra}<p><b>{e(x["summary"])}</b></p>'
        f'<details><summary>News, analysts, investor lenses and more</summary><p class=md>{e(x["news"])}</p><p class=md>{e(x["analysts"])}</p>'
        + ''.join(f'<p class=md><b>{label}:</b> {e(x[k])}</p>' for k, label in (('analyst_trend', 'Analyst counts'),
                  ('earnings', 'Earnings'), ('insiders', 'Insiders'), ('fundamentals', 'Fundamentals'), ('options', 'Options'),
                  ('filings', 'SEC filings'), ('policy', 'Policy exposure'), ('lenses', 'Investor lenses'),
                  ('patterns', 'Patterns')) if x.get(k)) +
        f'<p class=md>{e(x["valuation"])}</p>{sources_html(x.get("sources"))}</details></div>')
    def trend(sym):
        t = stats.get(sym)
        return (f'<div class=mute>6 months {t["chg_6m_pct"]:+.1f}% · 3 months {t["chg_3m_pct"]:+.1f}% · '
                f'{t["pct_from_high"]:+.1f}% from the 52-week high · {t["vs_sma200_pct"]:+.1f}% vs 200-day average</div>'
                if t and t['chg_6m_pct'] is not None and t['vs_sma200_pct'] is not None else '')
    if d.get('sectors'):
        h += f'<h2>Sectors</h2><div class="card md">{e(d["sectors"])}</div>'
    h += '<h2>Stocks held</h2><div class=grid>' + ''.join(note(x, trend(x['symbol'])) for x in d['stocks']) + '</div>'
    if d.get('candidates'):
        h += '<h2>Candidates</h2><div class=grid>' + ''.join(
            note(x, f'<div class=mute>{e(x["sector"])} · {e(x["trend"])}</div><p class=md>{e(x["why"])}</p>')
            for x in d['candidates']) + '</div>'
    if d.get('parking'):
        h += '<h2>Parking idle cash</h2>' + option_cards(d['parking'])
    if d.get('gaps'):
        h += f'<h2>Gaps</h2><p class=md>{e(d["gaps"])}</p>'
    return page('Research', h)


def trace_page(c, uid):
    u = c.execute('SELECT * FROM usage WHERE id=?', (uid,)).fetchone()
    if not u:
        return None
    acct = c.execute('SELECT name FROM accounts WHERE id=?', (u['account_id'],)).fetchone()
    steps = c.execute('SELECT * FROM trace WHERE usage_id=? ORDER BY seq', (uid,)).fetchall()
    n = {k: sum(1 for x in steps if x['kind'] == k) for k in ('search', 'fetch', 'ibkr')}
    h = (f'<p><a href="/">← all accounts</a> · <a href="/how">How it works</a></p>'
         f'<h1>How this {e(u["kind"])} was made</h1><p class=mute>{e(acct["name"] if acct else "all accounts")} · '
         f'{e(u["at"].replace("T", " ")[:16])} · {(u["seconds"] or 0) / 60:.1f} min · {u["turns"] or 0} turns · '
         f'{usd(u["cost_usd"])} API-equivalent</p>'
         f'<p>{n["search"]} web searches, {n["fetch"]} pages opened, {n["ibkr"]} IBKR data requests, in the order they happened. '
         'Notes are Claude\'s own working remarks between steps.</p>')
    label = {'search': 'Search', 'found': 'Results', 'fetch': 'Opened', 'ibkr': 'IBKR', 'note': 'Note', 'other': 'Step'}
    for x in steps:
        d = x['detail'] or ''
        if x['kind'] == 'fetch':
            body = f'<b>{e(domain(d))}</b> <a href="{e(d)}" rel=noreferrer class=mute style="word-break:break-all">{e(d)}</a>'
        elif x['kind'] == 'search':
            body = f'“{e(d)}”'
        elif x['kind'] == 'found':
            body = '<ul style="margin:4px 0">' + ''.join(
                f'<li><b>{e(domain(l["url"]))}</b> <a href="{e(l["url"])}" rel=noreferrer>{e(l["title"])}</a></li>'
                for l in json.loads(d)) + '</ul>'
        elif x['kind'] == 'note':
            body = (f'<span class=md>{e(d)}</span>' if len(d) < 400 else
                    f'<details><summary>{e(d[:160])}…</summary><p class=md>{e(d)}</p></details>')
        else:
            body = f'<b>{e(x["tool"] or "")}</b> <span class=mute style="overflow-wrap:anywhere">{e(d)}</span>'  # long JSON arguments
        h += (f'<div class=card style="padding:8px 12px"><span class="v" style="background:var(--btn)">'
              f'{label[x["kind"]]}</span> {body}</div>')
    if not steps:
        h += '<p class=mute>No steps recorded for this run (it ran before recording started).</p>'
    return page('Trace', h)


# ---------------------------------------------------------------- login
FAILS = {}  # client address -> failure times; in memory, so a restart clears lockouts


check_password = webauth.check_password


def password_page(msg=None, ok=False):
    h = ('<div class=card style="max-width:380px;margin:20px auto"><h1 style="margin-top:0">Change password</h1>'
         + (f'<p class="{"up" if ok else "err"}">{e(msg)}</p>' if msg else '') +
         '<form method=post action="/password">'
         '<label for=o>Current password</label><input id=o name=old type=password autocomplete=current-password required>'
         f'<label for=n>New password (at least {webauth.MIN_LEN} characters)</label>'
         f'<input id=n name=new type=password autocomplete=new-password minlength={webauth.MIN_LEN} required>'
         '<label for=r>New password again</label><input id=r name=again type=password autocomplete=new-password required>'
         '<p><button style="width:100%" data-busy="Saving…">Change password</button></p></form>'
         '<p class=mute>One password per person: it also changes on the status page.</p></div>')
    return page('Change password', h)


def new_session(c, user):
    token = secrets.token_urlsafe(32)
    c.execute('DELETE FROM sessions WHERE expires < ?', (time.time(),))
    c.execute('INSERT INTO sessions VALUES (?,?,?)', (token, user, time.time() + SESSION_TTL))
    c.commit()
    return token


def session_user(c, token):
    r = token and c.execute('SELECT user, expires FROM sessions WHERE token=?', (token,)).fetchone()
    if not r or r['expires'] < time.time():
        return None
    if r['expires'] - time.time() < SESSION_TTL - 3600:  # slide, at most one write an hour
        c.execute('UPDATE sessions SET expires=? WHERE token=?', (time.time() + SESSION_TTL, token))
        c.commit()
    return r['user']


def safe_next(n):
    return n if n and n.startswith('/') and not n.startswith('//') and '\\' not in n else '/'


def login_page(nxt='/', err=None):
    h = ('<div class=card style="max-width:380px;margin:40px auto"><h1 style="margin-top:0">Sign in</h1>'
         + (f'<p class=err>{e(err)}</p>' if err else '') +
         f'<form method=post action="/login"><input type=hidden name=next value="{e(nxt)}">'
         '<label for=u>Username</label><input id=u name=user autocomplete=username autocapitalize=none '
         'autocorrect=off spellcheck=false required autofocus>'
         '<label for=p>Password</label><input id=p name=password type=password autocomplete=current-password required>'
         '<p><button style="width:100%" data-busy="Signing in…">Sign in</button></p></form>'
         '<p class=mute>Same username and password as the status page.</p></div>')
    return page('Sign in', h, nav=False)


class H(BaseHTTPRequestHandler):
    def send(self, body, code=200):
        b = body.encode()
        self.send_response(code)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(b)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(b)

    def go(self, where, cookie=None):
        self.send_response(303)
        self.send_header('Location', where)
        if cookie is not None:
            self.send_header('Set-Cookie', cookie)
        self.end_headers()

    def token(self):
        m = SimpleCookie(self.headers.get('Cookie') or '').get('sid')
        return m.value if m else None

    def signed_in(self, c, user):
        """Remember who is asking, for the pages and checks that differ by person."""
        m = SimpleCookie(self.headers.get('Cookie') or '').get('profile')
        CTX.user, CTX.profile = user, current_profile(c, user, m.value if m else None)

    def json(self, obj):
        b = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def ip(self):
        return (self.headers.get('X-Forwarded-For') or self.client_address[0]).split(',')[0].strip()

    def handle_one_request(self):
        # Each request opens its own connection (self.c); close it here so open files cannot pile up to the 1024 limit.
        self.c = None
        try:
            super().handle_one_request()
        finally:
            if self.c:
                self.c.close()

    def do_GET(self):
        c = self.c = db()
        path = self.path.split('?')[0]
        if path == '/login':
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            return self.send(login_page(safe_next((q.get('next') or ['/'])[0])))
        self.user = session_user(c, self.token())
        CTX.user = None
        if not self.user:
            return self.go('/login?next=' + urllib.parse.quote(self.path, safe=''))
        self.signed_in(c, self.user)
        q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).items()}
        if not family() and (path == '/usage' or path.startswith('/tax')):
            return self.send(page('Not found', '<p>Not found. <a href="/">Home</a></p>'), 404)
        if path == '/search':
            try:
                return self.json(search_symbols(q.get('q', '')[:40]) if q.get('q', '').strip() else [])
            except Exception:
                return self.json([])
        if path == '/quote':
            try:
                return self.json({'price': quote(q.get('s', '').upper()[:12])})
            except Exception:
                return self.json({'price': None})
        if path == '/':
            return self.send(home(c, self.user))
        if path == '/brief':
            b = c.execute('SELECT * FROM reports WHERE account_id IS NULL AND grp IS NULL ORDER BY id DESC LIMIT 1').fetchone()
            return self.send(page('Market brief', '<p><a href="/">← all accounts</a></p>' + (
                f'<h1>Market brief {b["run_date"]}</h1>'
                + (f'<p><a href="/trace/{b["usage_id"]}">How this was made</a></p>' if b['usage_id'] else '')
                + f'<div class="card prose">{md(b["body"])}</div>' if b else 'none yet')))
        if path == '/new':
            return self.send(sandbox_form(c))
        if path == '/password':
            return self.send(password_page())
        if path == '/usage':
            return self.send(usage_page(c))
        if path == '/progress':
            return self.json(progress_view())
        if path == '/research':
            return self.send(research_page(c))
        if path == '/learn':
            return self.send(learn_page(c))
        if path == '/dividends':
            return self.send(dividends_page(c, CTX.profile))
        if path == '/how':
            return self.send(how_page(c))
        m = re.fullmatch(r'/trace/(\d+)', path)
        u = m and c.execute('SELECT account_id FROM usage WHERE id=?', (int(m[1]),)).fetchone()
        if u and (u['account_id'] is None or can_see(self.user, c.execute('SELECT * FROM accounts WHERE id=?', (u['account_id'],)).fetchone())) \
                and (t := trace_page(c, int(m[1]))):
            return self.send(t)
        owner = q.get('owner') if q.get('owner') in CONNECTORS else None
        if path == '/tax' and owner:
            return self.send(tax_page(c, owner))
        if path == '/tax/new' and owner:
            return self.send(lot_form(c, owner))
        if path == '/tax.csv' and owner:
            b = tax.to_csv(tax.rows(c, owner), q.get('fy')).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', f'attachment; filename="capital-gains-{owner}-{q.get("fy", "all")}.csv"')
            self.send_header('Content-Length', str(len(b)))
            self.end_headers()
            return self.wfile.write(b)
        m = re.fullmatch(r'/tax/lot/(\d+)', path)
        lot = m and c.execute('SELECT * FROM lots WHERE id=?', (int(m[1]),)).fetchone()
        if lot:
            return self.send(lot_form(c, lot['owner'], lot))
        m = re.fullmatch(r'/a/(\d+)(/edit)?', path)
        a = m and c.execute('SELECT * FROM accounts WHERE id=?', (int(m[1]),)).fetchone()
        a = a if can_see(self.user, a) else None
        if a and not m[2]:
            return self.send(account_page(c, a))
        if a and a['mode'] == 'SANDBOX':
            return self.send(sandbox_form(c, a))
        self.send(page('Not found', '<p>Not found. <a href="/">Home</a></p>'), 404)

    def do_POST(self):
        # The session cookie is SameSite=Lax, and the Origin check on top makes a POST mean
        # "clicked on this page", not "sent by some other site the browser happened to visit".
        if self.headers.get('Origin') != ORIGIN:
            return self.send(page('Refused', '<p>Refused: request did not come from this site.</p>'), 403)
        n = int(self.headers.get('Content-Length') or 0)
        f = {k: v[0] for k, v in urllib.parse.parse_qs(self.rfile.read(n).decode()).items()}
        c = self.c = db()
        path = self.path
        if path == '/login':
            nxt, ip, t = safe_next(f.get('next')), self.ip(), time.time()
            fails = FAILS[ip] = [x for x in FAILS.get(ip, []) if t - x < LOCKOUT[1]]
            if len(fails) >= LOCKOUT[0]:
                return self.send(login_page(nxt, 'Too many failed attempts. Try again in 15 minutes.'), 429)
            if not check_password(f.get('user', ''), f.get('password', ''), 'stock'):
                fails.append(t)
                time.sleep(1)
                return self.send(login_page(nxt, 'Wrong username or password.'), 401)
            FAILS.pop(ip, None)
            return self.go(nxt, f'sid={new_session(c, f["user"])}; Path=/; Max-Age={SESSION_TTL}; '
                                'HttpOnly; Secure; SameSite=Lax')
        if path == '/logout':
            c.execute('DELETE FROM sessions WHERE token=?', (self.token() or '',))
            c.commit()
            return self.go('/login', 'sid=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax')
        user = session_user(c, self.token())
        CTX.user = None
        if not user:
            return self.send(page('Signed out', '<p>Your session has ended. <a href="/login">Sign in again</a>.</p>', nav=False), 401)
        self.signed_in(c, user)
        if not family() and (path in ('/refresh', '/model', '/run', '/learn/review') or path.startswith(('/tax', '/lesson/'))):
            return self.send(page('Refused', '<p>Refused: that is for the account holders.</p>'), 403)
        if path == '/profile':
            p = f.get('p', '')
            return self.go('/', f'profile={p}; Path=/; Max-Age={SESSION_TTL}; HttpOnly; Secure; SameSite=Lax'
                           if user in ADMINS and re.fullmatch(r'[a-z0-9_-]{1,32}', p) else None)
        if path == '/password':
            if not check_password(user, f.get('old', ''), 'stock'):
                time.sleep(1)
                return self.send(password_page('Current password is wrong.'), 401)
            if f.get('new') != f.get('again'):
                return self.send(password_page('The two new passwords differ.'), 400)
            try:
                webauth.set_password(user, f['new'])
            except ValueError as ex:
                return self.send(password_page(str(ex)), 400)
            c.execute('DELETE FROM sessions WHERE user=? AND token<>?', (user, self.token()))
            c.commit()
            return self.send(password_page('Password changed. Other devices are signed out.', ok=True))
        if path == '/refresh':
            ids = [r[0] for r in c.execute('SELECT DISTINCT account_id FROM proposals WHERE status="drafted"')]
            lives = [a for a in c.execute('SELECT * FROM accounts WHERE mode="LIVE"') if a['id'] in ids or not ids]
            errs = refresh_live(c, lives, 'DAYS_7') if lives else {}
            if errs:
                return self.send(page('Refresh failed', '<p class=err>' + e('; '.join(str(x) for x in errs.values())) +
                                      '</p><p><a href="/">Back</a></p>'), 502)
            back = urllib.parse.urlsplit(self.headers.get('Referer') or '').path
            return self.go(back if re.fullmatch(r'/(a/\d+)?', back) else '/')
        if path == '/model':
            m = f.get('m', '')
            if m in MODELS:
                c.execute('INSERT OR REPLACE INTO meta VALUES ("analysis_model", ?)', (m,))
                c.commit()
            return self.go('/')
        if path == '/prices':
            QUOTES.clear()
            back = urllib.parse.urlsplit(self.headers.get('Referer') or '').path
            return self.go(back if re.fullmatch(r'/(a/\d+)?', back) else '/')
        if path == '/run':
            if not running():
                subprocess.run(['systemctl', 'start', '--no-block', 'stock-analyse.service'])
            return self.go('/')
        if path == '/new':
            try:
                return self.go(f'/a/{save_sandbox(c, f, user=user, profile=CTX.profile)}')
            except Exception as ex:
                return self.send(sandbox_form(c, err=str(ex)), 400)
        if path.startswith('/tax/new?owner='):
            owner = path.split('=', 1)[1]
            if owner in CONNECTORS:
                try:
                    save_lot(c, owner, f)
                    return self.go(f'/tax?owner={owner}')
                except Exception as ex:
                    return self.send(lot_form(c, owner, err=str(ex)), 400)
        m = re.fullmatch(r'/tax/lot/(\d+)(/split|/delete)?', path)
        lot = m and c.execute('SELECT * FROM lots WHERE id=?', (int(m[1]),)).fetchone()
        if lot:
            try:
                if m[2] == '/delete':
                    c.execute('DELETE FROM lots WHERE id=?', (lot['id'],))
                elif m[2] == '/split':
                    q = float(f.get('qty', '').replace(',', ''))
                    if lot['sale_date'] or not 0 < q < lot['buy_qty']:
                        raise ValueError(f'split needs an unsold parcel and 0 < shares < {lot["buy_qty"]:g}')
                    share = q / lot['buy_qty']
                    c.execute('INSERT INTO lots (owner, currency, symbol, buy_date, buy_qty, buy_price, buy_fee, source, note) '
                              'SELECT owner, currency, symbol, buy_date, ?, buy_price, buy_fee*?, source, note FROM lots WHERE id=?',
                              (q, share, lot['id']))
                    c.execute('UPDATE lots SET buy_qty=buy_qty-?, buy_fee=buy_fee*? WHERE id=?', (q, 1 - share, lot['id']))
                else:
                    save_lot(c, lot['owner'], f, lot['id'])
                c.commit()
                return self.go(f'/tax?owner={lot["owner"]}')
            except Exception as ex:
                return self.send(lot_form(c, lot['owner'], lot, str(ex)), 400)
        m = re.fullmatch(r'/lesson/(\d+)/(keep|discard|retire)', path)
        if m:
            if m[2] == 'keep' and c.execute('SELECT COUNT(*) FROM lessons WHERE status="active"').fetchone()[0] >= MAX_ACTIVE_LESSONS:
                return self.send(learn_page(c, f'{MAX_ACTIVE_LESSONS} lessons are already in use: retire one first.'), 400)
            new, old = {'keep': ('active', 'proposed'), 'discard': ('discarded', 'proposed'), 'retire': ('retired', 'active')}[m[2]]
            c.execute('UPDATE lessons SET status=?, decided_by=?, decided_at=? WHERE id=? AND status=?', (new, user, now(), int(m[1]), old))
            c.commit()
            return self.go('/learn')
        if path == '/learn/review':
            subprocess.run(['systemd-run', '--no-block', '--collect', '--unit=stock-review-now', '-p', 'MemoryMax=800M',
                            '/usr/bin/python3', SELF, 'review'])
            return self.go('/learn')
        m = re.fullmatch(r'/p/(\d+)/(approve|reject)', path)
        if m and not can_see(user, c.execute('SELECT a.* FROM proposals p JOIN accounts a ON a.id=p.account_id WHERE p.id=?',
                                             (int(m[1]),)).fetchone()):
            return self.send(page('Not found', 'Not found'), 404)
        if m:
            a, err = decide(c, int(m[1]), m[2] == 'approve', user, f.get('reason'))
            if not a:
                return self.send(page('Not found', e(err)), 404)
            back = urllib.parse.urlsplit(self.headers.get('Referer') or '').path
            return self.send(account_page(c, a, err)) if err else self.go(back if re.fullmatch(r'/a/\d+', back) else f'/a/{a["id"]}')
        m = re.fullmatch(r'/a/(\d+)/(edit|refresh|run|deep|delete|trade|cash)', path)
        a = m and c.execute('SELECT * FROM accounts WHERE id=?', (int(m[1]),)).fetchone()
        if not can_see(user, a) or (m[2] == 'deep' and not family()):
            return self.send(page('Not found', 'Not found'), 404)
        if m[2] in ('trade', 'cash') and a['mode'] == 'SANDBOX':
            try:
                if m[2] == 'trade':
                    manual_trade(c, a, f, user)
                else:
                    top_up(c, a, float(f.get('amount', '').replace(',', '').replace('$', '') or 'nan'))
            except Exception as ex:
                c.rollback()
                return self.send(account_page(c, a, str(ex)), 400)
        if m[2] == 'refresh' and a['mode'] == 'LIVE':
            err = refresh_live(c, [a]).get(a['id'])
            if err:
                return self.send(account_page(c, a, f'refresh failed: {err}'))
        elif m[2] == 'edit' and a['mode'] == 'SANDBOX':
            try:
                save_sandbox(c, f, a['id'])
            except Exception as ex:
                return self.send(sandbox_form(c, a, str(ex)), 400)
        elif m[2] in ('run', 'deep') and not running():
            subprocess.run(['systemd-run', '--no-block', '--collect', f'--unit=stock-analyse-a{a["id"]}',
                            '-p', 'MemoryMax=800M', '/usr/bin/python3', SELF, 'analyse', str(a['id']),
                            *(['deep'] if m[2] == 'deep' else [])])
        elif m[2] == 'delete' and a['mode'] == 'SANDBOX':
            for t in ('positions', 'proposals', 'reports', 'snapshots'):
                c.execute(f'DELETE FROM {t} WHERE account_id=?', (a['id'],))
            c.execute('DELETE FROM accounts WHERE id=?', (a['id'],))
            c.commit()
            return self.go('/')
        self.go(f'/a/{a["id"]}')

    def log_message(self, *a):
        pass


# ---------------------------------------------------------------- self-check
def test():
    global DB
    assert [safe_next(x) for x in ('/a/1', '//evil.com', 'http://evil.com', '/\\evil', None)] == ['/a/1', '/', '/', '/', '/']
    import tempfile
    DB = os.path.join(tempfile.mkdtemp(), 't.db')
    c = db()
    QUOTES.update({'AAA': (0, 100.0, 95.0), 'AUDUSD=X': (0, 0.5)})
    assert quote('AAA') == 100.0 and prev_close('AAA') == 95.0 and prev_close('AUDUSD=X') is None
    # The chart read: a steady climb is an uptrend at a new high; a steady fall a downtrend.
    up = signals([100 * 1.002 ** i for i in range(260)])
    assert up['setup'].startswith('uptrend') and up['breakout'] == 'new 55-day high' and up['mom_12_1_pct'] > 0, up
    dn = signals([100 * 0.998 ** i for i in range(260)])
    assert dn['setup'].startswith('downtrend') and dn['rsi14'] == 0, dn
    # Day move: 10 held from the 95 close, 5 bought today at 98, 2 sold today at 99; price now 100.
    F = lambda side, q, x: {'side': side, 'qty': q, 'price': x}
    chg, base = day_move(13, 100.0, 95.0, [F('BUY', 5, 98.0), F('SELL', 2, 99.0)])
    assert (chg, base) == (10 * 5 + 5 * 2 + 2 * 4, 10 * 95 + 5 * 98 + 2 * 95), (chg, base)
    assert day_move(5, 100.0, 95.0, [F('BUY', 5, 98.0)]) == (10.0, 490.0)  # bought today: from its fill, not the close
    aid = save_sandbox(c, {'name': 'T', 'cash': '1000', 'positions': 'AAA 10 50'})
    prop = lambda side, q, t='MARKET', lim=None: {'account_id': aid, 'symbol': 'AAA', 'qty': q,
                                                  'side': side, 'order_type': t, 'limit_price': lim}
    assert ibkr_fee(10, 100) == 1.0 and ibkr_fee(1000, 100) == 5.0 and ibkr_fee(1, 0.5) == 0.01
    fill(c, prop('BUY', 5), 100)                            # cost 501
    cash = c.execute('SELECT cash FROM accounts WHERE id=?', (aid,)).fetchone()[0]
    pos = c.execute('SELECT qty, avg_price FROM positions').fetchone()
    assert abs(cash - 499) < 1e-9 and pos[0] == 15 and abs(pos[1] - (500 + 501) / 15) < 1e-9
    for bad in (prop('BUY', 5), prop('SELL', 16), prop('BUY', 1, 'LIMIT', 99)):
        try:
            fill(c, bad, 100)
            raise AssertionError(f'should refuse {bad}')
        except ValueError:
            pass
    assert fill(c, prop('SELL', 15), 100)[1] == 1499 - 1001  # proceeds 1499 against cost 1001
    assert abs(c.execute('SELECT cash FROM accounts WHERE id=?', (aid,)).fetchone()[0] - 1998) < 1e-9
    assert not c.execute('SELECT 1 FROM positions').fetchone()
    s = state(c, c.execute('SELECT * FROM accounts WHERE id=?', (aid,)).fetchone())
    assert abs(s['value'] - 1998) < 1e-9 and s['aud_per_usd'] == 2
    st = {'J': {'cash': 1000, 'value': 2500, 'positions': [{'symbol': 'AAA', 'qty': 10, 'value': 1500}]},
          'A': {'cash': 50000, 'value': 50500, 'positions': [{'symbol': 'BBB', 'qty': 5, 'value': 500}]}}
    P = lambda n, side, sym, q, px: {'account': n, 'side': side, 'symbol': sym, 'quantity': q, 'order_type': 'LIMIT',
                                     'limit_price': px, 'ref_price': px}
    got = [(p['account'], p['side'], p['symbol'], s_, (n_ or '')[:20]) for p, s_, n_ in check_proposals([
        P('J', 'BUY', 'CCC', 10, 200),     # J: 1,000 cash + this batch's 1,500 sale = ~2,500, so 2,001 fits
        P('J', 'SELL', 'AAA', 10, 150),
        P('J', 'BUY', 'DDD', 10, 200),     # a second 2,001 does not, though A has 50,000 idle
        P('A', 'BUY', 'AAA', 1, 100),      # A buying what J holds: allowed, with a warning
        P('A', 'SELL', 'AAA', 1, 100),     # A does not hold AAA
        P('X', 'BUY', 'EEE', 1, 1),
        P('J', 'SELL', 'AAA', 1, 150),     # J's 10 AAA are already sold above
        P('A', 'BUY', 'FFF', 150, 100),    # 15,001 is 28% of the 53,000 total: over the 25% limit
        P('A', 'BUY', 'GGG', 100, 100)],   # 10,001 fits, but is far above its risk-based size and before results
        st, {'GGG': {'size_usd': 5000, 'results': '2026-10-05'}}, '2026-10-01')]
    assert got == [('J', 'BUY', 'CCC', 'pending', ''), ('J', 'SELL', 'AAA', 'pending', ''),
                   ('J', 'BUY', 'DDD', 'invalid', "J's account cannot p"), ('A', 'BUY', 'AAA', 'pending', 'Warning: AAA would t'),
                   ('A', 'SELL', 'AAA', 'invalid', 'A holds 0 AAA'), ('X', 'BUY', 'EEE', 'invalid', 'unknown account'),
                   ('J', 'SELL', 'AAA', 'invalid', 'J holds 10 AAA (less'), ('A', 'BUY', 'FFF', 'invalid', 'FFF would be 28% of t'[:20]),
                   ('A', 'BUY', 'GGG', 'pending', 'Warning: $10,000.00 ')], got
    st4 = list(check_proposals([P('A', 'BUY', 'GGG', 1, 100), dict(P('A', 'BUY', 'HHH', 1, 0), order_type='MARKET')], st, {'GGG': {'stage': 4}}, '2026-10-01'))
    assert [(s_, n_[:30]) for _, s_, n_ in st4] == [('invalid', 'GGG is in Weinstein stage 4 (b'), ('invalid', 'no price to judge the order by')], st4
    mk = dict(P('A', 'BUY', 'GGG', 1, 100), order_type='MARKET')
    assert 'major policy shock' in list(check_proposals([mk], st, {}, '2026-10-01', 'major'))[0][2]
    gnote = [n_ for p, _, n_ in check_proposals([P('A', 'BUY', 'GGG', 100, 100)], st,
                                                 {'GGG': {'size_usd': 5000, 'results': '2026-10-05'}}, '2026-10-01')][0]
    assert 'risk-based size' in gnote and 'results are due 2026-10-05' in gnote, gnote
    t = trend_stats([100.0] * 200 + [110.0] * 52)
    assert t['last'] == 110 and t['chg_1m_pct'] == 0 and t['chg_3m_pct'] == 10.0 and t['high_52w'] == 110
    assert t['pct_from_high'] == 0 and t['vs_sma200_pct'] == round(100 * (110 / ((148 * 100 + 52 * 110) / 200) - 1), 1)
    h = md('# Title\n\nSome **bold** and a [link](https://x.com/a?b=1) <script>.\n\n| | A |\n|---|---|\n| r | 1 |\n\n- one\n- two')
    assert h == ('<h2>Title</h2><p>Some <b>bold</b> and a <a href="https://x.com/a?b=1" rel=noreferrer>link</a> &lt;script&gt;.</p>'
                 '<div class=scroll><table><tr><th></th><th>A</th></tr><tr><td>r</td><td>1</td></tr></table></div>'
                 '<ul><li>one</li><li>two</li></ul>'), h
    assert 'href="javascript' not in md('[x](javascript:alert(1))')
    assert md_inline('SPY -0.16% and +1.93%, range 3.75-4.00%, [a](https://x.com/?p=-1)') == (
        'SPY <span class=dn>-0.16%</span> and <span class=up>+1.93%</span>, range 3.75-4.00%, '
        '<a href="https://x.com/?p=-1" rel=noreferrer>a</a>')
    c.execute("INSERT INTO proposals (id, account_id, run_date, symbol, side, qty, order_type, status, decided_at) "
              "VALUES (900, ?, '2026-09-28', 'CVS', 'BUY', 170, 'LIMIT', 'drafted', '2026-09-29T00:03:53')", (aid,))
    tj = lambda *ts: json.dumps({'trades': [{'trade_id': i, 'symbol': 'CVS', 'side': 'BUY', 'size': q, 'price': 10.0,
                                             'commission': 1, 'sec_type': 'STK', 'trade_time': t, 'order_id': t}  # one order per time
                                            for i, q, t in ts]})
    assert match_fills(c, aid, tj(('old', 170, '2026-09-27T14:00:00Z'), ('a', 45, '2026-09-28T14:06:24Z'))) == 0
    assert 'partly' in c.execute('SELECT note FROM proposals WHERE id=900').fetchone()[0]
    assert match_fills(c, aid, tj(('a', 45, '2026-09-28T14:06:24Z'), ('b', 125, '2026-09-28T14:06:24Z'))) == 1
    assert tuple(c.execute('SELECT status, fill_price, fee FROM proposals WHERE id=900').fetchone()) == ('filled', 10.0, 2.0)
    # 147 drafted and rejected in IBKR, then 110 drafted and filled: the 110 fills, the 147 closes, nothing is partial
    c.execute("INSERT INTO proposals (id, account_id, run_date, symbol, side, qty, order_type, status, decided_at) VALUES "
              "(901, ?, '2026-09-29', 'CVS', 'BUY', 147, 'LIMIT', 'drafted', '2026-09-29T23:49:18'), "
              "(902, ?, '2026-09-29', 'CVS', 'BUY', 110, 'LIMIT', 'drafted', '2026-09-29T23:53:15')", (aid, aid))
    assert match_fills(c, aid, tj(('c', 110, '2026-09-29T14:00:00Z'))) == 1
    assert [tuple(r) for r in c.execute('SELECT id, status FROM proposals WHERE id IN (901, 902) ORDER BY id')] == [
        (901, 'expired'), (902, 'filled')]
    pl = {'horizon_months': 12, 'target_price': 150, 'stop_price': 80, 'exit_if': 'x', 'review_by': '2027-01-01'}
    assert plan_triggers(pl, '2026-01-31', 100, '2026-09-29') == []
    assert plan_triggers(pl, '2025-09-29', 160, '2027-01-01') == ['reached its target $150.00 (now $160.00)',
                                                                   'review date 2027-01-01 reached', 'planned holding period of 12 months is up']
    assert plan_triggers(pl, '2026-09-29', 79, '2026-09-29') == ['below its review level $80.00 (now $79.00)']
    assert cgt_text([('2025-09-01', 10), ('2026-03-01', 5)], '2026-09-29') == '5 of 15 shares qualify from 2027-03-02'
    assert cgt_text([('2024-01-01', 10)], '2026-09-29') == 'all shares qualify for the discount'
    assert cgt_text([('2027-03-10', 1)], '2027-04-01') == '1 of 1 shares qualify from 2028-03-11'  # across 29 Feb 2028
    real = dividends.histories
    dividends.histories = lambda c_, syms, daily=False: {'AAA': {'divs': [['2020-01-01', 9.0], [date.today().isoformat(), 2.0]]}}
    c.execute('INSERT INTO positions VALUES (?,?,?,?)', (aid, 'AAA', 10, 100))
    before = c.execute('SELECT cash FROM accounts WHERE id=?', (aid,)).fetchone()[0]
    assert credit_dividends(c) == 1 and credit_dividends(c) == 0   # today's only (the 2020 one predates the account), once
    assert abs(c.execute('SELECT cash FROM accounts WHERE id=?', (aid,)).fetchone()[0] - before - 17.0) < 1e-9
    dividends.histories = real
    # LIVE by IBKR's time-weighted returns: 1% then 1% (cumulative 1% and 2.01%), and a deposit in the values changes nothing.
    save_perf(c, aid, {'accounts': {'U1': {'base_currency': 'USD', 'periods': {'1M': {'dates': ['20260925', '20260928'],
                                                                                     'cps': [0.01, 0.0201]}}}}})
    g = live_twr(c, ['2026-09-25', '2026-09-29'], {aid: {'2026-09-25': 100.0, '2026-09-29': 999.0}})
    assert abs(g[1] - 1.0201) < 1e-9, g
    assert live_twr(c, ['2026-09-24', '2026-09-29'], {aid: {'2026-09-24': 100.0, '2026-09-29': 999.0}}) is None  # not covered
    # A market order whose price has moved 10% since it was proposed asks once; approving again fills it.
    c.execute("INSERT INTO proposals (id, account_id, run_date, symbol, side, qty, order_type, ref_price, status) "
              "VALUES (950, ?, '2026-10-01', 'AAA', 'BUY', 1, 'MARKET', 90.0, 'pending')", (aid,))
    real_quote = globals()['quote']
    globals()['quote'] = lambda sym: 100.0
    _, err = decide(c, 950, True, 'alex')
    assert 'moved +11.1%' in err and c.execute('SELECT status FROM proposals WHERE id=950').fetchone()[0] == 'pending', err
    assert decide(c, 950, True, 'alex')[1] is None
    assert c.execute('SELECT status, fill_price FROM proposals WHERE id=950').fetchone()[:] == ('filled', 100.0)
    globals()['quote'] = real_quote
    # Proposals carry the brief's policy-shock verdict for their day, for the Learning page and the review.
    c.execute("INSERT INTO policy_days VALUES ('2026-10-01', 'major', 'tariffs')")
    assert {p['id']: p['policy'] for p in learn.rows(c)}.get(950) == 'major'
    assert '{' not in BRIEF_PROMPT.format(today='x', indexes='', macro='', links='', sectors='')
    assert 'policy' in NOTE['required']
    assert [withholding_rate(x) for x in ('15% US treaty rate with W-8BEN', 'unverified: Germany withholds 26.375%; reclaim 15%',
                                          'free of Swiss withholding tax, so 0%', 'unverified: No Dutch withholding since 2022',
                                          'Pays no dividend, so nothing is withheld.', 'unverified: no source', None)] == \
        [0.15, 0.26375, 0.0, 0.0, 0.0, 0.15, 0.15]
    NYt = lambda *a: datetime(*a, tzinfo=learn.NY)
    k = market_clock(NYt(2026, 10, 1, 10, 0))
    assert k['open'] and k['end'] == NYt(2026, 10, 1, 16, 0), k
    k = market_clock(NYt(2026, 10, 2, 17, 0))  # Friday after the close: next open is Monday
    assert not k['open'] and k['state'] == 'After hours' and k['end'] == NYt(2026, 10, 5, 9, 30), k
    k = market_clock(NYt(2026, 11, 26, 12, 0))  # Thanksgiving; the day after closes at 1 pm
    assert k['state'] == 'Closed for Thanksgiving' and k['start'] == NYt(2026, 11, 25, 16, 0), k
    assert market_clock(NYt(2026, 11, 27, 12, 0))['end'] == NYt(2026, 11, 27, 13, 0)
    assert [dur(x) for x in (0.4, 45, 200, 180, 840, 4000)] == ['under a minute', '45 min', '3 h 20 min', '3 h', '14 h', '3 days']
    note = 'Market brief for 1 Oct 2026 written (about 600 words).'
    real = '# Brief\n\n| Index | Last |\n|---|---|\n| SPY | 1 |\n' + 'x' * 2500
    assert full_brief(note, ['Thinking about it', real]) == real and full_brief(real, []) == real and full_brief(note, []) == note
    # IBKR alerts: a failure shows a banner naming what to do, and the next success clears it.
    c.execute("UPDATE accounts SET snapshot_at='2026-10-01T23:50:39' WHERE mode='LIVE' AND owner='sam'")
    ibkr_status(c, 'sam', 'sign-in')
    assert 'Sam needs to sign in to IBKR again' in ibkr_alert(c) and 'from 1 Oct 23:50' in ibkr_alert(c)
    ibkr_status(c, 'sam', None)
    assert ibkr_alert(c) == ''
    global AUTH_CACHE
    real_cache, AUTH_CACHE = AUTH_CACHE, os.path.join(os.path.dirname(DB), 'auth.json')
    json.dump({'claude.ai IBKR Sam': {'timestamp': 1}, 'plugin:x': {}}, open(AUTH_CACHE, 'w'))
    assert needs_signin() == {'claude.ai IBKR Sam', 'plugin:x'}
    forget_auth_failures(['sam'])
    assert needs_signin() == {'plugin:x'}
    AUTH_CACHE = real_cache
    # Profiles: a friend sees only their own accounts; Alex sees all and may switch; Sam stays on the family's.
    tid = save_sandbox(c, {'name': 'Chris Sandbox', 'cash': '1000'}, user='chris', profile='chris')
    ta = c.execute('SELECT * FROM accounts WHERE id=?', (tid,)).fetchone()
    fa = c.execute('SELECT * FROM accounts WHERE id=?', (aid,)).fetchone()
    assert (ta['owner'], ta['profile'], fa['profile']) == ('chris', 'chris', 'family')
    assert can_see('chris', ta) and not can_see('chris', fa) and can_see('alex', ta) and can_see('sam', fa) and not can_see('sam', ta)
    assert current_profile(c, 'alex', 'chris') == 'chris' and current_profile(c, 'chris', 'family') == 'chris'
    assert current_profile(c, 'sam', 'chris') == 'family' and current_profile(c, 'alex', 'nobody') == 'family'
    assert save_sandbox(c, {'name': 'X', 'cash': '5', 'copy': '1'}, user='chris', profile='chris')  # copying LIVE is ignored
    # By hand: buy 5 at the typed price 90 (fee 1), sell 2 at the current 100 (fee 1, gain 2 * (100 - 451/5) - 1).
    globals()['quote'] = lambda sym: 100.0
    manual_trade(c, ta, {'symbol': 'aaa', 'side': 'BUY', 'qty': '5', 'price': '90'}, 'chris')
    manual_trade(c, ta, {'symbol': 'AAA', 'side': 'SELL', 'qty': '2', 'price': ''}, 'chris')
    rows = c.execute('SELECT side, fill_price, fee, realised, status FROM proposals WHERE account_id=? ORDER BY id', (tid,)).fetchall()
    assert [tuple(r) for r in rows] == [('BUY', 90.0, 1.0, None, 'manual'), ('SELL', 100.0, 1.0, 2 * (100 - 451 / 5) - 1)
                                        + ('manual',)], [tuple(r) for r in rows]
    assert abs(c.execute('SELECT cash FROM accounts WHERE id=?', (tid,)).fetchone()[0] - (1000 - 451 + 199)) < 1e-9
    assert learn.klass({'grp': 'a1', 'run_date': '2026-10-05', 'status': 'manual'}) is None  # never scored as advice
    for bad in ({'symbol': 'AAA', 'side': 'SELL', 'qty': '9'}, {'symbol': 'AAA', 'side': 'BUY', 'qty': '0'},
                {'symbol': '', 'side': 'BUY', 'qty': '1'}, {'symbol': 'AAA', 'side': 'X', 'qty': '1'}):
        try:
            manual_trade(c, ta, bad, 'chris')
            raise AssertionError(f'should refuse {bad}')
        except ValueError:
            c.rollback()
    # A top-up scales the past values, so a 50% deposit is not a 50% gain.
    c.execute('INSERT INTO snapshots VALUES (?,?,?,?)', (tid, '2026-10-01', 1000.0, 500.0))
    ta = c.execute('SELECT * FROM accounts WHERE id=?', (tid,)).fetchone()
    v = state(c, ta)['value']
    top_up(c, ta, v / 2)
    assert abs(c.execute('SELECT value_usd FROM snapshots WHERE account_id=?', (tid,)).fetchone()[0] - 1500) < 1e-9
    assert abs(state(c, c.execute('SELECT * FROM accounts WHERE id=?', (tid,)).fetchone())['value'] - 1.5 * v) < 1e-9
    globals()['quote'] = real_quote
    # Chris's Learning chart: his sandbox against his frozen account, on the days both have a value.
    fid = save_sandbox(c, {'name': 'Chris Frozen', 'cash': '2000', 'frozen': '1'}, user='chris', profile='chris')
    c.execute('DELETE FROM accounts WHERE profile="chris" AND id NOT IN (?,?)', (tid, fid))
    assert profile_series(c, 'chris') == ([], {'advisor': [], 'fixed': []}, [])
    c.executemany('INSERT INTO snapshots VALUES (?,?,?,?)', [(fid, '2026-10-01', 2000.0, 500.0), (tid, '2026-10-02', 1600.0, 510.0),
                                                            (fid, '2026-10-02', 2100.0, 510.0)])
    days, vals, spy = profile_series(c, 'chris')
    assert days == ['2026-10-01', '2026-10-02'] and vals == {'advisor': [1500.0, 1600.0], 'fixed': [2000.0, 2100.0]}, (days, vals)
    assert abs(friends_bench(c)['chris']['advisor'] - 1 / 15) < 1e-9 and 'Chris: can the advisor' in learn_page(c)
    # His dividends: what was credited to his cash, gross, and his holdings dated by their first buy.
    c.execute('INSERT INTO sandbox_divs VALUES (?,?,?,?,?,?)', (tid, 'AAA', '2026-10-03', 3, 0.5, 1.275))
    lots, pays = friend_lots(c, 'chris')
    assert [(l['owner'], l['symbol'], l['buy_qty'], l['buy_date']) for l in lots] == [('Chris Sandbox', 'AAA', 3, date.today().isoformat())]
    assert [(p['owner'], p['amount']) for p in pays] == [('Chris Sandbox', 1.5)]
    tok = new_session(c, 'alex')
    assert session_user(c, tok) == 'alex' and session_user(c, 'nope') is None and session_user(c, None) is None
    c.execute('UPDATE sessions SET expires=?', (time.time() - 1,))
    assert session_user(c, tok) is None
    print('ok')


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'serve'
    if cmd == 'test':
        test()
        learn.test()
        dividends.test()
        tax.test()
        feeds.test()
        lenses.test()
    elif cmd == 'sync-trades':  # one-off catch-up, e.g. YEAR_TO_DATE
        c = db()
        print(refresh_live(c, c.execute('SELECT * FROM accounts WHERE mode="LIVE"').fetchall(), sys.argv[2]) or 'ok')
    elif cmd == 'plans':  # rewrite every household's exit plans with Opus
        rewrite_plans(sys.argv[2] if len(sys.argv) > 2 else 'opus')
    elif cmd == 'score':
        print(learn.score(db()), 'scores written')
    elif cmd == 'review':
        review()
    elif cmd == 'weekly':  # the Saturday timer: scores every week, the Opus review the first Saturday of the month
        print(learn.score(db()), 'scores written')
        if date.today().day <= 7:
            review()
    elif cmd == 'analyse':
        os.makedirs(HOME, exist_ok=True)
        analyse(int(sys.argv[2]) if len(sys.argv) > 2 else None, deep=sys.argv[3:4] == ['deep'])
    else:
        os.makedirs(HOME, exist_ok=True)
        c = db()
        # An approval cut off by a restart would stay "working" for good: give it back to decide again.
        c.execute('UPDATE proposals SET status="pending", note="An approval was interrupted by a restart: check the IBKR '
                  'app for a draft before approving again." WHERE status="working"')
        c.commit()
        c.close()
        ThreadingHTTPServer((HOST, PORT), H).serve_forever()
