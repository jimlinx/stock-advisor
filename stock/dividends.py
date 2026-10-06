"""Dividends by script: what the family's shares earned, what is coming, and how the dividend picks compare.

Earned dividends are ESTIMATED from the tax register's parcels (`lots`: who held how many shares of what, and when)
and Yahoo's published dividend history: a parcel earns a dividend when it was bought before the ex-dividend date and
not sold before it. This goes back to 2020 without needing IBKR's cash statements. The amounts are gross, before
withholding tax, and grouped by ex-dividend date (payment comes a few weeks later).

ponytail: assumes the register's share counts are split-adjusted like Yahoo's per-share amounts, which the IBKR
import is. A parcel recorded before a split would be off by the split ratio.
"""
import json, statistics, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import feeds

# Well-known dividend funds and long-running dividend growers, to measure the family's picks against.
COMPARE = [('SCHD', 'Schwab US Dividend Equity ETF'), ('VYM', 'Vanguard High Dividend Yield ETF'),
           ('DGRO', 'iShares Core Dividend Growth ETF'), ('NOBL', 'S&P 500 Dividend Aristocrats ETF'),
           ('HDV', 'iShares Core High Dividend ETF'), ('KO', 'Coca-Cola'), ('PG', 'Procter & Gamble'),
           ('JNJ', 'Johnson & Johnson'), ('PEP', 'PepsiCo'), ('O', 'Realty Income')]
BENCH = 'SCHD'
# T-bill funds: parked cash whose payout is interest that follows the Fed's rate, not a dividend pick.
CASH_LIKE = {'BIL', 'SGOV', 'SHV', 'USFR', 'TFLO', 'BILS', 'GBIL'}
INCOME_YIELD = 0.015  # a holding counts as a dividend pick above this trailing yield; below it dividends are incidental
NY = ZoneInfo('America/New_York')


def fetch(sym, daily):
    """{'price', 'currency', 'divs': [[day, amount]], 'closes': [[day, close]]} from Yahoo's chart API. Daily closes
    for ten years only when asked (the comparison funds); held stocks need just the dividends."""
    rng, iv = ('10y', '1d') if daily else ('max', '1mo')
    req = urllib.request.Request(f'https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(feeds.yahoo_symbol(sym))}'
                                 f'?range={rng}&interval={iv}&events=div', headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)['chart']['result'][0]
    day = lambda ts: datetime.fromtimestamp(ts, NY).date().isoformat()
    divs = sorted([day(v['date']), v['amount']] for v in (d.get('events') or {}).get('dividends', {}).values())
    closes = [[day(t), x] for t, x in zip(d.get('timestamp') or [], d['indicators']['quote'][0].get('close') or []) if x] if daily else []
    return {'price': d['meta'].get('regularMarketPrice'), 'currency': d['meta'].get('currency'), 'divs': divs, 'closes': closes}


def histories(c, symbols, daily=False):
    """{symbol: fetch(...)}, cached for the day, 8 at a time."""
    out, need = {}, []
    for s in dict.fromkeys(symbols):
        r = c.execute('SELECT body FROM pages WHERE key=? AND day=?', (f'dv:{int(daily)}:{s}', date.today().isoformat())).fetchone()
        if r:
            out[s] = json.loads(r[0])
        else:
            need.append(s)
    def one(s):
        try:
            return s, fetch(s, daily)
        except Exception:
            return s, None
    with ThreadPoolExecutor(8) as ex:
        for s, h in ex.map(one, need):
            if h:
                out[s] = h
                c.execute('INSERT OR REPLACE INTO pages VALUES (?,?,?)', (f'dv:{int(daily)}:{s}', date.today().isoformat(), json.dumps(h)))
    c.commit()
    return out


def received(lots, divs):
    """Every dividend a parcel earned: [{owner, symbol, day, qty, per_share, amount, currency}]. A parcel earns it when
    bought before the ex-date and still held on it (a sale on the ex-date keeps the dividend)."""
    out = []
    for l in lots:
        for day, amt in divs.get(l['symbol'], []):
            if l['buy_date'] < day and (not l['sale_date'] or l['sale_date'] >= day):
                q = l['sale_qty'] if l['sale_date'] and l['sale_qty'] else l['buy_qty']
                out.append({'owner': l['owner'], 'symbol': l['symbol'], 'day': day, 'qty': q, 'per_share': amt,
                            'amount': q * amt, 'currency': l['currency']})
    return out


def ttm(divs, on):
    """Dividends per share with an ex-date in the 365 days to `on`."""
    start = (date.fromisoformat(on) - timedelta(days=365)).isoformat()
    return sum(a for d, a in divs if start < d <= on)


def growth(divs, today, years=5):
    """Yearly growth of the dividend over `years`, from trailing-year totals; None without that much history."""
    now = ttm(divs, today)
    then = ttm(divs, (date.fromisoformat(today) - timedelta(days=365 * years)).isoformat())
    return (now / then) ** (1 / years) - 1 if now and then else None


def next_ex(divs, announced, today):
    """(day, per share, 'announced' | 'expected') for the next ex-dividend date within 75 days, or None. `announced` is
    Finviz's ex-date, which is the next one once the company has declared it; otherwise the date is projected from
    the usual gap between payments, with the last amount."""
    if not divs:
        return None
    last_day, last_amt = divs[-1]
    if announced and announced > today:
        return announced, last_amt, 'announced'
    last5 = divs[-5:]
    gaps = [g for a, b in zip(last5, last5[1:]) if (g := (date.fromisoformat(b[0]) - date.fromisoformat(a[0])).days) > 0]
    if not gaps or date.fromisoformat(last_day) < date.fromisoformat(today) - timedelta(days=400):
        return None  # stopped paying
    d = date.fromisoformat(last_day) + timedelta(days=round(statistics.median(gaps)))
    while d.isoformat() <= today:
        d += timedelta(days=round(statistics.median(gaps)))
    return (d.isoformat(), last_amt, 'expected') if (d - date.fromisoformat(today)).days <= 75 else None


def total_return(closes, divs, start, end=None):
    """Price change plus dividends (not reinvested) from the first close on or after `start` to the last on or before
    `end` (today when None), as a fraction; None when the history does not cover it."""
    a = next(((d, x) for d, x in closes if d >= start), None)
    b = next(((d, x) for d, x in reversed(closes) if not end or d <= end), None)
    if not a or not b or b[0] < a[0]:
        return None
    return (b[1] + sum(amt for d, amt in divs if a[0] < d <= b[0])) / a[1] - 1


def fy(day):
    """Australian financial year of a date: '2026-27' for 1 Jul 2026 to 30 Jun 2027."""
    y = int(day[:4]) if day[5:7] >= '07' else int(day[:4]) - 1
    return f'{y}-{str(y + 1)[2:]}'


def test():
    L = lambda b, s=None, q=10: {'owner': 'j', 'symbol': 'X', 'buy_date': b, 'sale_date': s, 'buy_qty': q, 'sale_qty': q if s else None,
                                 'currency': 'USD'}
    divs = {'X': [['2025-03-01', 1.0], ['2025-06-01', 1.0], ['2025-09-01', 1.0]]}
    # bought before the first ex-date; sold on the second ex-date (keeps it); bought on the third (does not get it)
    got = [(p['day'], p['amount']) for p in received([L('2025-02-01', '2025-06-01'), L('2025-09-01')], divs)]
    assert got == [('2025-03-01', 10.0), ('2025-06-01', 10.0)], got
    d = [['2020-09-01', 1.0], ['2025-09-01', 2.0]]
    assert ttm(d, '2025-09-26') == 2.0 and abs(growth(d, '2025-09-26') - (2 ** 0.2 - 1)) < 1e-9
    q = [['2026-01-10', 1.0], ['2026-04-10', 1.0], ['2026-07-10', 1.0]]
    assert next_ex(q, None, '2026-09-26') == ('2026-10-08', 1.0, 'expected'), next_ex(q, None, '2026-09-26')
    assert next_ex(q, '2026-10-01', '2026-09-26') == ('2026-10-01', 1.0, 'announced')
    assert next_ex([['2024-01-01', 1.0], ['2024-04-01', 1.0]], None, '2026-09-26') is None
    closes = [['2025-01-02', 100.0], ['2025-06-02', 105.0], ['2026-01-02', 110.0]]
    assert abs(total_return(closes, [['2025-03-01', 2.0]], '2025-01-01') - 0.12) < 1e-9
    assert abs(total_return(closes, [], '2025-01-01', '2025-12-31') - 0.05) < 1e-9
    assert fy('2026-07-01') == '2026-27' and fy('2026-06-30') == '2025-26'
    print('dividends ok')


if __name__ == '__main__':
    test()
