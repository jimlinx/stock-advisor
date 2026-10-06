"""Market data fetched by script, so Claude reads a compact summary instead of searching for it.

Finviz quote pages give each stock's valuation, analyst ratings and dated headlines; Finviz groups
and screener pages give the sector table and candidate lists; MarketBeat gives the day's analyst
upgrades; Yahoo gives daily closes. All are free pages without an API key, parsed from HTML, so a
site redesign breaks a parser: each fetcher returns None or [] on failure and the research prompt
tells Claude to fill the gap itself.

Finnhub (free key, FINNHUB_KEY in .env) gives what the pages do not: the next earnings date, the last
four earnings surprises, how analyst recommendations moved over three months, and insiders' open-market
buys and sales. Without a key those fields are simply left out.

FRED (free key, FRED_KEY in .env) gives the US macro figures for the market brief: inflation, jobs, the Fed
funds rate, the yield curve and credit spreads.

Closes are kept in the database and topped up once a day; quote pages and Finnhub answers are cached for the day.
"""
import html, json, re, statistics, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

NY = ZoneInfo('America/New_York')
UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36'
SCHEMA = '''
CREATE TABLE IF NOT EXISTS closes (symbol TEXT NOT NULL, day TEXT NOT NULL, close REAL NOT NULL,
  PRIMARY KEY (symbol, day));
CREATE TABLE IF NOT EXISTS closes_at (symbol TEXT PRIMARY KEY, day TEXT NOT NULL);  -- last fetch, Sydney date
CREATE TABLE IF NOT EXISTS pages (key TEXT PRIMARY KEY, day TEXT NOT NULL, body TEXT NOT NULL);  -- parsed, per day
CREATE TABLE IF NOT EXISTS facts (symbol TEXT PRIMARY KEY, domicile TEXT, withholding TEXT, at TEXT);  -- rarely change
'''
# Fields worth passing on from a Finviz quote page (stocks and ETFs have different ones).
KEEP = ['P/E', 'Forward P/E', 'PEG', 'EPS next Y', 'EPS next 5Y', 'Dividend TTM', 'Dividend Est.', 'Dividend Ex-Date', 'Payout',
        'Target Price', 'Recom', 'Earnings', 'Market Cap', 'Short Float', 'Beta',
        'Category', 'Expense', 'Return% 1Y', 'Asset Type',
        # quality, growth, positioning and technicals: on the same page, so free to keep
        'P/FCF', 'EV/EBITDA', 'Debt/Eq', 'Current Ratio', 'ROE', 'ROIC', 'Gross Margin', 'Oper. Margin', 'Profit Margin',
        'Sales Q/Q', 'EPS Q/Q', 'Sales Y/Y TTM', 'EPS next Q', 'EPS/Sales Surpr.', 'Dividend Gr. 3/5Y',
        'Insider Own', 'Inst Own', 'Inst Trans', 'Short Ratio', 'RSI (14)', 'ATR (14)', 'Volatility', 'Rel Volume', 'Price',
        # for the investor lenses (Graham, Greenblatt, Piotroski, Lynch, O'Neil)
        'P/B', 'ROA', 'EPS (ttm)', 'Book/sh', 'EPS past 3/5Y', 'EPS Y/Y TTM', 'LT Debt/Eq', 'Income', 'Enterprise Value',
        'Insider Trans']


def setup(c):
    c.executescript(SCHEMA)
    if 'open' not in [r[1] for r in c.execute('PRAGMA table_info(closes)')]:  # daily bars for the candlestick trial
        for col in ('open', 'high', 'low'):
            c.execute(f'ALTER TABLE closes ADD COLUMN {col} REAL')


def get(url, timeout=20):
    req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept-Language': 'en-US'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode('utf-8', 'replace')


def text(x):
    return ' '.join(html.unescape(re.sub(r'<[^>]+>', ' ', x)).split())


def cached(c, key, fn):
    """fn() once per day per key; a failure is not cached, so a later run tries again."""
    today = date.today().isoformat()
    r = c.execute('SELECT body FROM pages WHERE key=? AND day=?', (key, today)).fetchone()
    if r:
        return json.loads(r[0])
    try:
        out = fn()
    except Exception:
        return None
    if out:
        c.execute('INSERT OR REPLACE INTO pages VALUES (?,?,?)', (key, today, json.dumps(out)))
        c.commit()
    return out


# ---------------------------------------------------------------- Finviz
def fv_symbol(sym):
    return sym.strip().replace(' ', '-').replace('.', '-').upper()


def parse_quote(s, today=None):
    today = today or date.today()
    out = {}
    for k, v in re.findall(r'class="snapshot-td-label"[^>]*>(.*?)</div>.*?class="snapshot-td-content"[^>]*>(.*?)</div>', s, re.S):
        k, v = text(k), text(v)
        if k in KEEP and v and v != '-' and k not in out:
            out[k] = v
    for kind, title, name in re.findall(r'f=(sec|ind|geo)_[a-z]+" class="quote-header_category"(?: title="([^"]*)")?>([^<]*)<', s):
        out[{'sec': 'sector', 'ind': 'industry', 'geo': 'country'}[kind]] = html.unescape(title or name).strip()
    ratings = []
    i = s.find('js-table-ratings')
    if i >= 0:
        for row in re.findall(r'<tr[^>]*styled-row[^>]*>(.*?)</tr>', s[i:s.find('</table>', i)], re.S)[:6]:
            cells = [text(x) for x in re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)]
            if len(cells) >= 5:
                try:
                    d = datetime.strptime(cells[0], '%b-%d-%y').date()
                except ValueError:
                    continue
                if (today - d).days <= 60:
                    ratings.append(f'{d} {cells[1]} {cells[2]}: {cells[3]}' + (f', target {cells[4]}' if cells[4] else ''))
    out['ratings'] = ratings
    news, day = [], today
    i = s.find('id="news-table"')
    if i >= 0:
        for when, url, title, src in re.findall(
                r'<td width="130" align="right">\s*(.*?)\s*</td>.*?<a class="tab-link-news" href="([^"]*)"[^>]*>(.*?)</a>'
                r'.*?news-link-right">\s*<span>\(?(.*?)\)?</span>', s[i:i + 60000], re.S):
            when = text(when)
            if when.startswith('Today'):
                day = today
            elif re.match(r'[A-Z][a-z]{2}-\d\d-\d\d', when):
                day = datetime.strptime(when[:9], '%b-%d-%y').date()
            if (today - day).days > 14 or len(news) >= 8:
                break
            news.append({'date': day.isoformat(), 'title': text(title), 'source': text(src), 'url': html.unescape(url)})
    out['news'] = news
    return out


def quote_page(c, sym):
    return cached(c, 'fv:' + fv_symbol(sym),
                  lambda: parse_quote(get(f'https://finviz.com/quote.ashx?t={urllib.parse.quote(fv_symbol(sym))}')))


def quote_pages(c, symbols):
    """{symbol: parsed page}, fetched 4 at a time (the database write happens in this thread)."""
    todo = [s for s in dict.fromkeys(symbols)]
    pages = {}
    def one(sym):
        try:
            return sym, parse_quote(get(f'https://finviz.com/quote.ashx?t={urllib.parse.quote(fv_symbol(sym))}'))
        except Exception:
            return sym, None
    today = date.today().isoformat()
    need = []
    for s in todo:
        r = c.execute('SELECT body FROM pages WHERE key=? AND day=?', ('fv:' + fv_symbol(s), today)).fetchone()
        if r:
            pages[s] = json.loads(r[0])
        else:
            need.append(s)
    with ThreadPoolExecutor(4) as ex:
        for s, p in ex.map(one, need):
            if p:
                pages[s] = p
                c.execute('INSERT OR REPLACE INTO pages VALUES (?,?,?)', ('fv:' + fv_symbol(s), today, json.dumps(p)))
    c.commit()
    return pages


def valuation(p):
    """One line of the numbers Claude used to search for: P/E, forward P/E, dividend, target, consensus."""
    if not p:
        return ''
    keys = ['P/E', 'Forward P/E', 'PEG', 'EPS next Y', 'Dividend TTM', 'Target Price', 'Recom', 'Earnings',
            'Category', 'Expense', 'Return% 1Y']
    names = {'EPS next Y': 'EPS growth next Y', 'Recom': 'analyst consensus (1 strong buy .. 5 sell)',
             'Earnings': 'next earnings'}
    return ' · '.join(f'{names.get(k, k)} {p[k]}' for k in keys if p.get(k))


FUNDAMENTALS = [
    ('quality', ['ROE', 'ROIC', 'Gross Margin', 'Oper. Margin', 'Profit Margin', 'Debt/Eq', 'Current Ratio', 'P/FCF',
                 'EV/EBITDA']),
    ('growth', ['Sales Q/Q', 'EPS Q/Q', 'Sales Y/Y TTM', 'EPS next Q', 'EPS/Sales Surpr.', 'Dividend Gr. 3/5Y']),
    ('positioning', ['Inst Own', 'Inst Trans', 'Insider Own', 'Short Float', 'Short Ratio']),
    ('technicals', ['RSI (14)', 'ATR (14)', 'Volatility', 'Rel Volume', 'Beta'])]
NAMES = {'Debt/Eq': 'debt/equity', 'EPS next Q': 'EPS estimate next quarter', 'EPS/Sales Surpr.': 'last surprise EPS/sales',
         'Inst Trans': 'fund buying (+) or selling (-) 3m', 'Short Ratio': 'days to cover shorts', 'Volatility': 'daily range week/month',
         'Rel Volume': 'volume vs average today', 'ATR (14)': 'average daily range $'}


def fundamentals(p):
    """'quality: ROE 30% ...; growth: ...; positioning: ...; technicals: ...' from the quote page."""
    if not p:
        return ''
    parts = []
    for label, keys in FUNDAMENTALS:
        xs = [f'{NAMES.get(k, k)} {p[k]}' for k in keys if p.get(k)]
        if xs:
            parts.append(f'{label}: ' + ', '.join(xs))
    return '; '.join(parts)


def compact(p):
    """What Claude needs from a quote page, as short text."""
    if not p:
        return None
    return {'sector': p.get('sector'), 'industry': p.get('industry'), 'country': p.get('country'),
            'numbers': valuation(p), 'fundamentals': fundamentals(p), 'ratings_60d': p['ratings'],
            'news_14d': [f'{n["date"]} {n["title"]} ({n["source"]}) {n["url"]}' for n in p['news']]}


def sector_code(name):
    return 'sec_' + re.sub(r'[^a-z]', '', name.lower())


def parse_sectors(s):
    rows = []
    for row in re.findall(r'<tr[^>]*styled-row[^>]*>(.*?)</tr>', s, re.S):
        cells = [text(x) for x in re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)]
        if len(cells) >= 8:
            rows.append({'sector': cells[1], 'week': cells[2], 'month': cells[3], 'quarter': cells[4],
                         'half': cells[5], 'year': cells[6], 'ytd': cells[7]})
    return rows


def sectors(c, url):
    return cached(c, 'fv:sectors', lambda: parse_sectors(get(url))) or []


def parse_screen(s):
    rows = []
    for row in re.findall(r'<tr[^>]*styled-row[^>]*>(.*?)</tr>', s, re.S):
        cells = [text(x) for x in re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)]
        if len(cells) >= 10:
            rows.append({'symbol': cells[1].split()[-1], 'company': cells[2], 'sector': cells[3], 'industry': cells[4],
                         'country': cells[5], 'market_cap': cells[6], 'pe': cells[7], 'price': cells[8]})
    return rows


def screen(c, url, sector):
    return cached(c, 'fv:screen:' + sector, lambda: parse_screen(get(screen_url(url, sector)))) or []


def screen_url(url, sector):
    """The screen's URL with a sector added to its filter list (f=...), wherever that sits in the query."""
    return re.sub(r'([?&]f=[^&]*)', lambda m: m[1] + ',' + sector_code(sector), url, count=1)


def parse_upgrades(s):
    rows = []
    t = max(re.findall(r'<table.*?</table>', s, re.S) or [''], key=len)
    for row in re.findall(r'<tr[^>]*>(.*?)</tr>', t, re.S):
        cells = [text(x) for x in re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)]
        if len(cells) >= 7 and 'Upgrade' in cells[1]:
            firm = cells[2].split(' Subscribe')[0]
            rows.append(f'{cells[0]}: {cells[1]} {firm}, now {cells[6]}' + (f', target {cells[5]}' if cells[5] else ''))
    return rows


def upgrades(c, url):
    return cached(c, 'mb:upgrades', lambda: parse_upgrades(get(url))) or []


# ---------------------------------------------------------------- Yahoo daily closes, stored
def yahoo_symbol(sym):
    # Indexes (^VIX), futures and currencies (CL=F) and ICE indexes (DX-Y.NYB) keep their Yahoo spelling.
    return sym if sym.startswith('^') or '=' in sym or sym.endswith('.NYB') else sym.strip().replace(' ', '-').replace('.', '-')


def fetch_closes(sym, rng):
    req = urllib.request.Request(
        f'https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yahoo_symbol(sym))}?range={rng}&interval=1d',
        headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        res = json.load(r)['chart']['result'][0]
    # New York dates: the server's Sydney clock put half the year's bars on the next day (a Friday close on Saturday).
    q = res['indicators']['quote'][0]
    n = len(res['timestamp'])
    col = lambda k: q.get(k) or [None] * n
    return [(datetime.fromtimestamp(t, NY).date().isoformat(), x, o, h, l)
            for t, x, o, h, l in zip(res['timestamp'], col('close'), col('open'), col('high'), col('low')) if x]


def closes(c, symbols):
    """{symbol: [(day, close), ...] oldest first, about a year}. Topped up at most once a day: a month
    of closes when the database already holds the year, the whole year otherwise."""
    today = date.today().isoformat()
    symbols = list(dict.fromkeys(symbols))
    need = []
    for s in symbols:
        r = c.execute('SELECT day FROM closes_at WHERE symbol=?', (s,)).fetchone()
        if not r or r[0] != today:
            n = c.execute('SELECT COUNT(*) FROM closes WHERE symbol=?', (s,)).fetchone()[0]
            need.append((s, '1mo' if n >= 200 else '1y'))
    def one(job):
        try:
            return job[0], fetch_closes(*job)
        except Exception:
            return job[0], None
    with ThreadPoolExecutor(8) as ex:
        for s, rows in ex.map(one, need):
            if rows:
                c.executemany('INSERT OR REPLACE INTO closes (symbol, day, close, open, high, low) VALUES (?,?,?,?,?,?)',
                              [(s, *r) for r in rows])
                c.execute('INSERT OR REPLACE INTO closes_at VALUES (?,?)', (s, today))
    cut = (date.today() - timedelta(days=380)).isoformat()
    c.execute('DELETE FROM closes WHERE day < ?', (cut,))
    c.commit()
    return {s: [tuple(r) for r in rows] for s in symbols
            if (rows := c.execute('SELECT day, close FROM closes WHERE symbol=? ORDER BY day', (s,)).fetchall())}


def bar_cut():
    """Bars dated before this are complete: today's New York bar counts only from 16:15 there."""
    ny = datetime.now(NY)
    return ny.date().isoformat() if ny.hour * 60 + ny.minute < 16 * 60 + 15 else '9999'


def bars(c, symbols):
    """{symbol: [(day, open, high, low, close)]} oldest first, for the days with a whole bar, after closes() has topped
    them up. Today's New York bar is left out until 16:15 there: a bar still forming is not a candle yet."""
    cut = bar_cut()
    return {s: [tuple(r) for r in rows] for s in dict.fromkeys(symbols)
            if (rows := c.execute('SELECT day, open, high, low, close FROM closes WHERE symbol=? AND open IS NOT NULL '
                                  'AND high IS NOT NULL AND low IS NOT NULL AND day < ? ORDER BY day', (s, cut)).fetchall())}


# ---------------------------------------------------------------- Finnhub
FH = 'https://finnhub.io/api/v1/'
FH_GAP = 1.05  # the free plan allows 60 calls a minute
_fh_last = [0.0]


def env(name, path='/opt/stock-advisor/.env'):
    try:
        return next((l.split('=', 1)[1].strip() for l in open(path) if l.startswith(name + '=')), None)
    except OSError:
        return None


def fh_get(path, key):
    wait = _fh_last[0] + FH_GAP - time.time()
    if wait > 0:
        time.sleep(wait)
    _fh_last[0] = time.time()
    req = urllib.request.Request(FH + path, headers={'X-Finnhub-Token': key})  # a header, so the key is never in a URL
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def fh_symbol(sym):
    return sym.strip().replace(' ', '.')  # IBKR "BRK B" is Finnhub "BRK.B"


def analyst_trend(recs):
    """'buy 34 / hold 15 / sell 4 (Sep); 3 months earlier buy 37 / hold 14 / sell 3' from monthly counts, newest first."""
    if not recs:
        return None
    fmt = lambda r: f'buy {r["strongBuy"] + r["buy"]} / hold {r["hold"]} / sell {r["sell"] + r["strongSell"]}'
    out = f'{fmt(recs[0])} ({date.fromisoformat(recs[0]["period"]):%b %Y})'
    return out + (f'; 3 months earlier {fmt(recs[3])}' if len(recs) > 3 else '')


def surprises(rows):
    """'beat 3 of the last 4: quarter to Jun 2026 +10.9%, ...' (actual earnings per share against the consensus
    estimate). Named by the quarter's end month: Finnhub's quarter numbers are fiscal, so AGX's "Q2 2027" was mid-2026."""
    rows = [r for r in rows or [] if r.get('surprisePercent') is not None][:4]
    if not rows:
        return None
    beat = sum(r['surprisePercent'] > 0 for r in rows)
    return (f'beat {beat} of the last {len(rows)}: quarters to ' +
            ', '.join(f'{date.fromisoformat(r["period"]):%b %Y} {r["surprisePercent"]:+.1f}%' for r in rows))


def insiders(rows, since):
    """Open-market buys (code P) and sales (code S) by insiders since `since`. Buying is the signal worth
    noting; selling is common (tax, diversification) and says less."""
    rows = [r for r in rows or [] if not r.get('isDerivative') and r.get('transactionDate', '') >= since
            and r.get('transactionCode') in ('P', 'S')]
    if not rows:
        return 'no open-market insider buys or sales in 90 days'
    def part(code, verb, noun):
        xs = [r for r in rows if r['transactionCode'] == code]
        if not xs:
            return f'no {noun}'
        v = sum(abs(r['change']) * (r['transactionPrice'] or 0) for r in xs)
        return f'{len({r["name"] for r in xs})} insider(s) {verb} ' + (f'${v / 1e6:,.1f}M' if v >= 1e6 else f'${v / 1e3:,.0f}K')
    return f'90 days: {part("P", "bought", "buys")}; {part("S", "sold", "sales")}'


def finnhub(c, symbols, key=None):
    """{symbol: {'earnings', 'analyst_trend', 'insiders'}} as short text; {} without a key. ETFs get nothing back
    and are skipped quietly."""
    key = key or env('FINNHUB_KEY')
    if not key:
        return {}
    today = date.today()
    since = (today - timedelta(days=90)).isoformat()
    out = {}
    for sym in dict.fromkeys(symbols):
        f = fh_symbol(sym)
        q = urllib.parse.quote(f)
        recs = cached(c, f'fh:rec:{f}', lambda: fh_get(f'stock/recommendation?symbol={q}', key))
        eps = cached(c, f'fh:eps:{f}', lambda: fh_get(f'stock/earnings?symbol={q}&limit=4', key))
        ins = cached(c, f'fh:ins:{f}', lambda: fh_get(f'stock/insider-transactions?symbol={q}&from={since}', key).get('data') or ['none'])
        # Per symbol: the whole-market calendar stops at 1,500 rows, which is about a week in results season.
        cal = cached(c, f'fh:cal:{f}', lambda: fh_get(f'calendar/earnings?from={today}&to={today + timedelta(days=60)}&symbol={q}',
                                                      key).get('earningsCalendar') or ['none'])
        nxt = min((r for r in cal or [] if isinstance(r, dict)), key=lambda r: r['date'], default=None)
        d = {'earnings': (f'next results {nxt["date"]}' + {'bmo': ' before the open', 'amc': ' after the close'}.get(nxt['hour'], '')
                          if nxt else 'no results date in the next 60 days')
                         + (f'; {s}' if (s := surprises(eps)) else ''),
             'analyst_trend': analyst_trend(recs),
             'insiders': insiders([r for r in ins or [] if isinstance(r, dict)], since)}
        if recs or eps:  # an ETF or an unknown symbol: nothing to say
            out[sym] = {k: v for k, v in d.items() if v}
    return out


# ---------------------------------------------------------------- SEC EDGAR (company filings)
# The SEC asks every client to name itself and a contact; requests without one are refused (403).
SEC_UA = {'User-Agent': 'Stock advisor admin@example.com'}
ITEMS = {'1.01': 'major agreement signed', '1.02': 'major agreement ended', '1.03': 'BANKRUPTCY',
         '2.01': 'acquisition or sale completed', '2.02': 'results', '2.03': 'new debt', '2.05': 'RESTRUCTURING, job cuts',
         '2.06': 'IMPAIRMENT (write-down)', '3.01': 'DELISTING NOTICE', '3.02': 'new shares sold', '4.01': 'AUDITOR CHANGE',
         '4.02': 'EARLIER ACCOUNTS NOT RELIABLE (restatement)', '5.01': 'CHANGE OF CONTROL', '5.02': 'director or officer change',
         '5.03': 'bylaws changed', '5.07': 'shareholder vote', '7.01': 'investor update', '8.01': 'other event'}


def sec_get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=SEC_UA), timeout=20) as r:
        return json.load(r)


def filings_text(recent, since):
    """'2026-09-12 8-K: director or officer change; 2026-08-02 SC 13D (a 5%+ holder with plans, often an activist)'
    from EDGAR's recent-filings columns: current-event reports (8-K, and 6-K for foreign issuers) and activist stakes."""
    out = []
    for form, day, items in zip(recent['form'], recent['filingDate'], recent['items']):
        if day < since:
            break  # newest first
        if form in ('8-K', '8-K/A'):
            what = [ITEMS[i] for i in items.split(',') if i in ITEMS]
            out.append(f'{day} {form}: {", ".join(what) or "other"}')
        elif form == '6-K':
            out.append(f'{day} 6-K (foreign issuer report)')
        elif '13D' in form:
            out.append(f'{day} {form} (a 5%+ holder with plans, often an activist)')
    return '; '.join(out[:8]) or 'no current-event filings in 30 days'


def edgar(c, symbols):
    """{symbol: recent filings text} from SEC EDGAR, cached for the day; ETFs and foreign names EDGAR does not know are
    skipped."""
    ciks = cached(c, 'sec:tickers', lambda: {v['ticker']: v['cik_str'] for v in
                                            sec_get('https://www.sec.gov/files/company_tickers.json').values()}) or {}
    since = (date.today() - timedelta(days=30)).isoformat()
    out = {}
    for sym in dict.fromkeys(symbols):
        cik = ciks.get(sym.strip().replace(' ', '-').replace('.', '-'))
        if not cik:
            continue
        txt = cached(c, f'sec:{cik}', lambda: filings_text(
            sec_get(f'https://data.sec.gov/submissions/CIK{cik:010d}.json')['filings']['recent'], since))
        if txt:
            out[sym] = txt
        time.sleep(0.15)  # the SEC allows 10 requests a second
    return out


# ---------------------------------------------------------------- FRED (US macro)
# series, label, how to show it: 'level' as published, 'yoy' % change on a year earlier, 'diff' change on the month before
MACRO = [('CPIAUCSL', 'CPI inflation, % a year', 'yoy'), ('CPILFESL', 'Core CPI inflation, % a year', 'yoy'),
         ('UNRATE', 'Unemployment %', 'level'), ('PAYEMS', 'Jobs added in the month, thousands', 'diff'),
         ('DFF', 'Fed funds rate %', 'level'), ('T10Y2Y', '10-year minus 2-year yield, points', 'level'),
         ('BAMLH0A0HYM2', 'High-yield credit spread, points', 'level'),
         ('DEXUSAL', 'US$ per A$ (a stronger A$ shrinks US returns in A$)', 'level')]


def fred_series(key, sid):
    """[(day, value)] oldest first, about 26 months (a yearly change a year ago needs two years); FRED writes '.'
    for a missing day."""
    start = (date.today() - timedelta(days=800)).isoformat()
    # FRED only takes the key in the URL: never log this URL.
    with urllib.request.urlopen(f'https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={key}'
                                f'&file_type=json&observation_start={start}', timeout=20) as r:
        return [(o['date'], float(o['value'])) for o in json.load(r)['observations'] if o['value'] != '.']


def transform(rows, how):
    if how == 'yoy':
        at = dict(rows)
        return [(d, 100 * (v / at[y] - 1)) for d, v in rows if (y := f'{int(d[:4]) - 1}{d[4:]}') in at]
    if how == 'diff':
        return [(d, v - rows[i - 1][1]) for i, (d, v) in enumerate(rows) if i]
    return rows


def at_or_before(rows, day):
    return next((v for d, v in reversed(rows) if d <= day), None)


def macro_table(c, key=None):
    """Markdown table of the latest US macro figures against 3 and 12 months earlier; '' without a key."""
    key = key or env('FRED_KEY')
    if not key:
        return ''
    lines = ['| Measure | Latest | 3 months earlier | A year earlier |', '|---|---|---|---|']
    for sid, label, how in MACRO:
        rows = cached(c, f'fred:{sid}', lambda: fred_series(key, sid))
        rows = transform([tuple(r) for r in rows or []], how)
        if not rows:
            continue
        d, v = rows[-1]
        f = lambda x: '–' if x is None else f'{x:,.0f}' if how == 'diff' else f'{x:.4f}' if sid == 'DEXUSAL' else f'{x:.2f}'
        back = lambda n: at_or_before(rows, (date.fromisoformat(d) - timedelta(days=n)).isoformat())
        when = f'{date.fromisoformat(d):%b %Y}' if d.endswith('-01') else f'{date.fromisoformat(d):%-d %b %Y}'  # monthly figures: the month
        lines.append(f'| {label} | {f(v)} ({when}) | {f(back(91))} | {f(back(365))} |')
    return '\n'.join(lines) + '\n\n(FRED, St. Louis Fed)' if len(lines) > 2 else ''


# Markets that usually move together (+1) or against each other (-1); None where the usual link itself has changed
# over the years (stocks and bonds fell together in 2022). Yields (FRED's DFII10, Yahoo's ^VIX level) are compared by
# their daily change in points, prices by their daily % change.
LINKS = [('GC=F', 'DX-Y.NYB', -1, 'Gold vs the US dollar', 'gold is priced in dollars'),
         ('GC=F', 'DFII10', -1, 'Gold vs the 10-year real yield', 'gold pays no interest'),
         ('GC=F', 'SPY', None, 'Gold vs the S&P 500', 'gold as a haven'),
         ('SPY', 'TLT', None, 'S&P 500 vs long Treasuries (TLT)', 'bonds as the hedge for stocks'),
         ('CL=F', 'XLE', 1, 'Oil vs energy stocks (XLE)', 'producers earn more when oil is dear'),
         ('SPY', '^VIX', -1, 'S&P 500 vs the VIX', 'fear rises when stocks fall'),
         ('AUDUSD=X', 'SPY', 1, 'A$ vs the S&P 500', 'the A$ is a risk-on currency')]
POINTS = {'DFII10', '^VIX'}
BROKEN_GAP = 0.6  # a correlation this far from the past year's, or of the wrong sign, marks the link as broken


def changes(rows, points):
    """{day: change since the previous row}: points for yields and levels, a fraction for prices."""
    return {d: (b - a if points else b / a - 1) for (_, a), (d, b) in zip(rows, rows[1:]) if points or a}


def link_stats(a, b):
    """(past year, last 60, last 20) correlations of two {day: change} series on their common days; None where too few.
    The past year leaves out the last 60 days, so it is the baseline the recent figures are compared with."""
    days = sorted(set(a) & set(b))
    def corr(ds):
        if len(ds) < 15:
            return None
        try:
            return statistics.correlation([a[d] for d in ds], [b[d] for d in ds])
        except statistics.StatisticsError:
            return None
    return corr(days[-250:-60]), corr(days[-60:]), corr(days[-20:])


def broken(usual, past, d60, d20):
    wrong = lambda x: usual is not None and x is not None and x * usual < 0 and abs(x) >= 0.2
    far = lambda x: past is not None and x is not None and abs(x - past) >= BROKEN_GAP
    return wrong(d60) or wrong(d20) or far(d60) or far(d20)


def links_table(c, key=None):
    """Markdown table of how the usual cross-market relationships have held up, from daily changes: the past year
    against the last 60 and 20 trading days, each pair marked BROKEN when it has stopped behaving as usual."""
    key = key or env('FRED_KEY')
    data = closes(c, sorted({s for x, y, *_ in LINKS for s in (x, y) if s != 'DFII10'}))
    if key:
        data['DFII10'] = [tuple(r) for r in cached(c, 'fred:DFII10', lambda: fred_series(key, 'DFII10')) or []]
    f = lambda x: '–' if x is None else f'{x:+.2f}'
    lines = ['| Pair | Usually | Why | Past year | Last 60 days | Last 20 days | 20-day moves | |', '|---|---|---|---|---|---|---|---|']
    move = lambda sym: ('–' if len(data[sym]) < 21 else f'{data[sym][-1][1] - data[sym][-21][1]:+.2f} pts' if sym in POINTS
                        else f'{100 * (data[sym][-1][1] / data[sym][-21][1] - 1):+.1f}%')
    for x, y, usual, label, why in LINKS:
        if not data.get(x) or not data.get(y):
            continue
        past, d60, d20 = link_stats(changes(data[x], x in POINTS), changes(data[y], y in POINTS))
        if d60 is None:
            continue
        lines.append(f'| {label} | {"together" if usual == 1 else "opposite" if usual == -1 else "varies"} | {why} | '
                     f'{f(past)} | {f(d60)} | {f(d20)} | {move(x)}, {move(y)} | {"BROKEN" if broken(usual, past, d60, d20) else "as usual"} |')
    return ('\n'.join(lines) + '\n\n(Correlation of daily changes, -1 to +1; Yahoo closes and FRED)') if len(lines) > 2 else ''


def index_table(c, symbols, year=None):
    """Markdown table of day, week, month and year-to-date moves: what the brief used IBKR calls for."""
    data = closes(c, [s for s, _ in symbols])
    year = str(year or date.today().year)
    lines = ['| Index | Last | Day | Week | Month | YTD |', '|---|---|---|---|---|---|']
    for sym, name in symbols:
        rows = data.get(sym)
        if not rows or len(rows) < 22:
            continue
        last = rows[-1][1]
        pct = lambda old: f'{100 * (last / old - 1):+.2f}%'
        base = [x for d, x in rows if d < year]
        lines.append(f'| {name} | {last:,.2f} | {pct(rows[-2][1])} | {pct(rows[-6][1])} | {pct(rows[-22][1])} | '
                     f'{pct(base[-1]) if base else "–"} |')
    return '\n'.join(lines) + f'\n\n(Yahoo daily closes, last bar {max((r[-1][0] for r in data.values()), default="?")})'


def test():
    import sqlite3
    q = ('<div class="snapshot-td-label">P/E</div><div class="snapshot-td-content">28.76</div>'
         '<div class="snapshot-td-label">Forward P/E</div><div class="snapshot-td-content"><b>21.98</b></div>'
         '<div class="snapshot-td-label">Employees</div><div class="snapshot-td-content">9</div>'
         '<a href="screener.ashx?v=111&f=sec_technology" class="quote-header_category">Technology</a>'
         '<a href="screener.ashx?v=111&f=geo_usa" class="quote-header_category">USA</a>'
         '<table class="js-table-ratings"><tr class="styled-row x"><td>Sep-23-26</td><td>Upgrade</td><td>Stifel</td>'
         '<td>Hold &rarr; Buy</td><td>$575</td></tr><tr class="styled-row"><td>Jan-02-26</td><td>Old</td><td>X</td>'
         '<td>Buy</td><td></td></tr></table>'
         '<table id="news-table"><tr><td width="130" align="right"> Today 06:51PM </td><td><a class="tab-link-news" '
         'href="https://a.com/1">Up &amp; away</a><div class="news-link-right"><span>(Reuters)</span></div></td></tr>'
         '<tr><td width="130" align="right"> Sep-20-26 04:19PM </td><td><a class="tab-link-news" href="https://a.com/2">'
         'Older</a><div class="news-link-right"><span>(WSJ)</span></div></td></tr></table>')
    p = parse_quote(q, date(2026, 9, 26))
    assert p['P/E'] == '28.76' and p['Forward P/E'] == '21.98' and 'Employees' not in p, p
    assert p['sector'] == 'Technology' and p['country'] == 'USA', p
    assert p['ratings'] == ['2026-09-23 Upgrade Stifel: Hold → Buy, target $575'], p['ratings']
    assert [(n['date'], n['title'], n['source']) for n in p['news']] == [
        ('2026-09-26', 'Up & away', 'Reuters'), ('2026-09-20', 'Older', 'WSJ')], p['news']
    assert sector_code('Basic Materials') == 'sec_basicmaterials'
    assert screen_url('https://x/s?v=1&f=a,b&o=-m', 'Energy') == 'https://x/s?v=1&f=a,b,sec_energy&o=-m'
    assert parse_sectors('<tr class="styled-row a"><td>1</td><td>Energy</td><td>-2.89%</td><td>-0.44%</td><td>13.18%</td>'
                         '<td>-0.59%</td><td>34.17%</td><td>35.19%</td><td>x</td></tr>')[0]['quarter'] == '13.18%'
    c = sqlite3.connect(':memory:')
    setup(c)
    c.executemany('INSERT INTO closes (symbol, day, close) VALUES (?,?,?)', [('SPY', f'2025-12-{d:02d}', 100.0) for d in range(1, 31)] +
                  [('SPY', f'2026-01-{d:02d}', 110.0) for d in range(1, 31)])
    c.execute('INSERT INTO closes_at VALUES (?,?)', ('SPY', date.today().isoformat()))
    t = index_table(c, [('SPY', 'S&P 500')], 2026)
    assert '| S&P 500 | 110.00 | +0.00% | +0.00% | +0.00% | +10.00% |' in t, t
    R = lambda p, sb, b, h, s, ss: {'period': p, 'strongBuy': sb, 'buy': b, 'hold': h, 'sell': s, 'strongSell': ss}
    assert analyst_trend([R('2026-09-01', 12, 22, 15, 3, 1), R('2026-08-01', 0, 0, 0, 0, 0), R('2026-07-01', 0, 0, 0, 0, 0),
                          R('2026-06-01', 14, 23, 14, 2, 0)]) == 'buy 34 / hold 15 / sell 4 (Sep 2026); 3 months earlier buy 37 / hold 14 / sell 2'
    assert surprises([{'surprisePercent': 10.92, 'period': '2026-06-30'}, {'surprisePercent': -0.9, 'period': '2026-03-31'}]) == \
        'beat 1 of the last 2: quarters to Jun 2026 +10.9%, Mar 2026 -0.9%'
    I = lambda n, code, chg, px, d='2026-09-20', der=False: {'name': n, 'transactionCode': code, 'change': chg,
                                                             'transactionPrice': px, 'transactionDate': d, 'isDerivative': der}
    assert insiders([I('A', 'P', 1000, 50), I('B', 'P', 1000, 50), I('A', 'S', -2e5, 10), I('C', 'S', -1, 1, '2026-01-01'),
                     I('D', 'P', 9, 9, der=True), I('E', 'M', 5, 5)], '2026-07-01') == \
        '90 days: 2 insider(s) bought $100K; 1 insider(s) sold $2.0M'
    assert insiders([], '2026-07-01').startswith('no open-market')
    cpi = [('2025-01-01', 100.0), ('2025-02-01', 101.0), ('2026-01-01', 103.0), ('2026-02-01', 104.03)]
    assert [(d, round(v, 2)) for d, v in transform(cpi, 'yoy')] == [('2026-01-01', 3.0), ('2026-02-01', 3.0)]
    assert transform([('a', 10.0), ('b', 12.5)], 'diff') == [('b', 2.5)]
    assert at_or_before(cpi, '2025-12-31') == 101.0 and at_or_before(cpi, '2024-01-01') is None
    rec = {'form': ['4', '8-K', 'SC 13D', '10-Q', '8-K'], 'filingDate': ['2026-09-20', '2026-09-12', '2026-09-02', '2026-08-01', '2026-07-01'],
           'items': ['', '5.02,9.01', '', '', '2.02']}
    assert filings_text(rec, '2026-09-01') == ('2026-09-12 8-K: director or officer change; '
                                               '2026-09-02 SC 13D (a 5%+ holder with plans, often an activist)'), filings_text(rec, '2026-09-01')
    # Cross-market links: gold rising with the dollar for the last 60 days, after a year of moving against it, is broken.
    import random
    rnd = random.Random(1)
    days = [f'd{i:03d}' for i in range(300)]
    dx = {d: rnd.gauss(0, 1) for d in days}
    gold = {d: (-1 if i < 240 else 1) * dx[d] + rnd.gauss(0, 0.5) for i, d in enumerate(days)}
    past, d60, d20 = link_stats(gold, dx)
    assert past < -0.7 and d60 > 0.7 and d20 > 0.7 and broken(-1, past, d60, d20), (past, d60, d20)
    calm = {d: -dx[d] + rnd.gauss(0, 0.5) for d in days}
    assert not broken(-1, *link_stats(calm, dx))
    assert changes([('a', 100.0), ('b', 110.0)], False) == {'b': 0.10000000000000009} and changes([('a', 1.5), ('b', 1.75)], True) == {'b': 0.25}
    print('feeds ok')


if __name__ == '__main__':
    test()
