"""Investor lenses and chart patterns, by script.

Lenses: what eight well-known investors would check, from the Finviz quote page and a year of daily closes. Each gives
'pass', 'mixed' or 'fail' with the numbers behind it, or nothing when the data is missing (ETFs have no earnings).
  Buffett & Munger   quality at a fair price: high returns on capital, little debt, good margins, growing earnings
  Graham             margin of safety: P/E x P/B under 22.5 (his defensive investor's limit)
  Greenblatt         Magic Formula: a high earnings yield and a high return on capital together
  Piotroski          financial health: up to 7 of his 9 accounting checks that Finviz's figures allow
  Lynch              growth at a reasonable price (PEG), and which of his six kinds of stock it is
  O'Neil (CAN SLIM)  growing earnings, near a new high, beating the market, funds buying, in a rising market
  Minervini          his trend template: 8 checks on the moving averages, the 52-week range and relative strength
  Weinstein          the stage around the 30-week (150-day) average: 1 basing, 2 advancing, 3 topping, 4 declining

Patterns: candlesticks (from daily open, high, low and close) and classic chart shapes (from closes). The research
behind them is weak, so they are ON TRIAL: recorded each time they appear, scored against the S&P 500 like the
proposals, and treated as evidence only once they have proven themselves on this app's own data (see learn.py).
"""
import math, re, statistics

# key, investor, idea, passes when (shown on the How it works page), what to own or when to buy
LENSES = [('buffett', 'Buffett & Munger', 'Quality at a fair price',
           'return on capital 15%+, long-term debt under 0.8x equity, margin 10%+, earnings up over 5 years, and P/FCF 30 or less', 'own'),
          ('graham', 'Graham', 'Margin of safety', 'P/E x P/B is 22.5 or less: the price is backed by earnings and assets', 'own'),
          ('greenblatt', 'Greenblatt', 'Magic Formula', 'earnings yield 6%+ and return on capital 20%+ together', 'own'),
          ('piotroski', 'Piotroski', 'Financial health',
           '6 or more of 7 checks: profitable, cash flow positive and above profit, earnings and sales up, liquid, modest debt', 'own'),
          ('lynch', 'Lynch', 'Growth at a reasonable price',
           'PEG 1 or less (dividend added for slow growers); also names the kind of stock and its sell rule', 'own'),
          ('oneil', "O'Neil", 'CAN SLIM',
           '5 or more of 6: quarterly and 3-year earnings up 25%+, near a new high, beating the market, funds buying, market rising', 'buy'),
          ('minervini', 'Minervini', 'Trend template',
           'all 8: above rising 50-, 150- and 200-day averages in that order, 30%+ off the low, within 25% of the high, beating the S&P 500', 'buy'),
          ('weinstein', 'Weinstein', 'Stage analysis', 'stage 2: above a rising 30-week average. Stage 4 means never buy', 'buy')]
LENS_NAMES = {k: n for k, n, *_ in LENSES}

# Lynch's six kinds of stock, and the sell discipline he gave each (One Up on Wall Street, 1989).
LYNCH_SELL = {
    'slow grower': 'sell when the dividend is at risk or after a 30-50% gain; little upside beyond the dividend',
    'stalwart': 'take profit after a 30-50% gain, or when the P/E runs well above its usual range',
    'fast grower': 'hold while growth holds; sell when growth slows for two quarters or the P/E far outruns growth',
    'cyclical': 'sell as earnings peak: a low P/E late in the cycle is a warning, not a bargain',
    'turnaround': 'sell once the turnaround is done and the market has re-rated it',
    'asset play': 'sell when the market recognises the hidden value, or the value shrinks'}

# pattern: (name, bullish or bearish, what it is)
PATTERNS = {
    'bullish_engulfing': ('Bullish engulfing', 'bullish', 'after a fall, a rising day whose body covers the previous falling day'),
    'bearish_engulfing': ('Bearish engulfing', 'bearish', 'after a rise, a falling day whose body covers the previous rising day'),
    'hammer': ('Hammer', 'bullish', 'after a fall, a small body with a long lower shadow: sellers pushed down and lost'),
    'shooting_star': ('Shooting star', 'bearish', 'after a rise, a small body with a long upper shadow: buyers pushed up and lost'),
    'morning_star': ('Morning star', 'bullish', 'after a fall: a big falling day, a small one, then a rising day past the first one\'s midpoint'),
    'evening_star': ('Evening star', 'bearish', 'after a rise: a big rising day, a small one, then a falling day past the first one\'s midpoint'),
    'three_white_soldiers': ('Three white soldiers', 'bullish', 'three strong rising days, each opening inside the last one\'s body'),
    'three_black_crows': ('Three black crows', 'bearish', 'three strong falling days, each opening inside the last one\'s body'),
    'double_bottom': ('Double bottom', 'bullish', 'two similar lows with a peak between, and the close breaking above that peak'),
    'double_top': ('Double top', 'bearish', 'two similar highs with a trough between, and the close breaking below that trough'),
    'head_shoulders': ('Head and shoulders', 'bearish', 'three highs, the middle one highest, and the close breaking the neckline'),
    'inverse_head_shoulders': ('Inverse head and shoulders', 'bullish', 'three lows, the middle one lowest, and the close breaking the neckline'),
    'bull_flag': ('Bull flag', 'bullish', 'a rise of 15% or more in under a month, a shallow pullback, then a close above it'),
}
CANDLES = [k for k in PATTERNS if k not in ('double_bottom', 'double_top', 'head_shoulders', 'inverse_head_shoulders', 'bull_flag')]


def num(v):
    """'44.23%' -> 44.23, '370.11B' -> 3.7011e11, '-' or None -> None. The first figure when there are two."""
    if v is None:
        return None
    m = re.match(r'\s*(-?[\d,]*\.?\d+)\s*([KMBT%])?', str(v))
    if not m:
        return None
    x = float(m[1].replace(',', ''))
    return x * {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}.get(m[2], 1)


def second(v):
    """'11.48% 11.14%' -> 11.14 (the 5-year figure of a 3/5-year pair)."""
    xs = re.findall(r'-?[\d.]+', str(v or ''))
    return float(xs[1]) if len(xs) > 1 else None


def verdict(ok, known, need, pass_at, fail_at):
    """'pass' / 'mixed' / 'fail' from how many of the known checks passed, or None with too little data."""
    if known < need:
        return None
    share = ok / known
    return 'pass' if share >= pass_at else 'fail' if share <= fail_at else 'mixed'


def checks(items):
    """items: [(label, True/False/None)] -> (passed, known, the failed labels)."""
    known = [(l, x) for l, x in items if x is not None]
    return sum(1 for _, x in known if x), len(known), [l for l, x in known if not x]


def fmt(x, unit='', d=1):
    return '–' if x is None else f'{x:,.{d}f}{unit}'


def fundamental_lenses(p, market_up=None, beats_spy=None, near_high=None):
    """{lens: (verdict, detail)} for the lenses read from the quote page. ETFs and pages without earnings give none."""
    if not p or p.get('Asset Type') or p.get('Category'):
        return {}
    g = lambda k: num(p.get(k))
    fin = (p.get('sector') or '') == 'Financial'  # a bank's debt is its raw material: leverage tests do not apply
    out = {}
    roic, roe, margin = g('ROIC'), g('ROE'), g('Profit Margin')
    debt = g('LT Debt/Eq') if p.get('LT Debt/Eq') else g('Debt/Eq')
    g5 = second(p.get('EPS past 3/5Y'))
    pe, pb, pfcf = g('P/E'), g('P/B'), g('P/FCF')
    eps, book, price = g('EPS (ttm)'), g('Book/sh'), g('Price')

    # Buffett & Munger: a wonderful business (returns on capital, little debt, margins, earnings that grow), at a fair price.
    ret = roic if roic is not None else roe
    q, k, miss = checks([('return on capital 15%+', None if ret is None else ret >= 15),
                         ('long-term debt under 0.8x equity', None if fin or debt is None else debt <= 0.8),
                         ('profit margin 10%+', None if margin is None else margin >= 10),
                         ('earnings grew over 5 years', None if g5 is None else g5 > 0)])
    fair = pfcf if pfcf is not None else pe
    fair_ok = None if fair is None else 0 < fair <= 30
    v = verdict(q + bool(fair_ok), k + (fair_ok is not None), 3, 1, 0.5)
    if v:
        out['buffett'] = (v, f'ROIC {fmt(ret, "%")}, LT debt/equity {fmt(debt, "", 2)}, margin {fmt(margin, "%")}, EPS 5-yr '
                             f'{fmt(g5, "%/yr")}, {"P/FCF" if pfcf is not None else "P/E"} {fmt(fair)}'
                             + (f'; misses: {", ".join(miss + ([] if fair_ok in (True, None) else ["a fair price (30x or less)"]))}'
                                if miss or fair_ok is False else ''))

    # Graham: P/E x P/B at most 22.5, so the price has a margin of safety over earnings and assets.
    if pe is not None and pb is not None:
        prod = pe * pb
        gn = math.sqrt(22.5 * eps * book) if eps and book and eps > 0 and book > 0 else None
        out['graham'] = ('pass' if prod <= 22.5 else 'mixed' if prod <= 50 else 'fail',
                         f'P/E x P/B = {prod:.1f} (limit 22.5)' + (f'; Graham number ${gn:,.2f} against ${price:,.2f}' if gn and price else ''))
    elif eps is not None and eps <= 0:
        out['graham'] = ('fail', 'no earnings: no margin of safety in Graham\'s terms')

    # Greenblatt's Magic Formula ranks a whole market by earnings yield plus return on capital.
    # ponytail: fixed thresholds instead of a market-wide ranking; rank the screened universe if this proves too loose.
    inc, ev = g('Income'), g('Enterprise Value')
    ey = inc / ev if inc is not None and ev and ev > 0 else None
    if ey is not None and roic is not None:
        out['greenblatt'] = ('pass' if ey >= 0.06 and roic >= 20 else 'fail' if ey < 0.03 or roic < 10 else 'mixed',
                             f'earnings yield {100 * ey:.1f}% (6%+), ROIC {roic:.1f}% (20%+)')

    # Piotroski: the checks Finviz's figures allow (his F-score has nine, some need last year's statements).
    q, k, miss = checks([('profitable (ROA above 0)', None if g('ROA') is None else g('ROA') > 0),
                         ('free cash flow positive', None if pfcf is None and pe is None else pfcf is not None and pfcf > 0),
                         ('cash flow above profit', None if pfcf is None or pe is None or pe <= 0 else pfcf < pe),
                         ('earnings up on a year ago', None if g('EPS Y/Y TTM') is None else g('EPS Y/Y TTM') > 0),
                         ('sales up on a year ago', None if g('Sales Y/Y TTM') is None else g('Sales Y/Y TTM') > 0),
                         ('current ratio 1+', None if fin or g('Current Ratio') is None else g('Current Ratio') >= 1),
                         ('long-term debt under 1x equity', None if fin or debt is None else debt <= 1)])
    v = verdict(q, k, 5, 0.8, 0.4)
    if v:
        out['piotroski'] = (v, f'{q} of {k} health checks' + (f'; fails: {", ".join(miss)}' if miss else ''))

    # Lynch: PEG (P/E over growth), with the dividend added for slow growers, and his kind of stock.
    kind = lynch_type(p)
    growth = g('EPS next 5Y') if p.get('EPS next 5Y') else g5
    dy = num((re.search(r'\(([\d.]+)%\)', p.get('Dividend TTM') or '') or [None, None])[1])
    paid = (growth or 0) + ((dy or 0) if kind == 'slow grower' else 0)
    if kind and pe is not None and pe > 0 and paid > 0:
        peg = pe / paid
        out['lynch'] = ('pass' if peg <= 1 else 'mixed' if peg <= 1.5 else 'fail',
                        f'{kind}; PEG {peg:.2f}' + (' with the dividend' if kind == 'slow grower' else '')
                        + f' (1 or less is cheap for the growth). Sell rule: {LYNCH_SELL[kind]}')
    elif kind:
        out['lynch'] = ('fail' if kind == 'turnaround' or growth is not None else 'mixed',
                        f'{kind}; no PEG: ' + ('earnings are not expected to grow' if growth is not None else 'no earnings or growth estimate')
                        + f'. Sell rule: {LYNCH_SELL[kind]}')

    # O'Neil, CAN SLIM: the parts that can be read here (S, supply, is left out).
    q, k, miss = checks([('C: quarterly earnings up 25%+', None if g('EPS Q/Q') is None else g('EPS Q/Q') >= 25),
                         ('A: earnings up 25%+ a year over 3 years', None if not p.get('EPS past 3/5Y') else num(p['EPS past 3/5Y']) >= 25),
                         ('N: within 15% of its 52-week high', near_high),
                         ('L: beating the S&P 500 over a year', beats_spy),
                         ('I: funds adding to it', None if g('Inst Trans') is None else g('Inst Trans') > 0),
                         ('M: the market above its 200-day average', market_up)])
    v = verdict(q, k, 4, 0.8, 0.34)
    if v:
        out['oneil'] = (v, f'{q} of {k}' + (f'; misses: {", ".join(miss)}' if miss else ''))
    return out


def lynch_type(p):
    if not p or p.get('Asset Type') or p.get('Category'):
        return None
    g = lambda k: num(p.get(k))
    growth = g('EPS next 5Y') if p.get('EPS next 5Y') else second(p.get('EPS past 3/5Y'))
    if (g('EPS (ttm)') or 0) < 0 or (g('Profit Margin') or 0) < 0:
        return 'turnaround'
    if g('P/B') is not None and 0 < g('P/B') < 1:
        return 'asset play'
    if p.get('sector') in ('Energy', 'Basic Materials') or re.search(
            r'auto|airline|steel|chemical|aluminum|copper|homebuild|semiconductor equipment|oil|gas|mining|shipping',
            p.get('industry') or '', re.I):
        return 'cyclical'
    if growth is not None and growth >= 20:
        return 'fast grower'
    if (g('Market Cap') or 0) >= 10e9 and (growth or 0) >= 8:
        return 'stalwart'
    return 'slow grower'


def sma(xs, n, back=0):
    end = len(xs) - back
    return sum(xs[end - n:end]) / n if end >= n else None


def trend_lenses(closes, spy):
    """{lens: (verdict, detail)} for Minervini and Weinstein, from daily closes (oldest first), plus the facts O'Neil
    needs: ('near_high', 'beats_spy')."""
    out, facts = {}, {}
    if len(closes) < 220:
        return out, facts
    last = closes[-1]
    s50, s150, s200 = sma(closes, 50), sma(closes, 150), sma(closes, 200)
    s200_then = sma(closes, 200, 20)
    hi, lo = max(closes[-252:]), min(closes[-252:])
    yr = lambda xs: xs[-1] / xs[-min(252, len(xs))] - 1
    beats = None if len(spy) < 200 else yr(closes) > yr(spy)
    facts = {'near_high': last >= 0.85 * hi, 'beats_spy': beats}
    q, k, miss = checks([('above the 150- and 200-day averages', last > s150 and last > s200),
                         ('150-day above 200-day', s150 > s200),
                         ('200-day rising for a month', None if s200_then is None else s200 > s200_then),
                         ('50-day above both', s50 > s150 and s50 > s200),
                         ('above the 50-day', last > s50),
                         ('30%+ above the 52-week low', last >= 1.3 * lo),
                         ('within 25% of the 52-week high', last >= 0.75 * hi),
                         ('beating the S&P 500 over a year', beats)])
    out['minervini'] = ('pass' if q == k else 'fail' if q <= k / 2 else 'mixed',
                        f'{q} of {k} trend checks' + (f'; misses: {", ".join(miss)}' if miss else ''))
    if len(closes) >= 250:
        now, then, before = s150, sma(closes, 150, 20), sma(closes, 150, 100)
        slope = now / then - 1
        rising, falling = slope > 0.005, slope < -0.005
        if last > now and rising:
            st, what = 2, 'advancing: above a rising 30-week average'
        elif last < now and falling:
            st, what = 4, 'declining: below a falling 30-week average'
        elif last < now and rising:
            st, what = 3, 'topping: fallen below a 30-week average that is still rising'
        elif last > now and falling:
            st, what = 1, 'basing: back above a 30-week average that is still falling'
        elif now < before:
            st, what = 1, 'basing after a decline: the 30-week average has flattened'
        else:
            st, what = 3, 'topping after a rise: the 30-week average has flattened'
        out['weinstein'] = ({2: 'pass', 4: 'fail'}.get(st, 'mixed'), f'stage {st}, {what} ({100 * slope:+.1f}% in 4 weeks)')
    return out, facts


def market_up(spy):
    return None if len(spy) < 200 else spy[-1] > sma(spy, 200)


def lenses(page, closes, spy):
    """All eight: {lens: (verdict, detail)}. closes and spy: daily closes, oldest first."""
    t, facts = trend_lenses(closes, spy)
    return {**fundamental_lenses(page, market_up(spy), facts.get('beats_spy'), facts.get('near_high')), **t}


def lens_text(ls):
    return '; '.join(f'{LENS_NAMES[k]} {v} ({d})' for k, *_ in LENSES if k in ls for v, d in [ls[k]])


# ---------------------------------------------------------------- patterns
def candles(bars):
    """Candlestick patterns completed on the last bar. bars: [(day, open, high, low, close)], oldest first."""
    if len(bars) < 9:
        return []
    O, H, L, C = ([b[i] for b in bars] for i in (1, 2, 3, 4))
    t = len(bars) - 1
    body = lambda i: C[i] - O[i]
    rng = lambda i: max(H[i] - L[i], 1e-9)
    trend = lambda end: C[end] / C[end - 5] - 1  # the move over the 5 days ending at `end`
    down, up = trend(t - 1) <= -0.03, trend(t - 1) >= 0.03
    avg_body = statistics.mean(abs(body(i)) for i in range(t - 8, t)) or 1e-9
    found = []
    if down and body(t - 1) < 0 < body(t) and O[t] <= C[t - 1] and C[t] >= O[t - 1]:
        found.append('bullish_engulfing')
    if up and body(t - 1) > 0 > body(t) and O[t] >= C[t - 1] and C[t] <= O[t - 1]:
        found.append('bearish_engulfing')
    small = abs(body(t)) <= 0.35 * rng(t)
    lower, upper = min(O[t], C[t]) - L[t], H[t] - max(O[t], C[t])
    if down and small and lower >= 0.6 * rng(t) and upper <= 0.15 * rng(t):
        found.append('hammer')
    if up and small and upper >= 0.6 * rng(t) and lower <= 0.15 * rng(t):
        found.append('shooting_star')
    big = lambda i: abs(body(i)) >= max(0.6 * rng(i), avg_body)
    mid = (O[t - 2] + C[t - 2]) / 2
    if trend(t - 2) <= -0.03 and body(t - 2) < 0 and big(t - 2) and abs(body(t - 1)) <= 0.3 * rng(t - 1) and body(t) > 0 and C[t] > mid:
        found.append('morning_star')
    if trend(t - 2) >= 0.03 and body(t - 2) > 0 and big(t - 2) and abs(body(t - 1)) <= 0.3 * rng(t - 1) and body(t) < 0 and C[t] < mid:
        found.append('evening_star')
    three = range(t - 2, t + 1)
    if (all(body(i) > 0 and abs(body(i)) >= 0.6 * rng(i) for i in three) and C[t - 2] < C[t - 1] < C[t]
            and all(O[i - 1] <= O[i] <= C[i - 1] for i in (t - 1, t)) and trend(t - 3) <= 0):
        found.append('three_white_soldiers')
    if (all(body(i) < 0 and abs(body(i)) >= 0.6 * rng(i) for i in three) and C[t - 2] > C[t - 1] > C[t]
            and all(C[i - 1] <= O[i] <= O[i - 1] for i in (t - 1, t)) and trend(t - 3) >= 0):
        found.append('three_black_crows')
    return found


def swings(xs, k=5):
    """(highs, lows): indexes of closes that are the highest or lowest within k days either side."""
    hi = [i for i in range(k, len(xs) - k) if xs[i] == max(xs[i - k:i + k + 1])]
    lo = [i for i in range(k, len(xs) - k) if xs[i] == min(xs[i - k:i + k + 1])]
    return hi, lo


def chart_patterns(closes):
    """Classic chart shapes whose breakout happened on the last close, from the last 120 closes, oldest first."""
    xs = closes[-120:]
    if len(xs) < 40:
        return []
    t, last, prev = len(xs) - 1, xs[-1], xs[-2]
    hi, lo = swings(xs)
    found = []
    crossed_up = lambda lvl: last > lvl >= prev
    crossed_down = lambda lvl: last < lvl <= prev
    if len(lo) >= 2:
        a, b = lo[-2], lo[-1]
        peak = max(xs[a:b + 1])
        if b - a >= 10 and abs(xs[a] / xs[b] - 1) <= 0.03 and peak >= 1.05 * max(xs[a], xs[b]) and crossed_up(peak):
            found.append('double_bottom')
    if len(hi) >= 2:
        a, b = hi[-2], hi[-1]
        trough = min(xs[a:b + 1])
        if b - a >= 10 and abs(xs[a] / xs[b] - 1) <= 0.03 and trough <= 0.95 * min(xs[a], xs[b]) and crossed_down(trough):
            found.append('double_top')
    if len(hi) >= 3:
        s1, h, s2 = hi[-3:]
        neck = min(min(xs[s1:h + 1]), min(xs[h:s2 + 1]))
        if xs[h] >= 1.03 * max(xs[s1], xs[s2]) and abs(xs[s1] / xs[s2] - 1) <= 0.05 and crossed_down(neck):
            found.append('head_shoulders')
    if len(lo) >= 3:
        s1, h, s2 = lo[-3:]
        neck = max(max(xs[s1:h + 1]), max(xs[h:s2 + 1]))
        if xs[h] <= 0.97 * min(xs[s1], xs[s2]) and abs(xs[s1] / xs[s2] - 1) <= 0.05 and crossed_up(neck):
            found.append('inverse_head_shoulders')
    top = max(range(t - 25, t - 4), key=lambda i: xs[i])  # the pole's top: 5 to 25 days ago
    base = min(xs[max(0, top - 20):top])
    flag = xs[top + 1:t]
    if xs[top] >= 1.15 * base and flag and min(flag) >= 0.9 * xs[top] and crossed_up(max(flag)) and max(flag) < xs[top]:
        found.append('bull_flag')
    return found


def test():
    p = {'P/E': '25.92', 'P/B': '10.24', 'PEG': '2.97', 'ROE': '44.23%', 'ROIC': '19.57%', 'ROA': '13.49%', 'Profit Margin': '28.31%',
         'LT Debt/Eq': '1.02', 'Debt/Eq': '1.20', 'EPS past 3/5Y': '11.48% 11.14%', 'EPS next 5Y': '8.21%', 'P/FCF': '25.89',
         'EPS (ttm)': '3.32', 'Book/sh': '8.40', 'Price': '86.02', 'Income': '14.32B', 'Enterprise Value': '399.44B',
         'EPS Y/Y TTM': '17.58%', 'Sales Y/Y TTM': '7.42%', 'Current Ratio': '1.30', 'EPS Q/Q': '16.19%', 'Inst Trans': '-0.03%',
         'Market Cap': '370.11B', 'Dividend TTM': '2.10 (2.44%)', 'sector': 'Consumer Defensive', 'industry': 'Beverages - Non-Alcoholic'}
    assert num('370.11B') == 370.11e9 and num('-') is None and second('11.48% 11.14%') == 11.14
    ls = fundamental_lenses(p, market_up=True, beats_spy=True, near_high=True)
    assert ls['buffett'][0] == 'mixed' and 'long-term debt' in ls['buffett'][1], ls['buffett']   # Coca-Cola: debt 1.02 > 0.8
    assert ls['graham'][0] == 'fail' and '265.4' in ls['graham'][1], ls['graham']
    assert ls['greenblatt'][0] == 'mixed' and ls['piotroski'][0] == 'pass', ls
    assert lynch_type(p) == 'stalwart' and ls['lynch'][0] == 'fail', ls['lynch']   # PEG 25.92 / 8.21 = 3.2
    assert ls['oneil'][0] == 'mixed', ls['oneil']
    assert fundamental_lenses({'Asset Type': 'Equity'}) == {}
    # A steady climb passes Minervini and is Weinstein stage 2; a steady fall is stage 4.
    up = [100 * 1.003 ** i for i in range(260)]
    spy = [100 * 1.001 ** i for i in range(260)]
    t, f = trend_lenses(up, spy)
    assert t['minervini'][0] == 'pass' and t['weinstein'][0] == 'pass' and f == {'near_high': True, 'beats_spy': True}, (t, f)
    t, _ = trend_lenses([100 * 0.997 ** i for i in range(260)], spy)
    assert t['minervini'][0] == 'fail' and t['weinstein'][1].startswith('stage 4'), t
    # Candles: a fall, then a rising day engulfing the last falling day.
    B = lambda o, h, l, c: ('d', o, h, l, c)
    fall = [B(110 - 2 * i, 111 - 2 * i, 107 - 2 * i, 108 - 2 * i) for i in range(8)]   # each day 110->108, 108->106 ...
    assert candles(fall + [B(93.5, 97, 93, 96.5)]) == ['bullish_engulfing'], candles(fall + [B(93.5, 97, 93, 96.5)])
    assert candles(fall + [B(95, 95.2, 90, 94.8)]) == ['hammer'], candles(fall + [B(95, 95.2, 90, 94.8)])
    assert candles(fall + [B(95.6, 96, 95.4, 95.5)]) == []
    # Chart: two lows near 90 a month apart with a peak at 100 between, and today's close breaking above 100.
    xs = [100.0] * 20 + [100 - i for i in range(1, 11)] + [90 + i for i in range(1, 11)] + [100 - i for i in range(1, 11)] \
        + [90 + i * 0.95 for i in range(1, 11)] + [99.5, 101.0]
    assert chart_patterns(xs) == ['double_bottom'], chart_patterns(xs)
    flag = [100.0] * 30 + [100 + 1.5 * i for i in range(1, 15)] + [121 - 0.5 * i for i in range(1, 8)] + [122.0]
    assert 'bull_flag' in chart_patterns(flag), chart_patterns(flag)
    print('lenses ok')


if __name__ == '__main__':
    test()
