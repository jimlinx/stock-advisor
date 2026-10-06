"""Capital-gains register for the tax accountant.

One row is one parcel: its purchase, and -- once sold -- the sale matched to it. IBKR trades
arrive with every live refresh (IBKR's API only reaches back four quarters): a BUY becomes a
new parcel, a SELL is matched to open parcels. Any row can be edited or split by hand.

Dates are the trade date on the exchange's own calendar (Alex, 2026-09-30): New York dates for US shares, Sydney
dates for ASX shares. Before then IBKR trades were given Sydney dates, which put a US trade on the next day. Amounts are in the
currency of the market (USD for IBKR, AUD for the ASX shares); no conversion is done.
"""
import csv, io, json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

SCHEMA = '''
CREATE TABLE IF NOT EXISTS lots (id INTEGER PRIMARY KEY, owner TEXT NOT NULL, currency TEXT NOT NULL,
  symbol TEXT NOT NULL, buy_date TEXT NOT NULL, buy_qty REAL NOT NULL, buy_price REAL NOT NULL,
  buy_fee REAL NOT NULL DEFAULT 0, sale_date TEXT, sale_qty REAL, sale_price REAL, sale_fee REAL,
  source TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS ibkr_seen (trade_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
'''
FIELDS = ['symbol', 'currency', 'buy_date', 'buy_qty', 'buy_price', 'buy_fee',
          'sale_date', 'sale_qty', 'sale_price', 'sale_fee', 'note']


# ---------------------------------------------------------------- IBKR trades
def sync(c, owner, trades_json):
    """Add IBKR stock trades not seen before. Returns rows added."""
    orders = {}
    for t in json.loads(trades_json).get('trades', []):
        if t.get('sec_type') != 'STK' or c.execute('SELECT 1 FROM ibkr_seen WHERE trade_id=?', (t['trade_id'],)).fetchone():
            continue
        day = datetime.fromisoformat(t['trade_time'].replace('Z', '+00:00')).astimezone(
            ZoneInfo('America/New_York' if t['currency'] == 'USD' else 'Australia/Sydney')).date().isoformat()
        o = orders.setdefault((t['order_id'], t['side'], t['symbol'], day),
                              {'qty': 0, 'value': 0, 'fee': 0, 'cur': t['currency'], 'ids': []})
        o['qty'] += t['size']
        o['value'] += t['size'] * t['price']
        o['fee'] += t.get('commission') or 0
        o['ids'].append(t['trade_id'])
    added = 0
    for (_, side, sym, day), o in sorted(orders.items(), key=lambda kv: (kv[0][3], kv[0][1] != 'BUY')):
        price = o['value'] / o['qty']
        if side == 'BUY':
            c.execute('INSERT INTO lots (owner, currency, symbol, buy_date, buy_qty, buy_price, buy_fee, source) '
                      'VALUES (?,?,?,?,?,?,?,?)', (owner, o['cur'], sym, day, o['qty'], price, o['fee'], 'ibkr'))
            added += 1
        else:
            added += sell(c, owner, o['cur'], sym, day, o['qty'], price, o['fee'])
        c.executemany('INSERT INTO ibkr_seen VALUES (?)', [(i,) for i in o['ids']])
    c.commit()
    return added


def taxable_per_share(lot, price, day):
    """What selling one share of this parcel at `price` on `day` adds to the taxable gain: a loss counts in full, a gain
    in full, or half once the parcel qualifies for the 50% discount."""
    g = price - (lot['buy_qty'] * lot['buy_price'] + lot['buy_fee']) / lot['buy_qty'] if lot['buy_qty'] else 0
    return g / 2 if g > 0 and date.fromisoformat(day) > plus_year(date.fromisoformat(lot['buy_date'])) else g


def sell(c, owner, cur, sym, day, qty, price, fee):
    """Match a sale to open parcels, splitting the last one if needed: the parcels that add the least taxable gain
    first (losses, then discounted gains, then the rest), oldest first among equals. The ATO lets the seller say which
    parcels were sold when they can be told apart, which a register like this does; IBKR's own lot matching does not
    decide Australian tax. Anything left unmatched becomes a row with no purchase, flagged for the accountant."""
    left, rows = qty, 0
    lots = c.execute('SELECT * FROM lots WHERE owner=? AND currency=? AND symbol=? AND sale_date IS NULL '
                     'ORDER BY buy_date, id', (owner, cur, sym)).fetchall()
    for lot in sorted(lots, key=lambda l: taxable_per_share(l, price, day)):  # stable: oldest first among equals
        if left < 1e-9:
            break
        take = min(left, lot['buy_qty'])
        if take < lot['buy_qty'] - 1e-9:  # split: the unsold remainder becomes its own parcel
            share = take / lot['buy_qty']
            c.execute('INSERT INTO lots (owner, currency, symbol, buy_date, buy_qty, buy_price, buy_fee, source, note) '
                      'VALUES (?,?,?,?,?,?,?,?,?)', (owner, cur, sym, lot['buy_date'], lot['buy_qty'] - take,
                                                      lot['buy_price'], lot['buy_fee'] * (1 - share), lot['source'], lot['note']))
            c.execute('UPDATE lots SET buy_qty=?, buy_fee=? WHERE id=?', (take, lot['buy_fee'] * share, lot['id']))
        c.execute('UPDATE lots SET sale_date=?, sale_qty=?, sale_price=?, sale_fee=? WHERE id=?',
                  (day, take, price, fee * take / qty, lot['id']))
        left -= take
        rows += 1
    if left > 1e-9:
        c.execute('INSERT INTO lots (owner, currency, symbol, buy_date, buy_qty, buy_price, buy_fee, sale_date, '
                  'sale_qty, sale_price, sale_fee, source, note) VALUES (?,?,?,?,0,0,0,?,?,?,?,?,?)',
                  (owner, cur, sym, day, day, left, price, fee * left / qty, 'ibkr',
                   'NO PURCHASE FOUND: enter the cost base'))
        rows += 1
    return rows


# ---------------------------------------------------------------- the report
def fy(d):
    y = int(d[:4]) if int(d[5:7]) >= 7 else int(d[:4]) - 1
    return f'{y}-{str(y + 1)[2:]}'


def plus_year(d):
    try:
        return d.replace(year=d.year + 1)
    except ValueError:  # 29 February
        return d.replace(year=d.year + 1, day=28)


def rows(c, owner):
    out = []
    for r in c.execute('SELECT * FROM lots WHERE owner=? ORDER BY currency DESC, sale_date IS NULL, '
                       'sale_date, buy_date, id', (owner,)):
        r = dict(r)
        r['cost'] = r['buy_qty'] * r['buy_price'] + r['buy_fee']
        if r['sale_date']:
            b, s = date.fromisoformat(r['buy_date']), date.fromisoformat(r['sale_date'])
            months = (s.year - b.year) * 12 + s.month - b.month - (s.day < b.day)
            r['proceeds'] = r['sale_qty'] * r['sale_price'] - r['sale_fee']
            r['pnl'] = r['proceeds'] - r['cost']
            r['held'] = f'{months // 12} Years {months % 12} Months'
            # ATO: held at least 12 months, not counting the days of purchase and sale.
            r['discount'] = s > plus_year(b)
            r['fy'] = fy(r['sale_date'])
        out.append(r)
    return out


def summary(rs):
    """Per (currency, FY): total, discount-eligible gains, other gains, losses."""
    s = {}
    for r in rs:
        if not r['sale_date']:
            continue
        t = s.setdefault((r['currency'], r['fy']), {'total': 0, 'long': 0, 'short': 0, 'loss': 0, 'n': 0})
        t['total'] += r['pnl']
        t['n'] += 1
        t['loss' if r['pnl'] < 0 else 'long' if r['discount'] else 'short'] += r['pnl']
    return dict(sorted(s.items(), key=lambda kv: (kv[0][0] != 'USD', kv[0][1]), reverse=False))


def net(summ):
    """{(currency, FY): {'net_gain', 'losses_carried_in', 'losses_carried_out'}}, an estimate of the net capital gain the
    way the ATO works it out: this year's losses and losses carried in are taken off gains held under 12 months first,
    then off the others, which are then halved by the discount; losses left over carry into the next year. summ:
    summary() (years in order). ponytail: per currency, with no conversion to AUD, and blind to losses from before the
    register starts; the accountant's figures are the real ones."""
    out, carry = {}, {}
    for (cur, y), t in summ.items():
        loss = -t['loss'] + carry.get(cur, 0)
        short = max(t['short'] - loss, 0)
        rest = max(loss - t['short'], 0)
        long_ = max(t['long'] - rest, 0)
        out[(cur, y)] = {'net_gain': short + long_ / 2, 'losses_carried_in': carry.get(cur, 0),
                         'losses_carried_out': max(rest - t['long'], 0)}
        carry[cur] = out[(cur, y)]['losses_carried_out']
    return out


def reconcile(rs, positions):
    """Unsold shares per symbol in the register vs IBKR's positions. Any difference means
    the register is missing a trade (or a split, or a stock grant)."""
    reg = {}
    for r in rs:
        if r['currency'] == 'USD' and not r['sale_date']:
            reg[r['symbol']] = reg.get(r['symbol'], 0) + r['buy_qty']
    ib = {p['symbol']: p['qty'] for p in positions}
    return [(s, reg.get(s, 0), ib.get(s, 0)) for s in sorted(set(reg) | set(ib))
            if abs(reg.get(s, 0) - ib.get(s, 0)) > 1e-6]


def to_csv(rs, year=None):
    f = io.StringIO()
    w = csv.writer(f)
    w.writerow(['Financial year', 'Currency', 'Sale Date', 'Share', 'No. of Shares', 'Share price', 'Transaction fees',
                'Total Proceed', 'Purchase Date', 'Share', 'No. of Shares', 'Share cost', 'Transaction fees',
                'Total Cost', 'Profit & Loss', 'Duration', '12-month discount', 'Note'])
    for r in rs:
        if year and r.get('fy') != year:
            continue
        sold = bool(r['sale_date'])
        w.writerow([r.get('fy', 'unsold'), r['currency'], r['sale_date'] or '', r['symbol'] if sold else '',
                    f'{r["sale_qty"]:g}' if sold else '', r['sale_price'] if sold else '',
                    f'{r["sale_fee"]:.2f}' if sold else '', f'{r["proceeds"]:.2f}' if sold else '',
                    r['buy_date'], r['symbol'], f'{r["buy_qty"]:g}', r['buy_price'], f'{r["buy_fee"]:.2f}',
                    f'{r["cost"]:.2f}', f'{r["pnl"]:.2f}' if sold else '---', r.get('held', '---'),
                    ('yes' if r['discount'] else 'no') if sold else '', r['note'] or ''])
    return f.getvalue()


def test():
    import sqlite3
    c = sqlite3.connect(':memory:')
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA)
    for d, q, p, fee in (('2025-01-10', 10, 100, 1), ('2025-06-10', 10, 200, 1)):
        c.execute('INSERT INTO lots (owner, currency, symbol, buy_date, buy_qty, buy_price, buy_fee, source) '
                  'VALUES ("alex","USD","AAA",?,?,?,?,"ibkr")', (d, q, p, fee))
    t = lambda i, side, q, p, when: {'trade_id': i, 'sec_type': 'STK', 'currency': 'USD', 'symbol': 'AAA',
                                     'side': side, 'size': q, 'price': p, 'commission': 1, 'order_id': i,
                                     'trade_time': when}
    trades = {'trades': [t('s1', 'SELL', 15, 300, '2026-03-13T15:00:00Z')]}      # 10 from lot 2, 5 from lot 1
    assert sync(c, 'alex', json.dumps(trades)) == 2
    assert sync(c, 'alex', json.dumps(trades)) == 0                              # idempotent
    rs = rows(c, 'alex')
    sold = [r for r in rs if r['sale_date']]
    # Lot 2 first: its gain of 99.90 a share is taxed in full, lot 1's 199.90 is halved by the discount to 99.95.
    assert sorted((r['buy_date'], r['sale_qty']) for r in sold) == [('2025-01-10', 5), ('2025-06-10', 10)]
    s2 = next(r for r in sold if r['buy_date'] == '2025-06-10')
    assert abs(s2['pnl'] - (10 * 300 - 1 * 10 / 15 - 2001)) < 1e-9
    assert not s2['discount'] and s2['fy'] == '2025-26' and next(r for r in sold if r['buy_date'] == '2025-01-10')['discount']
    open_ = [r for r in rs if not r['sale_date']]
    assert len(open_) == 1 and open_[0]['buy_qty'] == 5 and open_[0]['buy_date'] == '2025-01-10' and abs(open_[0]['buy_fee'] - 0.5) < 1e-9
    # A loss parcel goes before any gain.
    assert taxable_per_share({'buy_qty': 1, 'buy_price': 400, 'buy_fee': 0, 'buy_date': '2026-01-01'}, 300, '2026-03-13') == -100
    # Net gain: losses off short-term gains first, the rest off long-term, which are halved; the excess carries on.
    S = lambda fy, short, long_, loss: {('USD', fy): {'short': short, 'long': long_, 'loss': loss, 'total': 0, 'n': 1}}
    got = net({**S('2024-25', 0, 0, -500), **S('2025-26', 300, 1000, -100)})
    assert got[('USD', '2024-25')]['losses_carried_out'] == 500 and got[('USD', '2025-26')]['net_gain'] == (1000 - 300) / 2, got
    assert reconcile(rs, [{'symbol': 'AAA', 'qty': 5}]) == []
    assert reconcile(rs, [{'symbol': 'AAA', 'qty': 6}]) == [('AAA', 5, 6)]
    # ATO 12 months excludes both days: bought 10 Jan 2025, first eligible sale is 11 Jan 2026.
    assert not (date(2026, 1, 10) > plus_year(date(2025, 1, 10))) and date(2026, 1, 11) > plus_year(date(2025, 1, 10))
    print('tax ok')


if __name__ == '__main__':
    test()
