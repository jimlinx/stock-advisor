# Stock advisor

A one-off snapshot of a personal US-stock advisor web app, shared to read and borrow from. It is not maintained.
Names, domains and paths have been replaced with placeholders (owners `alex` and `sam`, a friend `chris`,
`stock.example.com`, `/opt/stock-advisor`).

## What it does

- Gathers market data (Yahoo, Finviz, MarketBeat, SEC filings) and news, then asks Claude for a daily market brief,
  per-stock research and trade proposals for each portfolio.
- SANDBOX accounts fill approved trades at the current Yahoo price. LIVE accounts only get an IBKR order *draft*,
  which the account holder submits in the IBKR app. Every trade needs a human approval either way.
- Tracks capital gains per owner (Australian tax rules).
- Scores past proposals weekly and runs a monthly review; lessons only apply after an owner keeps them.

## How it talks to Claude

There is no API key. `stock.py`'s `claude()` runs the Claude Code CLI headless (`claude -p --output-format stream-json
--json-schema ...`) as the logged-in user, so it uses that user's Claude subscription. IBKR is reached through the
claude.ai Interactive Brokers connectors on the same account. Each run pins `--model`, allows only the tools that job
needs and blocks every write tool unless the user has approved that exact order.

## Layout

- `stock/stock.py`: web app, daily analysis and the self-check (`python3 stock.py test`)
- `stock/feeds.py`, `lenses.py`, `learn.py`, `tax.py`, `dividends.py`: data feeds, investor checks, scoring, tax register, dividends
- `lib/webauth.py`: password file, sessions and login lockout
- `systemd/`: the web service and the analyse and weekly timers

## Running it

Python 3 standard library plus `bcrypt`, and Claude Code logged in. Paths and connector names are hard-coded near the
top of `stock.py` (`HOME`, `CLAUDE`, `ORIGIN`, `CONNECTORS`) and in `lib/webauth.py` (`AUTH_FILE`), so expect to edit
those first. Optional API keys (`FINNHUB_KEY`, `FRED_KEY`) are read from a `.env` file.
