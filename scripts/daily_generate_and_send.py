"""
Daily Market Brief — Script d'automatisation v1
Gmail (label Daily Market Watch) + yfinance + FRED + Finnhub → Claude analyse → PDF → Email
Runs every day at 7pm Panama (00:00 UTC)
"""

import os, json, base64, re, requests, time
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO
from pathlib import Path

import anthropic
import yfinance as yf
import pandas as pd

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, PageBreak, HRFlowable
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT

# ─── Secrets ──────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
ANTHROPIC_MODEL     = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
GMAIL_REFRESH_TOKEN = os.environ["GMAIL_REFRESH_TOKEN"]
GMAIL_CLIENT_ID     = os.environ["GMAIL_CLIENT_ID"]
GMAIL_CLIENT_SECRET = os.environ["GMAIL_CLIENT_SECRET"]
EMAIL_FROM          = os.environ["EMAIL_FROM"]
EMAIL_TO            = os.environ["EMAIL_TO"]
FRED_API_KEY        = os.environ["FRED_API_KEY"]
FINNHUB_API_KEY     = os.environ.get("FINNHUB_API_KEY", "")

NOW     = datetime.now(timezone.utc)
DATE_FR = NOW.strftime("%A %d %B %Y")
TODAY   = NOW.strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════════════════════
#  TOKEN GMAIL
# ══════════════════════════════════════════════════════════════════════════════
def get_fresh_access_token() -> str:
    print("[0/6] Renouvellement du token Gmail...")
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": GMAIL_CLIENT_ID, "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": GMAIL_REFRESH_TOKEN, "grant_type": "refresh_token",
    })
    if resp.status_code != 200:
        raise RuntimeError(f"Token Gmail impossible : {resp.text}")
    print("[0/6] ✅ Token Gmail renouvelé")
    return resp.json()["access_token"]


# ══════════════════════════════════════════════════════════════════════════════
#  LECTURE EMAILS GMAIL — label "Daily Market Watch"
# ══════════════════════════════════════════════════════════════════════════════
def fetch_gmail_emails(access_token: str) -> list[dict]:
    """Reads emails from the last 24h with label 'Daily Market Watch'."""
    print("[1/6] Lecture des emails Gmail (label: Daily Market Watch)...")
    headers = {"Authorization": f"Bearer {access_token}"}
    base = "https://gmail.googleapis.com/gmail/v1/users/me"

    # Compute Unix timestamp for 24h ago
    since_ts = int((NOW - timedelta(hours=24)).timestamp())
    query = f"label:Daily-Market-Watch after:{since_ts}"

    # List ALL message IDs — no maxResults cap, paginate if needed
    all_message_ids = []
    page_token = None
    while True:
        params = {"q": query, "maxResults": 500}
        if page_token:
            params["pageToken"] = page_token
        r = requests.get(f"{base}/messages", headers=headers, params=params)
        if r.status_code != 200:
            print(f"  ⚠️ Gmail list failed: {r.text}")
            break
        page_data = r.json()
        all_message_ids.extend([m["id"] for m in page_data.get("messages", [])])
        page_token = page_data.get("nextPageToken")
        if not page_token:
            break

    message_ids = all_message_ids
    print(f"  → {len(message_ids)} email(s) trouvé(s) dans les dernières 24h")

    emails = []
    for mid in message_ids:  # ALL emails, no cap
        try:
            r2 = requests.get(f"{base}/messages/{mid}", headers=headers,
                              params={"format": "full"})
            if r2.status_code != 200:
                continue
            msg = r2.json()

            # Extract subject and date from headers
            hdrs = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            subject = hdrs.get("Subject", "(no subject)")
            sender  = hdrs.get("From", "unknown")
            date    = hdrs.get("Date", "")

            # Extract body text — 6000 chars per email to preserve content
            body = _extract_body(msg.get("payload", {}))
            if body:
                emails.append({
                    "subject": subject,
                    "from": sender,
                    "date": date,
                    "body": body[:6000]
                })
                print(f"  ✉️  {subject[:70]} — {sender[:50]}")
        except Exception as e:
            print(f"  ⚠️ Error reading message {mid}: {e}")
        except Exception as e:
            print(f"  ⚠️ Error reading message {mid}: {e}")

    print(f"[1/6] ✅ {len(emails)} email(s) chargé(s)")
    return emails


def _extract_body(payload: dict) -> str:
    """Recursively extracts plain text body from Gmail payload."""
    mime_type = payload.get("mimeType", "")
    if mime_type == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
    elif mime_type == "text/html":
        data = payload.get("body", {}).get("data", "")
        if data:
            html = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
            # Strip HTML tags simply
            return re.sub(r'<[^>]+>', ' ', html)

    # Recurse into parts
    parts = payload.get("parts", [])
    for part in parts:
        result = _extract_body(part)
        if result and len(result.strip()) > 50:
            return result
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  DONNÉES MARCHÉ — yfinance (gratuit) + FRED
# ══════════════════════════════════════════════════════════════════════════════
def fetch_yfinance_data() -> dict:
    """Fetch real-time market data using yfinance.
    Computes: daily change, weekly change (5 trading days), YTD change.
    All values are Python-calculated — Claude must NOT recompute or estimate them.
    """
    print("  [yfinance] Fetching market data...")
    data = {}
    year_start = f"{NOW.year}-01-01"

    # ── Helper: safe pct change
    def pct(new, old):
        try:
            if new and old and old != 0:
                return round(((new - old) / abs(old)) * 100, 2)
        except Exception:
            pass
        return None

    # ── Helper: fetch price + daily change via fast_info
    def quick_price(ticker, decimals=2):
        try:
            info = yf.Ticker(ticker).fast_info
            price = round(float(info.last_price), decimals) if info.last_price else None
            prev  = round(float(info.previous_close), decimals) if info.previous_close else None
            return price, prev, pct(price, prev)
        except Exception:
            return None, None, None

    # ══ INDICES — daily + weekly + YTD ════════════════════════════════════════
    index_tickers = {
        "S&P 500":      "^GSPC",
        "NASDAQ 100":   "^NDX",
        "Dow Jones":    "^DJI",
        "Russell 2000": "^RUT",
        "VIX":          "^VIX",
        "CAC 40":       "^FCHI",
        "DAX":          "^GDAXI",
        "FTSE 100":     "^FTSE",
        "EuroStoxx 50": "^STOXX50E",
        "Nikkei 225":   "^N225",
        "Hang Seng":    "^HSI",
        "Shanghai":     "000001.SS",
    }

    # Batch download: YTD history and 7-day history for all indices at once
    all_idx_tickers = list(index_tickers.values())
    print("    [yfinance] Downloading YTD history for indices...")
    try:
        hist_ytd  = yf.download(all_idx_tickers, start=year_start,
                                progress=False, auto_adjust=True, threads=True)
        hist_week = yf.download(all_idx_tickers, period="7d",
                                progress=False, auto_adjust=True, threads=True)
    except Exception as e:
        print(f"    ⚠️ Batch download failed: {e}")
        hist_ytd  = None
        hist_week = None

    idx_data = {}
    for name, ticker in index_tickers.items():
        price, prev, chg_1d = quick_price(ticker, 2)
        if not price:
            continue

        # YTD
        chg_ytd = None
        try:
            if hist_ytd is not None and not hist_ytd.empty:
                col = ("Close", ticker) if isinstance(hist_ytd.columns, pd.MultiIndex) else "Close"
                series = hist_ytd[col].dropna()
                if not series.empty:
                    jan1 = float(series.iloc[0])
                    chg_ytd = pct(price, jan1)
        except Exception:
            pass

        # Weekly (5 trading days back)
        chg_week = None
        try:
            if hist_week is not None and not hist_week.empty:
                col = ("Close", ticker) if isinstance(hist_week.columns, pd.MultiIndex) else "Close"
                series = hist_week[col].dropna()
                if len(series) >= 2:
                    week_open = float(series.iloc[0])
                    chg_week = pct(price, week_open)
        except Exception:
            pass

        idx_data[name] = {
            "price":    price,
            "chg_1d":   chg_1d,   # daily % change — CALCULATED, not estimated
            "chg_week": chg_week, # 5-day % change — CALCULATED, not estimated
            "chg_ytd":  chg_ytd,  # YTD % change  — CALCULATED from Jan 1 close
        }

    data["indices"] = idx_data
    print(f"    ✅ {len(idx_data)} indices loaded with YTD and weekly data")

    # ══ SECTOR ETFs — daily change, properly named ═════════════════════════════
    # Explicit ticker→sector name mapping (no ambiguity for Claude)
    sector_etfs = {
        "XLK":  "Technology",
        "XLF":  "Financials",
        "XLE":  "Energy",
        "XLV":  "Healthcare",
        "XLI":  "Industrials",
        "XLY":  "Consumer Discr.",
        "XLB":  "Materials",
        "XLU":  "Utilities",
        "XLRE": "Real Estate",
        "XLC":  "Comm. Services",
        "XLP":  "Cons. Staples",
    }
    sector_data = {}
    for ticker, sector_name in sector_etfs.items():
        price, prev, chg = quick_price(ticker, 2)
        if price:
            sector_data[sector_name] = {
                "ticker":   ticker,
                "price":    price,
                "chg_1d":   chg,  # daily % — CALCULATED
            }
    data["sectors"] = sector_data
    print(f"    ✅ {len(sector_data)} sectors loaded")

    # ══ FX RATES ═══════════════════════════════════════════════════════════════
    fx_pairs = {
        "EUR/USD": "EURUSD=X", "GBP/USD": "GBPUSD=X", "USD/JPY": "JPY=X",
        "USD/CHF": "CHF=X",    "AUD/USD": "AUDUSD=X", "USD/CNY": "CNY=X",
        "USD/BRL": "BRL=X",    "USD/MXN": "MXN=X",    "USD/CAD": "CAD=X",
        "DXY":     "DX-Y.NYB",
    }
    fx_data = {}
    for pair, ticker in fx_pairs.items():
        price, prev, chg = quick_price(ticker, 4)
        if price:
            fx_data[pair] = {"price": price, "chg_1d": chg}
    data["fx"] = fx_data

    # ══ COMMODITIES (futures) ══════════════════════════════════════════════════
    commodities = {
        "Gold":         ("GC=F",  "$/oz",    2),
        "Silver":       ("SI=F",  "$/oz",    2),
        "WTI Crude":    ("CL=F",  "$/bbl",   2),
        "Brent Crude":  ("BZ=F",  "$/bbl",   2),
        "Natural Gas":  ("NG=F",  "$/MMBtu", 3),
        "Copper":       ("HG=F",  "$/lb",    3),
        "Wheat":        ("ZW=F",  "¢/bu",    2),
        "Corn":         ("ZC=F",  "¢/bu",    2),
        "Soybeans":     ("ZS=F",  "¢/bu",    2),
    }
    cmd_data = {}
    for name, (ticker, unit, dec) in commodities.items():
        price, prev, chg = quick_price(ticker, dec)
        if price:
            cmd_data[name] = {"price": price, "unit": unit, "chg_1d": chg}
    data["commodities"] = cmd_data

    # ══ US TREASURIES ══════════════════════════════════════════════════════════
    bond_tickers = {"US 2Y": "^IRX", "US 10Y": "^TNX", "US 30Y": "^TYX"}
    bond_data = {}
    for name, ticker in bond_tickers.items():
        try:
            info  = yf.Ticker(ticker).fast_info
            yld   = round(float(info.last_price), 3) if info.last_price else None
            prev  = round(float(info.previous_close), 3) if info.previous_close else None
            chg_bp = round((yld - prev) * 100, 1) if yld and prev else None
            if yld:
                bond_data[name] = {"yield_pct": yld, "chg_bp": chg_bp}
        except Exception:
            pass
    # Compute 2s10s spread if both available
    if "US 2Y" in bond_data and "US 10Y" in bond_data:
        spread = round(
            (bond_data["US 10Y"]["yield_pct"] - bond_data["US 2Y"]["yield_pct"]) * 100, 1
        )
        bond_data["spread_2s10s_bp"] = spread
    data["bonds"] = bond_data

    # ══ BROAD MARKET ETFs (sentiment / flow) ══════════════════════════════════
    broad_etfs = {
        "SPY": "S&P 500", "QQQ": "NASDAQ 100", "IWM": "Russell 2000",
        "GLD": "Gold",    "TLT": "Long Bond",   "HYG": "High Yield",
        "LQD": "Inv.Grade","EEM": "EM Equities", "ARKK": "Innovation",
    }
    etf_data = {}
    for ticker, label in broad_etfs.items():
        price, prev, chg = quick_price(ticker, 2)
        if price:
            etf_data[ticker] = {"label": label, "price": price, "chg_1d": chg}
    data["etfs"] = etf_data

    print(f"  [yfinance] ✅ {len(idx_data)} indices | {len(fx_data)} FX | "
          f"{len(cmd_data)} commodities | {len(sector_data)} sectors")
    return data


def fetch_fred(series_id: str) -> dict:
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations", params={
            "series_id": series_id, "api_key": FRED_API_KEY,
            "file_type": "json", "limit": 3, "sort_order": "desc"
        }, timeout=10)
        if r.status_code == 200:
            obs = r.json().get("observations", [])
            return {"latest": obs[0] if obs else {}, "prev": obs[1] if len(obs) > 1 else {}}
    except Exception as e:
        print(f"  ⚠️ FRED {series_id}: {e}")
    return {}


def fetch_finnhub_calendar(today: str, end: str) -> dict:
    if not FINNHUB_API_KEY:
        return {}
    try:
        r = requests.get("https://finnhub.io/api/v1/calendar/economic",
                         params={"from": today, "to": end, "token": FINNHUB_API_KEY}, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  ⚠️ Finnhub calendar: {e}")
    return {}


def collect_all_data() -> dict:
    print("[2/6] Collecte des données marché...")
    data = {}

    # yfinance (gratuit, no API key)
    data["market"] = fetch_yfinance_data()

    # FRED (macro US)
    print("  [FRED] Fetching macro data...")
    fred_series = {
        "fed_funds_rate": "FEDFUNDS",
        "unemployment":   "UNRATE",
        "cpi_yoy":        "CPIAUCSL",
        "10y_treasury":   "DGS10",
        "2y_treasury":    "DGS2",
        "credit_spreads": "BAMLH0A0HYM2",
        "vix":            "VIXCLS",
        "m2":             "M2SL",
    }
    fred_data = {}
    for name, sid in fred_series.items():
        r = fetch_fred(sid)
        if r.get("latest"):
            fred_data[name] = {
                "value": r["latest"].get("value"),
                "date":  r["latest"].get("date"),
                "prev":  r["prev"].get("value") if r.get("prev") else None
            }
    data["fred"] = fred_data

    # Finnhub economic calendar (next 2 days)
    tomorrow = (NOW + timedelta(days=2)).strftime("%Y-%m-%d")
    cal = fetch_finnhub_calendar(TODAY, tomorrow)
    events = cal.get("economicCalendar", [])
    data["economic_calendar"] = [e for e in events if e.get("impact") in ("high", "3", 3)][:10]

    print(f"[2/6] ✅ Données collectées")
    return data


# ══════════════════════════════════════════════════════════════════════════════
#  CLAUDE : GÉNÉRATION DU RAPPORT
# ══════════════════════════════════════════════════════════════════════════════
def fmt(val, suffix="", na="N/A"):
    """Format a value with sign and suffix, or return N/A."""
    if val is None:
        return na
    try:
        f = float(val)
        return f"{f:+.2f}{suffix}" if suffix else f"{f}"
    except Exception:
        return str(val)


def format_market_context(data: dict) -> str:
    """
    Build a strict, explicitly-labelled data block for Claude.
    Every field name maps 1-to-1 to a JSON key Claude must populate.
    Claude is NOT allowed to estimate any of these values.
    """
    mkt  = data.get("market", {})
    fred = data.get("fred", {})

    lines = [
        "╔══════════════════════════════════════════════════════════════╗",
        "║  VERIFIED MARKET DATA — ALL VALUES PYTHON-CALCULATED        ║",
        "║  Claude must copy these values verbatim into the JSON.      ║",
        "║  If a value shows N/A → output N/A, do NOT estimate.        ║",
        "╚══════════════════════════════════════════════════════════════╝",
        "",
    ]

    # ── INDICES (daily + weekly + YTD — all calculated in Python)
    lines.append("=== INDICES ===")
    lines.append("  Format: NAME | price | daily_chg | weekly_chg_5d | ytd_chg")
    for name, v in mkt.get("indices", {}).items():
        lines.append(
            f"  {name} | "
            f"price={v['price']} | "
            f"daily={fmt(v.get('chg_1d'), '%')} | "
            f"weekly_5d={fmt(v.get('chg_week'), '%')} | "
            f"ytd={fmt(v.get('chg_ytd'), '%')}"
        )

    # ── SECTOR PERFORMANCE (daily, from sector ETFs — explicit mapping)
    lines.append("\n=== SECTOR PERFORMANCE (daily %, from SPDR ETFs) ===")
    lines.append("  Format: SECTOR_NAME | daily_chg | etf_ticker | etf_price")
    for sector, v in mkt.get("sectors", {}).items():
        lines.append(
            f"  {sector} | "
            f"daily={fmt(v.get('chg_1d'), '%')} | "
            f"ticker={v.get('ticker','?')} | "
            f"price={v.get('price','N/A')}"
        )

    # ── FX RATES
    lines.append("\n=== FX RATES ===")
    lines.append("  Format: PAIR | rate | daily_chg")
    for pair, v in mkt.get("fx", {}).items():
        lines.append(
            f"  {pair} | "
            f"rate={v['price']} | "
            f"daily={fmt(v.get('chg_1d'), '%')}"
        )

    # ── COMMODITIES
    lines.append("\n=== COMMODITIES ===")
    lines.append("  Format: NAME | price | unit | daily_chg")
    for name, v in mkt.get("commodities", {}).items():
        lines.append(
            f"  {name} | "
            f"price={v['price']} | "
            f"unit={v.get('unit','?')} | "
            f"daily={fmt(v.get('chg_1d'), '%')}"
        )

    # ── US TREASURIES
    lines.append("\n=== US TREASURY YIELDS ===")
    bonds = mkt.get("bonds", {})
    for name, v in bonds.items():
        if name == "spread_2s10s_bp":
            continue
        bp = f"{v['chg_bp']:+.1f}bp" if v.get("chg_bp") is not None else "N/A"
        lines.append(f"  {name} | yield={v['yield_pct']}% | change={bp}")
    spread = bonds.get("spread_2s10s_bp")
    if spread is not None:
        sign = "+" if spread >= 0 else ""
        lines.append(f"  2s10s spread = {sign}{spread}bp  "
                     f"({'INVERTED — recession signal' if spread < 0 else 'NORMAL — positive slope'})")

    # ── BROAD ETFs (sentiment / flow signals)
    lines.append("\n=== BROAD ETFs (flow / sentiment signals) ===")
    lines.append("  Format: TICKER (label) | price | daily_chg")
    for ticker, v in mkt.get("etfs", {}).items():
        lines.append(
            f"  {ticker} ({v['label']}) | "
            f"price=${v['price']} | "
            f"daily={fmt(v.get('chg_1d'), '%')}"
        )

    # ── FRED MACRO
    lines.append("\n=== FRED MACRO DATA (most recent official reading) ===")
    for name, v in fred.items():
        prev_str = f" | prev_reading={v['prev']}" if v.get("prev") else ""
        lines.append(f"  {name} = {v['value']} (as of {v['date']}){prev_str}")

    # ── ECONOMIC CALENDAR
    cal = data.get("economic_calendar", [])
    if cal:
        lines.append("\n=== UPCOMING HIGH-IMPACT ECONOMIC EVENTS ===")
        for e in cal[:8]:
            lines.append(
                f"  [{e.get('country','?')}] {e.get('event','')} | "
                f"time={e.get('time','')} | "
                f"prev={e.get('prev','?')} | "
                f"estimate={e.get('estimate','?')}"
            )
    else:
        lines.append("\n=== UPCOMING HIGH-IMPACT ECONOMIC EVENTS ===")
        lines.append("  No high-impact events in the next 48h.")

    return "\n".join(lines)


def format_email_context(emails: list[dict]) -> str:
    if not emails:
        return "=== EMAIL INTELLIGENCE ===\nNo emails received in the last 24h under label 'Daily Market Watch'.\n"

    lines = [f"=== EMAIL INTELLIGENCE ({len(emails)} emails from last 24h, label: Daily Market Watch) ===\n"]
    for i, email in enumerate(emails, 1):
        lines.append(f"--- Email {i}: {email['subject']} ---")
        lines.append(f"From: {email['from']} | Date: {email['date']}")
        lines.append(email['body'])  # Full body, no truncation
        lines.append("")
    return "\n".join(lines)


def generate_report_json(market_data: dict, emails: list[dict]) -> dict:
    print(f"[3/6] Calling Claude ({ANTHROPIC_MODEL})...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    market_context = format_market_context(market_data)
    email_context  = format_email_context(emails)

    tomorrow_str = (NOW + timedelta(days=1)).strftime("%B %d, %Y")
    fred_vix     = market_data.get("fred", {}).get("vix", {}).get("value", "N/A")
    mkt_bonds_vix = fred_vix

    system = (
        f"You are a senior financial analyst (ex-Goldman Sachs, ex-Bridgewater). "
        f"You produce a daily global markets intelligence brief for {DATE_FR}. "
        f"You will be given VERIFIED market data that was Python-calculated. "
        f"\n\n"
        f"ABSOLUTE DATA RULES — NON-NEGOTIABLE:\n"
        f"1. NEVER invent, estimate, or approximate any numerical value (price, %, bp, yield).\n"
        f"2. NEVER change a number that was given to you — copy it exactly.\n"
        f"3. If a field shows N/A in the data, output N/A in the JSON — never guess.\n"
        f"4. The 'change' fields in the JSON must come ONLY from the data block, nothing else.\n"
        f"5. For 'ytd', use the ytd= value from the INDICES section — it is already calculated.\n"
        f"6. For sector performance, use ONLY the daily= values from the SECTOR PERFORMANCE section.\n"
        f"7. Your job is ANALYSIS and NARRATIVE — numbers come from the data, words come from you.\n"
        f"\n"
        f"Style: dense analytical prose, explanatory, first-person institutional voice. "
        f"ALL text in JSON must be in ENGLISH. "
        f"Reply ONLY with valid JSON, no markdown fences, no surrounding text."
    )

    user = f"""Generate a daily markets intelligence brief for {DATE_FR}.

{market_context}

{email_context}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ANALYTICAL INSTRUCTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Synthesize email intelligence + verified market data
- Explain causality and second-order effects
- Dense analytical paragraphs — no bullet lists
- Surface what the consensus is missing
- All text in ENGLISH

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA POPULATION RULES (READ CAREFULLY)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For the "indices" array: populate each object using EXACT values from the INDICES section above.
  - "value"  → the price= field
  - "change" → the daily= field formatted as "+X.XX%" or "-X.XX%"
  - "ytd"    → the ytd= field formatted as "+X.XX%" or "-X.XX%"
  - If ytd=N/A → output "N/A", never guess

For "sector_performance": populate using EXACT values from SECTOR PERFORMANCE section.
  - "change"     → the daily= field
  - "change_num" → the raw number (positive or negative float)
  - "direction"  → "up" if positive, "down" if negative, "flat" if zero/N/A

For FX pairs: use rate= and daily= from FX RATES section.
For commodities: use price= and daily= from COMMODITIES section.
For yield_curve: use yield= from US TREASURY YIELDS section.
  - spread_2_10 → use the "2s10s spread = Xbp" line

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return this JSON (ALL keys mandatory):

{{
  "date": "{DATE_FR}",
  "generated_at": "{NOW.isoformat()}",
  "market_temperature": "risk_on|risk_off|neutral",
  "market_temperature_label": "One punchy sentence explaining today's market mood",

  "section1_overview": {{
    "title": "Global Market Overview & Sentiment",
    "headline": "One punchy sentence — the dominant market narrative today",
    "paragraphs": [
      "§1 — What moved across markets today and why (dominant narrative, risk tone) (6-8 lines)",
      "§2 — Macro backdrop and catalyst chain that explains the mood (5-6 lines)",
      "§3 — Secondary cross-currents and what the consensus is missing (4-5 lines)"
    ]
  }},

  "section2_macro": {{
    "title": "Macro-Economic Environment",
    "paragraphs": [
      "§1 — Inflation trend using FRED cpi_yoy value, labor market using FRED unemployment, liquidity (5-6 lines)",
      "§2 — How these macro forces transmit into asset prices today (4-5 lines)",
      "§3 — What could break the current equilibrium — explicit scenario (3-4 lines)"
    ]
  }},

  "section3_equities": {{
    "title": "Equity Markets",
    "indices": [
      {{"name": "S&P 500",      "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "NASDAQ 100",   "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "Dow Jones",    "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "Russell 2000", "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "VIX",          "value": "COPY from data", "change": "COPY daily from data", "ytd": "N/A"}},
      {{"name": "CAC 40",       "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "DAX",          "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "EuroStoxx 50", "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "Nikkei 225",   "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "Hang Seng",    "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}},
      {{"name": "Shanghai",     "value": "COPY from data", "change": "COPY daily from data", "ytd": "COPY ytd from data"}}
    ],
    "sector_performance": [
      {{"sector": "Technology",      "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Financials",      "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Energy",          "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Healthcare",      "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Industrials",     "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Consumer Discr.", "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Materials",       "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Utilities",       "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Real Estate",     "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Comm. Services",  "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}},
      {{"sector": "Cons. Staples",   "change": "COPY daily from data", "direction": "up/down", "change_num": 0.0}}
    ],
    "us": {{
      "headline": "...",
      "body": "US deep dive: sector rotation, flows, earnings catalysts. Reference the real S&P 500 price and daily change from the data. (8-10 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "Main driver in one sentence",
      "risk": "Main risk in one sentence"
    }},
    "europe": {{
      "headline": "...",
      "body": "European analysis: ECB, EUR dynamics, sector leaders/laggards. Reference real DAX/CAC data. (6-8 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }},
    "asia": {{
      "headline": "...",
      "body": "Asian analysis: China, Japan BOJ, EM. Reference real Nikkei/Hang Seng data. (6-8 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }}
  }},

  "section4_fixed_income": {{
    "title": "Fixed Income & Rates",
    "yield_curve": {{
      "us_2y":        "COPY yield from data",
      "us_10y":       "COPY yield from data",
      "us_30y":       "COPY yield from data",
      "spread_2_10":  "COPY 2s10s spread from data (e.g. +45bp or -12bp)",
      "interpretation": "Yield curve analysis: shape, inversion status, growth signal. (4-5 lines)"
    }},
    "narrative": "Full fixed income analysis: Fed stance, bond supply/demand, credit spreads from FRED, CB credibility. (8-10 lines)"
  }},

  "section5_forex": {{
    "title": "Foreign Exchange",
    "dxy": {{
      "value":          "COPY DXY rate from data",
      "change":         "COPY DXY daily from data",
      "interpretation": "DXY analysis and global implications (3-4 lines)"
    }},
    "narrative": "Global FX dynamics: rate differentials, CB divergence, carry trades. (6-8 lines)",
    "pairs": [
      {{"pair": "EUR/USD", "value": "COPY rate",  "change": "COPY daily", "change_num": 0.0, "direction": "up|down|flat", "analysis": "ECB/Fed divergence, eurozone data, positioning (3-4 lines)", "support": "technical level", "resistance": "technical level"}},
      {{"pair": "GBP/USD", "value": "COPY rate",  "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "USD/JPY", "value": "COPY rate",  "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "USD/CHF", "value": "COPY rate",  "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "AUD/USD", "value": "COPY rate",  "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "USD/CNY", "value": "COPY rate",  "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}}
    ]
  }},

  "section6_commodities": {{
    "title": "Commodities & Alternative Assets",
    "narrative": "Cross-asset commodities narrative: inflation signal, rotation, geopolitics, dollar. (6-8 lines)",
    "items": [
      {{"name": "Gold",        "value": "COPY price", "unit": "$/oz",    "change": "COPY daily", "change_num": 0.0, "direction": "up|down", "analysis": "2-3 lines: safe-haven flows, real yields, dollar impact"}},
      {{"name": "WTI Crude",   "value": "COPY price", "unit": "$/bbl",   "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Brent Crude", "value": "COPY price", "unit": "$/bbl",   "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Natural Gas", "value": "COPY price", "unit": "$/MMBtu", "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Copper",      "value": "COPY price", "unit": "$/lb",    "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Silver",      "value": "COPY price", "unit": "$/oz",    "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Wheat",       "value": "COPY price", "unit": "¢/bu",    "change": "COPY daily", "change_num": 0.0, "direction": "...", "analysis": "..."}}
    ]
  }},

  "section7_positioning": {{
    "title": "Market Positioning & Sentiment",
    "narrative": "VIX={mkt_bonds_vix} from FRED data. ETF flows analysis using TLT/HYG/GLD data above. Put/call ratio interpretation, skew, institutional positioning signals. Complacency or fear — and why. (8-10 lines)"
  }},

  "section8_email_intelligence": {{
    "title": "Email Intelligence — Daily Market Watch",
    "summary": "Synthesis of email intelligence: key themes, second-order implications. Connect to market moves above. (5-7 lines, or state no emails if none received)",
    "key_stories": [
      {{"title": "Story from email", "body": "2-3 line market analysis connecting this story", "relevance": "high|medium"}}
    ]
  }},

  "section9_synthesis": {{
    "title": "Daily Synthesis & Outlook",
    "regime": "risk_on|risk_off|transition|stagflation|goldilocks",
    "global_view": "Holistic synthesis: interconnections between rates/equities/dollar/commodities today. Reference actual prices from data. Institutional flows, macro backdrop, tail risks. (10-12 lines minimum)",
    "tomorrow_watch": "Key events and data to watch on {tomorrow_str} — specific times and expected market impact (4-5 lines)",
    "strategy": [
      {{"type": "Tactical",    "recommendation": "...", "rationale": "3-4 line rationale referencing real market levels", "timeframe": "1-3 days", "conviction": "high|medium|low"}},
      {{"type": "Risk Watch",  "recommendation": "...", "rationale": "...", "timeframe": "...", "conviction": "..."}},
      {{"type": "Opportunity", "recommendation": "...", "rationale": "...", "timeframe": "...", "conviction": "..."}}
    ]
  }}
}}

REMINDER: copy ALL numerical values from the data block above. Do not change a single digit."""

    for attempt in range(3):
        try:
            raw = ""
            with client.messages.stream(
                model=ANTHROPIC_MODEL, max_tokens=16000,
                system=system, messages=[{"role": "user", "content": user}]
            ) as stream:
                for text in stream.text_stream:
                    raw += text
            print(f"[3/6] Streaming complete — {len(raw)} chars")
            match = re.search(r'\{[\s\S]*\}', raw)
            if not match:
                print(f"[3/6] ⚠️ No JSON found (attempt {attempt + 1}/3)")
                continue
            report = json.loads(match.group())
            print("[3/6] ✅ JSON report generated")
            return report
        except json.JSONDecodeError as e:
            print(f"[3/6] ⚠️ JSON parse error (attempt {attempt + 1}/3): {e}")
            time.sleep(3)
        except Exception as e:
            print(f"[3/6] ⚠️ Claude error (attempt {attempt + 1}/3): {e}")
            time.sleep(5)

    raise RuntimeError("Failed to generate report after 3 attempts")


def save_json(report: dict, market_data: dict):
    Path("data").mkdir(exist_ok=True)
    out = {"report": report, "market_data": market_data, "generated": NOW.isoformat()}
    with open("data/latest-daily-report.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("[3/6] ✅ JSON sauvegardé dans data/latest-daily-report.json")


# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION PDF — v2 (design matching weekly brief)
# ══════════════════════════════════════════════════════════════════════════════
from reportlab.platypus import KeepTogether
from reportlab.lib.enums import TA_RIGHT, TA_LEFT

# Color palette
BG      = colors.HexColor("#07070D")
BG2     = colors.HexColor("#0D0D16")
BG3     = colors.HexColor("#12121C")
GOLD    = colors.HexColor("#B8965A")
GOLD_LT = colors.HexColor("#D4AF7A")
GOLD_DIM= colors.HexColor("#2A2010")
LIGHT   = colors.HexColor("#E8E8F4")
TEXT    = colors.HexColor("#C8C8D8")
TEXT2   = colors.HexColor("#9898B0")
MUTED   = colors.HexColor("#525268")
GREEN   = colors.HexColor("#3DB87A")
GREEN_DIM=colors.HexColor("#0D1F15")
RED     = colors.HexColor("#C85454")
RED_DIM = colors.HexColor("#1F0D0D")
YELLOW  = colors.HexColor("#C89A3D")
BLUE    = colors.HexColor("#5B7FD4")
BORDER  = colors.HexColor("#1E1E2E")
BORDER2 = colors.HexColor("#252535")

PAGE_W  = A4[0]
PAGE_H  = A4[1]
L_MAR   = 16 * mm
R_MAR   = 16 * mm
T_MAR   = 18 * mm
B_MAR   = 18 * mm
USABLE  = PAGE_W - L_MAR - R_MAR  # ~163mm


def _ps(name, **kw):
    base = dict(fontName="Helvetica", textColor=TEXT, fontSize=9,
                leading=15, spaceAfter=0, spaceBefore=0, leftIndent=0, rightIndent=0)
    base.update(kw)
    return ParagraphStyle(name, **base)


def _hr(col=BORDER, w=0.4, before=4, after=6):
    return HRFlowable(width="100%", thickness=w, color=col,
                      spaceBefore=before, spaceAfter=after)


def _tbl(rows, widths, header_bg=BG2, stripe_a=BG, stripe_b=BG2,
         font_size=8, hdr_font_size=7.5, v_pad=5, h_pad=7):
    """Build a styled table that auto-wraps cell content."""
    t = Table(rows, colWidths=widths, repeatRows=1,
              style=TableStyle([
                  # Header row
                  ("BACKGROUND",   (0, 0), (-1, 0),  header_bg),
                  ("TEXTCOLOR",    (0, 0), (-1, 0),  GOLD),
                  ("FONTNAME",     (0, 0), (-1, 0),  "Helvetica-Bold"),
                  ("FONTSIZE",     (0, 0), (-1, 0),  hdr_font_size),
                  ("BOTTOMPADDING",(0, 0), (-1, 0),  6),
                  ("TOPPADDING",   (0, 0), (-1, 0),  6),
                  # Data rows
                  ("ROWBACKGROUNDS",(0, 1), (-1, -1), [stripe_a, stripe_b]),
                  ("FONTNAME",     (0, 1), (-1, -1), "Helvetica"),
                  ("FONTSIZE",     (0, 1), (-1, -1), font_size),
                  ("TEXTCOLOR",    (0, 1), (-1, -1), TEXT),
                  ("TOPPADDING",   (0, 1), (-1, -1), v_pad),
                  ("BOTTOMPADDING",(0, 1), (-1, -1), v_pad),
                  ("LEFTPADDING",  (0, 0), (-1, -1), h_pad),
                  ("RIGHTPADDING", (0, 0), (-1, -1), h_pad),
                  ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
                  ("LINEBELOW",    (0, 0), (-1, 0),  0.5, GOLD),
                  ("LINEBELOW",    (0, 1), (-1, -1), 0.3, BORDER),
                  ("BOX",          (0, 0), (-1, -1), 0.5, BORDER),
              ]))
    return t


def _section_header(story, num, title):
    """Render a clean numbered section header identical to the weekly brief."""
    story.append(Spacer(1, 14))
    # Number pill + title on same baseline
    story.append(Paragraph(
        f'<font color="#B8965A" size="8" fontName="Helvetica-Bold">'
        f'{"0" if num < 10 else ""}{num}</font>'
        f'<font color="#2A2010">  ·  </font>'
        f'<font color="#D4AF7A" size="15" fontName="Helvetica-Bold">{title}</font>',
        _ps(f"sh{num}", fontSize=15, textColor=GOLD_LT,
            fontName="Helvetica-Bold", spaceBefore=0, spaceAfter=6, leading=20)))
    story.append(_hr(GOLD, w=0.7, before=0, after=10))


def _region_card(story, label, direction, headline, body, key_driver, risk):
    """Bordered card for US / Europe / Asia regional analysis."""
    dir_hex = "3DB87A" if direction == "bullish" else "C85454" if direction == "bearish" else "C89A3D"
    dir_label = direction.upper() if direction else "NEUTRAL"

    inner = []
    inner.append(Paragraph(
        f'<font color="#{dir_hex}" size="8" fontName="Helvetica-Bold">'
        f'{label.upper()}  ·  {dir_label}</font>',
        _ps("rl", fontSize=8, fontName="Helvetica-Bold",
            textColor=colors.HexColor(f"#{dir_hex}"), spaceAfter=4)))
    if headline:
        inner.append(Paragraph(headline,
            _ps("rh", fontSize=9.5, fontName="Helvetica-Bold",
                textColor=LIGHT, leading=13, spaceAfter=5)))
    if body:
        inner.append(Paragraph(str(body),
            _ps("rb", fontSize=8.5, textColor=TEXT2, leading=13, spaceAfter=4)))
    if key_driver:
        inner.append(Paragraph(
            f'<font color="#B8965A">▶</font>  {key_driver}',
            _ps("rd", fontSize=8, textColor=TEXT2, leading=12, spaceAfter=2)))
    if risk:
        inner.append(Paragraph(
            f'<font color="#C85454">⚠</font>  {risk}',
            _ps("rr", fontSize=8, textColor=RED, leading=12, spaceAfter=0)))

    # Wrap in a 1-cell table to get the border + background
    card = Table([[inner]], colWidths=[USABLE],
                 style=TableStyle([
                     ("BACKGROUND",    (0, 0), (-1, -1), BG2),
                     ("BOX",           (0, 0), (-1, -1), 0.5, BORDER2),
                     ("LINEBEfore",    (0, 0), (0, -1),  3,   colors.HexColor(f"#{dir_hex}")),
                     ("TOPPADDING",    (0, 0), (-1, -1), 10),
                     ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                     ("LEFTPADDING",   (0, 0), (-1, -1), 12),
                     ("RIGHTPADDING",  (0, 0), (-1, -1), 12),
                     ("VALIGN",        (0, 0), (-1, -1), "TOP"),
                 ]))
    story.append(KeepTogether([card, Spacer(1, 6)]))


def _strategy_card(story, stype, recommendation, rationale, timeframe, conviction):
    """Full-width strategy card with left gold border."""
    conv_hex = "3DB87A" if conviction == "high" else "C89A3D" if conviction == "medium" else "525268"

    inner = []
    inner.append(Paragraph(
        f'<font color="#{conv_hex}" size="7.5" fontName="Helvetica-Bold">'
        f'{stype.upper()}  ·  {conviction.upper() if conviction else ""} CONVICTION'
        f'{"  ·  " + timeframe if timeframe else ""}</font>',
        _ps("st", fontSize=7.5, fontName="Helvetica-Bold",
            textColor=colors.HexColor(f"#{conv_hex}"), spaceAfter=4)))
    if recommendation:
        inner.append(Paragraph(str(recommendation),
            _ps("sr", fontSize=9.5, fontName="Helvetica-Bold",
                textColor=LIGHT, leading=13, spaceAfter=5)))
    if rationale:
        inner.append(Paragraph(str(rationale),
            _ps("sb", fontSize=8.5, textColor=TEXT2, leading=13, spaceAfter=0)))

    card = Table([[inner]], colWidths=[USABLE],
                 style=TableStyle([
                     ("BACKGROUND",    (0, 0), (-1, -1), BG2),
                     ("BOX",           (0, 0), (-1, -1), 0.5, BORDER2),
                     ("LINEBEBRE",     (0, 0), (0, -1),  3,   GREEN),
                     ("TOPPADDING",    (0, 0), (-1, -1), 10),
                     ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                     ("LEFTPADDING",   (0, 0), (-1, -1), 14),
                     ("RIGHTPADDING",  (0, 0), (-1, -1), 12),
                     ("VALIGN",        (0, 0), (-1, -1), "TOP"),
                 ]))
    story.append(KeepTogether([card, Spacer(1, 5)]))


def _page_canvas(canvas, doc):
    """Draws the dark background, gold top bar and footer on every page."""
    canvas.saveState()
    w, h = PAGE_W, PAGE_H

    # Dark background
    canvas.setFillColor(BG)
    canvas.rect(0, 0, w, h, fill=1, stroke=0)

    # Gold top bar
    canvas.setFillColor(GOLD)
    canvas.rect(0, h - 3, w, 3, fill=1, stroke=0)

    # Footer line
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.4)
    canvas.line(L_MAR, B_MAR - 4, w - R_MAR, B_MAR - 4)

    # Footer text
    canvas.setFillColor(MUTED)
    canvas.setFont("Helvetica", 6.5)
    footer = (f"Daily Market Brief  ·  {DATE_FR}  ·  "
              f"yfinance · FRED · Finnhub · Gmail  ·  Claude AI  ·  Not investment advice")
    canvas.drawString(L_MAR, B_MAR - 10, footer)

    # Page number
    canvas.drawRightString(w - R_MAR, B_MAR - 10, f"{doc.page}")

    canvas.restoreState()


def generate_pdf(report: dict) -> bytes:
    print("[4/6] Génération du PDF...")
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        leftMargin=L_MAR, rightMargin=R_MAR,
        topMargin=T_MAR, bottomMargin=B_MAR + 6,
    )

    story = []

    # ── COVER HEADER ──────────────────────────────────────────────────────────
    temp      = report.get("market_temperature", "neutral")
    temp_lbl  = {"risk_on": "RISK ON ▲", "risk_off": "RISK OFF ▼", "neutral": "NEUTRAL ◆"}.get(temp, temp.upper())
    temp_hex  = "3DB87A" if temp == "risk_on" else "C85454" if temp == "risk_off" else "C89A3D"

    story.append(Paragraph(
        'DAILY MARKET BRIEF  ·  INTELLIGENCE NOTE',
        _ps("kicker", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD,
            spaceBefore=0, spaceAfter=6, leading=10)))

    story.append(Paragraph(
        f'Daily <font color="#B8965A"><i>Market</i></font> Brief',
        _ps("maintitle", fontSize=28, fontName="Helvetica-Bold", textColor=LIGHT,
            leading=32, spaceAfter=4)))

    story.append(Paragraph(
        f'{DATE_FR}',
        _ps("dateline", fontSize=9, textColor=MUTED, spaceAfter=4, leading=12)))

    story.append(Paragraph(
        f'<font color="#{temp_hex}" fontName="Helvetica-Bold">{temp_lbl}</font>'
        f'  <font color="#525268">·</font>  '
        f'<font color="#9898B0">{report.get("market_temperature_label", "")}</font>',
        _ps("regime", fontSize=9, textColor=TEXT2, leading=12, spaceAfter=6)))

    story.append(_hr(GOLD, w=1.2, before=2, after=14))

    # ── SECTION 1 — Overview ──────────────────────────────────────────────────
    s1 = report.get("section1_overview", {})
    _section_header(story, 1, s1.get("title", "Global Market Overview & Sentiment"))

    if s1.get("headline"):
        story.append(Paragraph(str(s1["headline"]),
            _ps("headline", fontSize=11, fontName="Helvetica-Bold",
                textColor=LIGHT, leading=15, spaceAfter=8)))

    for para in s1.get("paragraphs", []):
        story.append(Paragraph(str(para),
            _ps("body", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=8)))

    # ── SECTION 2 — Macro ─────────────────────────────────────────────────────
    s2 = report.get("section2_macro", {})
    _section_header(story, 2, s2.get("title", "Macro-Economic Environment"))

    for para in s2.get("paragraphs", []):
        story.append(Paragraph(str(para),
            _ps("macrobody", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=8)))

    # ── SECTION 3 — Equities ──────────────────────────────────────────────────
    story.append(PageBreak())
    s3 = report.get("section3_equities", {})
    _section_header(story, 3, s3.get("title", "Equity Markets"))

    # Indices table
    indices = s3.get("indices", [])
    if indices:
        hdr = [
            Paragraph("Index",  _ps("th", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Close",  _ps("th", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("1D Chg", _ps("th", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("YTD",    _ps("th", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
        ]
        rows = [hdr]
        for idx in indices:
            chg = idx.get("change", "—")
            ytd = idx.get("ytd", "—")
            chg_hex = "3DB87A" if str(chg).startswith("+") else "C85454" if str(chg).startswith("-") else "525268"
            ytd_hex = "3DB87A" if str(ytd).startswith("+") else "C85454" if str(ytd).startswith("-") else "525268"
            rows.append([
                Paragraph(idx.get("name", ""), _ps("idxn", fontSize=8.5, textColor=LIGHT, fontName="Helvetica-Bold")),
                Paragraph(str(idx.get("value", "—")), _ps("idxv", fontSize=8.5, textColor=TEXT)),
                Paragraph(str(chg), _ps("idxc", fontSize=8.5, textColor=colors.HexColor(f"#{chg_hex}"), fontName="Helvetica-Bold")),
                Paragraph(str(ytd), _ps("idxy", fontSize=8.5, textColor=colors.HexColor(f"#{ytd_hex}"))),
            ])
        story.append(_tbl(rows, [72*mm, 38*mm, 28*mm, 25*mm], v_pad=6))
        story.append(Spacer(1, 14))

    # Sector performance table
    sectors = s3.get("sector_performance", [])
    if sectors:
        story.append(Paragraph("US Sector Performance",
            _ps("subtl", fontSize=9, fontName="Helvetica-Bold", textColor=GOLD,
                spaceAfter=6, spaceBefore=0)))
        # Two columns side by side
        half = (len(sectors) + 1) // 2
        left  = sectors[:half]
        right = sectors[half:]
        sec_hdr = [
            Paragraph("Sector", _ps("sh", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Daily",  _ps("sh", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Sector", _ps("sh", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Daily",  _ps("sh", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
        ]
        sec_rows = [sec_hdr]
        for i in range(half):
            row = []
            for item in [left[i] if i < len(left) else None,
                         right[i] if i < len(right) else None]:
                if item:
                    chg = item.get("change", "—")
                    c_hex = "3DB87A" if item.get("direction") == "up" else "C85454" if item.get("direction") == "down" else "525268"
                    row += [
                        Paragraph(item.get("sector", ""), _ps("sn", fontSize=8.5, textColor=TEXT)),
                        Paragraph(str(chg), _ps("sc", fontSize=8.5,
                            textColor=colors.HexColor(f"#{c_hex}"), fontName="Helvetica-Bold")),
                    ]
                else:
                    row += [Paragraph("", _ps("e")), Paragraph("", _ps("e"))]
            sec_rows.append(row)
        story.append(_tbl(sec_rows, [52*mm, 28*mm, 52*mm, 28*mm], v_pad=5))
        story.append(Spacer(1, 14))

    # Regional cards
    for key, label in [("us", "United States"), ("europe", "Europe"), ("asia", "Asia")]:
        reg = s3.get(key, {})
        if reg:
            _region_card(story,
                label=label,
                direction=reg.get("direction", "neutral"),
                headline=reg.get("headline", ""),
                body=reg.get("body", ""),
                key_driver=reg.get("key_driver", ""),
                risk=reg.get("risk", ""),
            )

    # ── SECTION 4 — Fixed Income ──────────────────────────────────────────────
    story.append(PageBreak())
    s4 = report.get("section4_fixed_income", {})
    _section_header(story, 4, s4.get("title", "Fixed Income & Rates"))

    yc = s4.get("yield_curve", {})
    if yc:
        # Yield curve table
        yc_hdr = [
            Paragraph("Tenor",         _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Yield",         _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("2s10s Spread",  _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
        ]
        spread_val = yc.get("spread_2_10", "—")
        spread_hex = "3DB87A" if str(spread_val).lstrip("+").startswith("+") or (not str(spread_val).startswith("-")) else "C85454"
        yc_rows = [
            yc_hdr,
            [Paragraph("US 2Y",  _ps("t", fontSize=8.5, textColor=TEXT)),
             Paragraph(f"{yc.get('us_2y','—')}%", _ps("t", fontSize=8.5, textColor=GOLD_LT, fontName="Helvetica-Bold")),
             Paragraph(str(spread_val), _ps("t", fontSize=8.5, textColor=colors.HexColor(f"#{spread_hex}"), fontName="Helvetica-Bold"))],
            [Paragraph("US 10Y", _ps("t", fontSize=8.5, textColor=TEXT)),
             Paragraph(f"{yc.get('us_10y','—')}%", _ps("t", fontSize=8.5, textColor=GOLD_LT, fontName="Helvetica-Bold")),
             Paragraph("", _ps("e"))],
            [Paragraph("US 30Y", _ps("t", fontSize=8.5, textColor=TEXT)),
             Paragraph(f"{yc.get('us_30y','—')}%", _ps("t", fontSize=8.5, textColor=GOLD_LT, fontName="Helvetica-Bold")),
             Paragraph("", _ps("e"))],
        ]
        story.append(_tbl(yc_rows, [45*mm, 40*mm, 78*mm], v_pad=6))
        story.append(Spacer(1, 10))

        if yc.get("interpretation"):
            story.append(Paragraph(str(yc["interpretation"]),
                _ps("yci", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=8)))

    story.append(Paragraph(str(s4.get("narrative", "")),
        _ps("fibody", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=8)))

    # ── SECTION 5 — Forex ─────────────────────────────────────────────────────
    s5 = report.get("section5_forex", {})
    _section_header(story, 5, s5.get("title", "Foreign Exchange"))

    dxy = s5.get("dxy", {})
    if dxy:
        dxy_chg = str(dxy.get("change", ""))
        dxy_hex = "3DB87A" if dxy_chg.startswith("+") else "C85454"
        story.append(Paragraph(
            f'<font fontName="Helvetica-Bold" color="#D4AF7A">DXY</font>  '
            f'<font fontName="Helvetica-Bold" color="#E8E8F4">{dxy.get("value","—")}</font>  '
            f'<font fontName="Helvetica-Bold" color="#{dxy_hex}">{dxy_chg}</font>'
            f'<font color="#525268">  ·  </font>'
            f'<font color="#9898B0">{str(dxy.get("interpretation",""))[:220]}</font>',
            _ps("dxy", fontSize=8.5, textColor=TEXT2, leading=13, spaceAfter=8)))

    story.append(Paragraph(str(s5.get("narrative", "")),
        _ps("fxnarr", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=10)))

    pairs = s5.get("pairs", [])
    if pairs:
        fx_hdr = [
            Paragraph("Pair",       _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Rate",       _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Daily",      _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Support",    _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Resistance", _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Analysis",   _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
        ]
        fx_rows = [fx_hdr]
        for p in pairs:
            chg = str(p.get("change", "—"))
            c_hex = "3DB87A" if chg.startswith("+") else "C85454" if chg.startswith("-") else "525268"
            analysis_text = str(p.get("analysis", ""))
            # Keep analysis under ~300 chars so the cell doesn't explode
            if len(analysis_text) > 300:
                analysis_text = analysis_text[:297] + "…"
            fx_rows.append([
                Paragraph(p.get("pair","—"), _ps("fp", fontSize=8.5, fontName="Helvetica-Bold", textColor=LIGHT)),
                Paragraph(str(p.get("value","—")), _ps("fv", fontSize=8.5, textColor=TEXT)),
                Paragraph(chg, _ps("fc", fontSize=8.5, fontName="Helvetica-Bold",
                    textColor=colors.HexColor(f"#{c_hex}"))),
                Paragraph(str(p.get("support","—")), _ps("fs", fontSize=8, textColor=GREEN)),
                Paragraph(str(p.get("resistance","—")), _ps("fr", fontSize=8, textColor=RED)),
                Paragraph(analysis_text, _ps("fa", fontSize=7.5, textColor=TEXT2, leading=11)),
            ])
        story.append(_tbl(fx_rows, [18*mm, 18*mm, 14*mm, 14*mm, 14*mm, 85*mm], v_pad=6))

    # ── SECTION 6 — Commodities ───────────────────────────────────────────────
    story.append(PageBreak())
    s6 = report.get("section6_commodities", {})
    _section_header(story, 6, s6.get("title", "Commodities & Alternative Assets"))

    story.append(Paragraph(str(s6.get("narrative", "")),
        _ps("commbody", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=10)))

    items = s6.get("items", [])
    if items:
        cmd_hdr = [
            Paragraph("Asset",   _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Price",   _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Unit",    _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Daily",   _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
            Paragraph("Context", _ps("t", fontSize=7.5, fontName="Helvetica-Bold", textColor=GOLD)),
        ]
        cmd_rows = [cmd_hdr]
        for it in items:
            chg = str(it.get("change", "—"))
            c_hex = "3DB87A" if chg.startswith("+") else "C85454" if chg.startswith("-") else "525268"
            analysis = str(it.get("analysis", ""))
            if len(analysis) > 250:
                analysis = analysis[:247] + "…"
            cmd_rows.append([
                Paragraph(str(it.get("name","—")), _ps("cn", fontSize=8.5, fontName="Helvetica-Bold", textColor=LIGHT)),
                Paragraph(str(it.get("value","—")), _ps("cv", fontSize=8.5, textColor=TEXT)),
                Paragraph(str(it.get("unit","—")), _ps("cu", fontSize=7.5, textColor=MUTED)),
                Paragraph(chg, _ps("cc", fontSize=8.5, fontName="Helvetica-Bold",
                    textColor=colors.HexColor(f"#{c_hex}"))),
                Paragraph(analysis, _ps("ca", fontSize=7.5, textColor=TEXT2, leading=11)),
            ])
        story.append(_tbl(cmd_rows, [28*mm, 22*mm, 16*mm, 18*mm, 79*mm], v_pad=6))

    # ── SECTION 7 — Positioning ───────────────────────────────────────────────
    _section_header(story, 7, report.get("section7_positioning", {}).get("title", "Market Positioning & Sentiment"))
    s7 = report.get("section7_positioning", {})
    story.append(Paragraph(str(s7.get("narrative", "")),
        _ps("posbody", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=8)))

    # ── SECTION 8 — Email Intelligence ────────────────────────────────────────
    story.append(PageBreak())
    s8 = report.get("section8_email_intelligence", {})
    _section_header(story, 8, s8.get("title", "Email Intelligence — Daily Market Watch"))

    story.append(Paragraph(str(s8.get("summary", "")),
        _ps("emailsum", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=10)))

    for item in s8.get("key_stories", []):
        rel     = item.get("relevance", "medium")
        rel_hex = "3DB87A" if rel == "high" else "C89A3D"
        inner   = [
            Paragraph(
                f'<font color="#{rel_hex}" fontName="Helvetica-Bold" size="8">'
                f'{"▲ HIGH" if rel=="high" else "● MEDIUM"}</font>  '
                f'<font color="#E8E8F4" fontName="Helvetica-Bold" size="9">{item.get("title","")}</font>',
                _ps("et", fontSize=9, textColor=LIGHT, leading=13, spaceAfter=5)),
            Paragraph(str(item.get("body","")),
                _ps("eb", fontSize=8.5, textColor=TEXT2, leading=13, spaceAfter=0)),
        ]
        card = Table([[inner]], colWidths=[USABLE],
            style=TableStyle([
                ("BACKGROUND",    (0,0),(-1,-1), BG2),
                ("BOX",           (0,0),(-1,-1), 0.5, BORDER2),
                ("LINEBEBRE",     (0,0),(0,-1),  3, colors.HexColor(f"#{rel_hex}")),
                ("TOPPADDING",    (0,0),(-1,-1), 10),
                ("BOTTOMPADDING", (0,0),(-1,-1), 10),
                ("LEFTPADDING",   (0,0),(-1,-1), 12),
                ("RIGHTPADDING",  (0,0),(-1,-1), 12),
                ("VALIGN",        (0,0),(-1,-1), "TOP"),
            ]))
        story.append(KeepTogether([card, Spacer(1, 6)]))

    # ── SECTION 9 — Synthesis ─────────────────────────────────────────────────
    story.append(PageBreak())
    s9 = report.get("section9_synthesis", {})
    _section_header(story, 9, s9.get("title", "Daily Synthesis & Outlook"))

    regime     = s9.get("regime", "neutral")
    regime_lbl = {"risk_on":"RISK ON","risk_off":"RISK OFF","transition":"TRANSITION",
                  "stagflation":"STAGFLATION","goldilocks":"GOLDILOCKS"}.get(regime, regime.upper())
    regime_hex = "3DB87A" if regime in ("risk_on","goldilocks") else "C85454" if regime in ("risk_off","stagflation") else "C89A3D"

    story.append(Paragraph(
        f'<font color="#{regime_hex}" fontName="Helvetica-Bold">{regime_lbl}</font>',
        _ps("reglbl", fontSize=10, fontName="Helvetica-Bold",
            textColor=colors.HexColor(f"#{regime_hex}"), spaceAfter=8)))

    story.append(Paragraph(str(s9.get("global_view", "")),
        _ps("gview", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=12)))

    if s9.get("tomorrow_watch"):
        story.append(Paragraph("Tomorrow — Key Events & Data to Watch",
            _ps("twlbl", fontSize=9, fontName="Helvetica-Bold", textColor=GOLD,
                spaceAfter=6, spaceBefore=4)))
        story.append(_hr(BORDER, w=0.4, before=0, after=6))
        story.append(Paragraph(str(s9["tomorrow_watch"]),
            _ps("tw", fontSize=8.5, textColor=TEXT2, leading=14, spaceAfter=12)))

    story.append(Paragraph("Recommended Strategies",
        _ps("stlbl", fontSize=9, fontName="Helvetica-Bold", textColor=GOLD,
            spaceAfter=6, spaceBefore=4)))
    story.append(_hr(BORDER, w=0.4, before=0, after=8))

    for st in s9.get("strategy", []):
        _strategy_card(story,
            stype=st.get("type", ""),
            recommendation=st.get("recommendation", ""),
            rationale=st.get("rationale", ""),
            timeframe=st.get("timeframe", ""),
            conviction=st.get("conviction", "medium"),
        )

    doc.build(story, onFirstPage=_page_canvas, onLaterPages=_page_canvas)
    pdf = buffer.getvalue()
    print(f"[4/6] ✅ PDF généré ({len(pdf) // 1024} KB)")
    return pdf


# ══════════════════════════════════════════════════════════════════════════════
#  EMAIL HTML
# ══════════════════════════════════════════════════════════════════════════════
def build_email_html(report: dict) -> str:
    temp = report.get("market_temperature", "neutral")
    temp_c = "#4ade80" if temp == "risk_on" else "#f87171" if temp == "risk_off" else "#fbbf24"
    temp_label = {"risk_on": "RISK ON ▲", "risk_off": "RISK OFF ▼", "neutral": "NEUTRAL ◆"}.get(temp, temp.upper())

    s1 = report.get("section1_overview", {})
    s3 = report.get("section3_equities", {})
    s9 = report.get("section9_synthesis", {})

    # Indices rows
    rows = "".join([
        f'<tr><td style="padding:5px 10px;border-bottom:1px solid #1e1e2e;font-family:monospace;font-size:11px;color:#c8c8d8">{i.get("name", "")}</td>'
        f'<td style="padding:5px 10px;border-bottom:1px solid #1e1e2e;font-family:monospace;color:#c8c8d8">{i.get("value", "")}</td>'
        f'<td style="padding:5px 10px;border-bottom:1px solid #1e1e2e;font-family:monospace;color:{"#4ade80" if str(i.get("change","")).startswith("+") else "#f87171"}">{i.get("change", "")}</td>'
        f'<td style="padding:5px 10px;border-bottom:1px solid #1e1e2e;font-family:monospace;font-size:10px;color:#525268">{i.get("ytd", "")}</td></tr>'
        for i in s3.get("indices", [])[:8]
    ])

    # Strategy cards
    strats = "".join([
        f'<div style="border-left:2px solid #3db87a;padding:8px 12px;margin:6px 0;background:#0d0d16">'
        f'<div style="color:#{"3db87a" if s.get("conviction")=="high" else "c89a3d" if s.get("conviction")=="medium" else "525268"};font-family:monospace;font-size:9px;text-transform:uppercase;margin-bottom:3px">{s.get("type", "")} · {s.get("conviction", "")}</div>'
        f'<div style="color:#e8e8f0;font-size:12px;font-weight:bold;margin-bottom:3px">{s.get("recommendation", "")}</div>'
        f'<div style="color:#6b6b80;font-size:11px;line-height:1.5">{s.get("rationale", "")}</div></div>'
        for s in s9.get("strategy", [])
    ])

    # Overview paragraphs
    paras = "".join([
        f'<p style="color:#8888a0;font-size:12px;line-height:1.8;margin:0 0 8px">{p}</p>'
        for p in s1.get("paragraphs", [])[:2]
    ])

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="background:#07070d;color:#c8c8d8;font-family:Arial,sans-serif;margin:0;padding:20px">
<div style="max-width:640px;margin:0 auto">
  <div style="background:#b8965a;height:3px;margin-bottom:20px"></div>
  <div style="display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:14px">
    <div>
      <div style="font-size:8px;color:#b8965a;letter-spacing:3px;text-transform:uppercase;margin-bottom:5px">DAILY INTELLIGENCE BRIEF</div>
      <div style="font-size:24px;font-family:Georgia,serif;color:#e8e8f4">Daily <span style="color:#b8965a;font-style:italic">Market</span> Brief</div>
      <div style="font-size:10px;color:#525268;margin-top:4px">{DATE_FR}</div>
    </div>
    <div style="text-align:right">
      <div style="font-size:11px;color:{temp_c};font-family:monospace;font-weight:bold">{temp_label}</div>
      <div style="font-size:9px;color:#525268;margin-top:3px">{report.get("market_temperature_label", "")}</div>
    </div>
  </div>

  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:4px;padding:14px;margin-bottom:12px">
    <div style="color:#b8965a;font-size:8px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Market Overview</div>
    <div style="color:#e8e8f0;font-size:13px;font-weight:bold;margin-bottom:8px">{s1.get("headline", "")}</div>
    {paras}
  </div>

  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:4px;padding:14px;margin-bottom:12px">
    <div style="color:#b8965a;font-size:8px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Key Indices</div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr>
        <th style="text-align:left;padding:4px 10px;color:#525268;font-size:8px;border-bottom:1px solid #1e1e2e">Index</th>
        <th style="text-align:left;padding:4px 10px;color:#525268;font-size:8px;border-bottom:1px solid #1e1e2e">Price</th>
        <th style="text-align:left;padding:4px 10px;color:#525268;font-size:8px;border-bottom:1px solid #1e1e2e">1D</th>
        <th style="text-align:left;padding:4px 10px;color:#525268;font-size:8px;border-bottom:1px solid #1e1e2e">YTD</th>
      </tr></thead><tbody>{rows}</tbody>
    </table>
  </div>

  <div style="background:#0d0d16;border:1px solid #1e1e2e;border-radius:4px;padding:14px;margin-bottom:12px">
    <div style="color:#b8965a;font-size:8px;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Daily Strategy</div>
    {strats}
  </div>

  <p style="color:#2a2a3a;font-size:9px;text-align:center;margin-top:16px">📎 Full report attached as PDF · yfinance + FRED + Gmail Intelligence + Claude AI · Not investment advice</p>
  <div style="background:#b8965a;height:2px;margin-top:14px"></div>
</div></body></html>"""


# ══════════════════════════════════════════════════════════════════════════════
#  ENVOI EMAIL
# ══════════════════════════════════════════════════════════════════════════════
def send_email(report: dict, pdf: bytes, access_token: str):
    print(f"[5/6] Envoi email à {EMAIL_TO}...")
    msg = MIMEMultipart("mixed")
    msg["From"] = f"MarketBrief Daily <{EMAIL_FROM}>"
    msg["To"] = EMAIL_TO
    msg["Subject"] = f"Daily Market Brief — {DATE_FR}"

    msg.attach(MIMEText(build_email_html(report), "html", "utf-8"))

    pdf_part = MIMEBase("application", "pdf")
    pdf_part.set_payload(pdf)
    encoders.encode_base64(pdf_part)
    pdf_part.add_header("Content-Disposition", "attachment",
                        filename=f"daily-marketbrief-{TODAY}.pdf")
    msg.attach(pdf_part)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    resp = requests.post(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json={"raw": raw}
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Gmail API {resp.status_code}: {resp.text}")
    print(f"[5/6] ✅ Email envoyé à {EMAIL_TO}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"\n{'='*60}\n  DAILY MARKET BRIEF — {DATE_FR}\n{'='*60}\n")

    token       = get_fresh_access_token()
    emails      = fetch_gmail_emails(token)
    market_data = collect_all_data()
    report      = generate_report_json(market_data, emails)
    save_json(report, market_data)
    pdf         = generate_pdf(report)
    send_email(report, pdf, token)

    print(f"\n[6/6] ✅ Daily Brief envoyé !\n{'='*60}\n")
