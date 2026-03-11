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

    # List message IDs
    params = {"q": query, "maxResults": 30}
    r = requests.get(f"{base}/messages", headers=headers, params=params)
    if r.status_code != 200:
        print(f"  ⚠️ Gmail list failed: {r.text}")
        return []

    message_ids = [m["id"] for m in r.json().get("messages", [])]
    print(f"  → {len(message_ids)} email(s) trouvé(s) dans les dernières 24h")

    emails = []
    for mid in message_ids[:15]:  # Max 15 emails
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

            # Extract body text
            body = _extract_body(msg.get("payload", {}))
            if body:
                emails.append({
                    "subject": subject,
                    "from": sender,
                    "date": date,
                    "body": body[:3000]  # Truncate to avoid token explosion
                })
                print(f"  ✉️  {subject[:60]} — {sender[:40]}")
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
    """Fetch real-time market data using yfinance (free, no API key)."""
    print("  [yfinance] Fetching market data...")
    data = {}

    # ── Major Indices
    indices = {
        "S&P 500": "^GSPC", "NASDAQ 100": "^NDX", "Dow Jones": "^DJI",
        "Russell 2000": "^RUT", "VIX": "^VIX",
        "CAC 40": "^FCHI", "DAX": "^GDAXI", "FTSE 100": "^FTSE",
        "EuroStoxx 50": "^STOXX50E", "Nikkei 225": "^N225",
        "Hang Seng": "^HSI", "Shanghai": "000001.SS"
    }
    idx_data = {}
    for name, ticker in indices.items():
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price  = round(info.last_price, 2) if info.last_price else None
            prev   = round(info.previous_close, 2) if info.previous_close else None
            change = round(((price - prev) / prev) * 100, 2) if price and prev else None
            if price:
                idx_data[name] = {"price": price, "prev_close": prev, "change_pct": change}
        except Exception as e:
            print(f"    ⚠️ {name}: {e}")
    data["indices"] = idx_data

    # ── FX Rates
    fx_pairs = {
        "EUR/USD": "EURUSD=X", "GBP/USD": "GBPUSD=X", "USD/JPY": "JPY=X",
        "USD/CHF": "CHF=X", "AUD/USD": "AUDUSD=X", "USD/CNY": "CNY=X",
        "USD/BRL": "BRL=X", "USD/MXN": "MXN=X", "USD/CAD": "CAD=X",
        "DXY": "DX-Y.NYB"
    }
    fx_data = {}
    for pair, ticker in fx_pairs.items():
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price  = round(info.last_price, 4) if info.last_price else None
            prev   = round(info.previous_close, 4) if info.previous_close else None
            change = round(((price - prev) / prev) * 100, 4) if price and prev else None
            if price:
                fx_data[pair] = {"price": price, "change_pct": change}
        except Exception as e:
            print(f"    ⚠️ FX {pair}: {e}")
    data["fx"] = fx_data

    # ── Commodities (via ETFs/futures)
    commodities = {
        "Gold": "GC=F", "Silver": "SI=F", "WTI Crude": "CL=F",
        "Brent Crude": "BZ=F", "Natural Gas": "NG=F", "Copper": "HG=F",
        "Wheat": "ZW=F", "Corn": "ZC=F", "Soybeans": "ZS=F"
    }
    cmd_data = {}
    for name, ticker in commodities.items():
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price  = round(info.last_price, 2) if info.last_price else None
            prev   = round(info.previous_close, 2) if info.previous_close else None
            change = round(((price - prev) / prev) * 100, 2) if price and prev else None
            if price:
                cmd_data[name] = {"price": price, "change_pct": change}
        except Exception as e:
            print(f"    ⚠️ Commodity {name}: {e}")
    data["commodities"] = cmd_data

    # ── US Treasuries
    bonds = {
        "US 2Y": "^IRX", "US 10Y": "^TNX", "US 30Y": "^TYX",
        "GER 10Y Bund": "^TNX"  # fallback — FRED will give better data
    }
    bond_data = {}
    for name, ticker in bonds.items():
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price  = round(info.last_price, 3) if info.last_price else None
            prev   = round(info.previous_close, 3) if info.previous_close else None
            change = round(price - prev, 3) if price and prev else None
            if price:
                bond_data[name] = {"yield": price, "change_bp": round(change * 100, 1) if change else None}
        except Exception:
            pass
    data["bonds"] = bond_data

    # ── Key ETFs for sector/sentiment
    etfs = {
        "SPY": "S&P 500 ETF", "QQQ": "NASDAQ ETF", "IWM": "Russell 2000 ETF",
        "GLD": "Gold ETF", "TLT": "Long Bond ETF", "HYG": "High Yield ETF",
        "LQD": "Investment Grade ETF", "EEM": "Emerging Markets ETF",
        "XLF": "Financials", "XLE": "Energy", "XLK": "Technology",
        "XLV": "Healthcare", "XLI": "Industrials", "XLY": "Consumer Discr.",
        "ARKK": "Innovation ETF"
    }
    etf_data = {}
    for ticker, name in etfs.items():
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price  = round(info.last_price, 2) if info.last_price else None
            prev   = round(info.previous_close, 2) if info.previous_close else None
            change = round(((price - prev) / prev) * 100, 2) if price and prev else None
            if price:
                etf_data[ticker] = {"name": name, "price": price, "change_pct": change}
        except Exception:
            pass
    data["etfs"] = etf_data

    print(f"  [yfinance] ✅ {len(idx_data)} indices, {len(fx_data)} FX pairs, {len(cmd_data)} commodities")
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
def format_market_context(data: dict) -> str:
    mkt = data.get("market", {})
    fred = data.get("fred", {})

    lines = ["=== REAL-TIME MARKET DATA (yfinance) ===\n"]

    # Indices
    lines.append("--- Major Indices ---")
    for name, v in mkt.get("indices", {}).items():
        chg = f"{v['change_pct']:+.2f}%" if v.get("change_pct") is not None else "N/A"
        lines.append(f"  {name}: {v['price']} ({chg})")

    # FX
    lines.append("\n--- FX Rates ---")
    for pair, v in mkt.get("fx", {}).items():
        chg = f"{v['change_pct']:+.4f}%" if v.get("change_pct") is not None else "N/A"
        lines.append(f"  {pair}: {v['price']} ({chg})")

    # Commodities
    lines.append("\n--- Commodities ---")
    for name, v in mkt.get("commodities", {}).items():
        chg = f"{v['change_pct']:+.2f}%" if v.get("change_pct") is not None else "N/A"
        lines.append(f"  {name}: {v['price']} ({chg})")

    # Bonds
    lines.append("\n--- US Yields ---")
    for name, v in mkt.get("bonds", {}).items():
        bp = f"{v['change_bp']:+.1f}bp" if v.get("change_bp") is not None else "N/A"
        lines.append(f"  {name}: {v['yield']}% ({bp})")

    # Sector ETFs
    lines.append("\n--- Sector ETFs (daily change) ---")
    for ticker, v in mkt.get("etfs", {}).items():
        chg = f"{v['change_pct']:+.2f}%" if v.get("change_pct") is not None else "N/A"
        lines.append(f"  {ticker} ({v['name']}): ${v['price']} {chg}")

    # FRED
    lines.append("\n=== FRED MACRO DATA ===")
    for name, v in fred.items():
        prev_str = f" | prev: {v['prev']}" if v.get("prev") else ""
        lines.append(f"  {name}: {v['value']} ({v['date']}){prev_str}")

    # Economic Calendar
    cal = data.get("economic_calendar", [])
    if cal:
        lines.append("\n=== UPCOMING ECONOMIC EVENTS (high impact) ===")
        for e in cal[:8]:
            lines.append(f"  [{e.get('country','?')}] {e.get('event','')} — {e.get('time','')} | prev: {e.get('prev','')} | est: {e.get('estimate','?')}")

    return "\n".join(lines)


def format_email_context(emails: list[dict]) -> str:
    if not emails:
        return "=== EMAIL INTELLIGENCE ===\nNo emails received in the last 24h under label 'Daily Market Watch'.\n"

    lines = [f"=== EMAIL INTELLIGENCE ({len(emails)} emails from last 24h, label: Daily Market Watch) ===\n"]
    for i, email in enumerate(emails, 1):
        lines.append(f"--- Email {i}: {email['subject']} ---")
        lines.append(f"From: {email['from']} | Date: {email['date']}")
        lines.append(email['body'][:2500])
        lines.append("")
    return "\n".join(lines)


def generate_report_json(market_data: dict, emails: list[dict]) -> dict:
    print(f"[3/6] Calling Claude ({ANTHROPIC_MODEL})...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    market_context = format_market_context(market_data)
    email_context  = format_email_context(emails)

    tomorrow_str = (NOW + timedelta(days=1)).strftime("%B %d, %Y")

    system = (
        f"You are a senior financial analyst (ex-Goldman Sachs, ex-Bridgewater). "
        f"You produce a daily global markets intelligence brief — dense, analytical, "
        f"explanatory, and actionable. Written for sophisticated finance professionals. "
        f"Date: {DATE_FR}. "
        f"You have access to REAL market data and email intelligence below. "
        f"Synthesize them. Never regurgitate. Explain causality and second-order effects. "
        f"Write in structured paragraphs. Prose over bullet points. "
        f"ALL text in JSON must be in ENGLISH. "
        f"Reply ONLY with valid JSON, no markdown fences, no surrounding text."
    )

    user = f"""Generate a COMPLETE daily global markets intelligence brief for {DATE_FR}.

{market_context}

{email_context}

MASTER PROMPT INSTRUCTIONS:
- This is a DAILY brief (~3 pages of dense analysis), lighter than the weekly report
- Synthesize email intelligence + market data — connect the dots, explain causality
- Surface second-order implications and hidden opportunities
- Be critical and selective — never regurgitate. Explain origins and mechanisms
- Go deep on individual stories — this is NOT only a macro newsletter
- Prose paragraphs, analytical tone, confident explanations
- All text in ENGLISH

Return this exact JSON structure (ALL keys mandatory):

{{
  "date": "{DATE_FR}",
  "generated_at": "{NOW.isoformat()}",
  "market_temperature": "risk_on|risk_off|neutral",
  "market_temperature_label": "One punchy sentence explaining today's market mood",

  "section1_overview": {{
    "title": "Global Market Overview & Sentiment",
    "headline": "Punchy 1-sentence market narrative for today",
    "paragraphs": [
      "§1 — What happened across markets today: dominant narrative, risk tone, key movers (6-8 lines)",
      "§2 — Why this mood exists: macro context, recent catalyst chain, institutional behavior (5-6 lines)",
      "§3 — Secondary cross-currents and what the consensus is missing (4-5 lines)"
    ]
  }},

  "section2_macro": {{
    "title": "Macro-Economic Environment",
    "paragraphs": [
      "§1 — Current macro forces: inflation trend (use FRED CPI), labor market (FRED unemployment), liquidity conditions (5-6 lines)",
      "§2 — How these transmit into asset prices today specifically (4-5 lines)",
      "§3 — What could break the current equilibrium — explicit scenario (3-4 lines)"
    ]
  }},

  "section3_equities": {{
    "title": "Equity Markets",
    "indices": [
      {{"name": "S&P 500", "value": "use real data", "change": "+/-X.X%", "ytd": "~X.X%"}},
      {{"name": "NASDAQ 100", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "Dow Jones", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "Russell 2000", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "VIX", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "CAC 40", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "DAX", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "EuroStoxx 50", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "Nikkei 225", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "Hang Seng", "value": "...", "change": "...", "ytd": "..."}},
      {{"name": "Shanghai", "value": "...", "change": "...", "ytd": "..."}}
    ],
    "sector_performance": [
      {{"sector": "Technology", "change": "+/-X.X%", "direction": "up|down", "change_num": 0.0}},
      {{"sector": "Financials", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Energy", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Healthcare", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Industrials", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Consumer Discr.", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Materials", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Utilities", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Real Estate", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Comm. Services", "change": "...", "direction": "...", "change_num": 0.0}},
      {{"sector": "Cons. Staples", "change": "...", "direction": "...", "change_num": 0.0}}
    ],
    "us": {{
      "headline": "...",
      "body": "US equity market deep dive: macro drivers, sector rotation, institutional flows, key earnings/catalysts today. Use real index data. (8-10 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "Main driver in one sentence",
      "risk": "Main risk in one sentence"
    }},
    "europe": {{
      "headline": "...",
      "body": "European equity analysis: ECB posture, EUR dynamics, sector-level leaders/laggards, geopolitical backdrop. (6-8 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }},
    "asia": {{
      "headline": "...",
      "body": "Asian equity analysis: China stimulus/property, Japan BOJ dynamics, EM differentiation. (6-8 lines)",
      "direction": "bullish|bearish|neutral",
      "key_driver": "...",
      "risk": "..."
    }}
  }},

  "section4_fixed_income": {{
    "title": "Fixed Income & Rates",
    "yield_curve": {{
      "us_2y": "use real data or estimate",
      "us_10y": "...",
      "us_30y": "...",
      "spread_2_10": "X bp",
      "interpretation": "Yield curve interpretation: shape, inversion status, what it signals for growth expectations (4-5 lines)"
    }},
    "narrative": "Full fixed income narrative: Fed posture, supply/demand for bonds, credit spreads (use FRED), institutional positioning, CB credibility, links to equities and FX. (8-10 lines)"
  }},

  "section5_forex": {{
    "title": "Foreign Exchange",
    "dxy": {{"value": "use real data", "change": "+/-X.X%", "interpretation": "DXY narrative and implications (3-4 lines)"}},
    "narrative": "Global FX dynamics: rate differentials, CB divergence, carry trades, geopolitical flows. (6-8 lines)",
    "pairs": [
      {{"pair": "EUR/USD", "value": "use real data", "change": "+/-X.X%", "change_num": 0.0, "direction": "up|down|flat", "analysis": "EUR/USD drivers today: ECB/Fed divergence, eurozone data, positioning. (3-4 lines)", "support": "X.XXXX", "resistance": "X.XXXX"}},
      {{"pair": "GBP/USD", "value": "...", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "USD/JPY", "value": "...", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "USD/CHF", "value": "...", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "AUD/USD", "value": "...", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}},
      {{"pair": "USD/CNY", "value": "...", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "...", "support": "...", "resistance": "..."}}
    ]
  }},

  "section6_commodities": {{
    "title": "Commodities & Alternative Assets",
    "narrative": "Cross-asset commodities narrative: inflation signaling, capital rotation from equities/bonds, geopolitical supply dynamics, dollar impact. (6-8 lines)",
    "items": [
      {{"name": "Gold", "value": "use real data", "unit": "$/oz", "change": "+/-X.X%", "change_num": 0.0, "direction": "up|down", "analysis": "2-3 line analysis of gold today: safe haven flows, real yields, dollar impact"}},
      {{"name": "WTI Crude", "value": "...", "unit": "$/bbl", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Brent Crude", "value": "...", "unit": "$/bbl", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Natural Gas", "value": "...", "unit": "$/MMBtu", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Copper", "value": "...", "unit": "$/lb", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Silver", "value": "...", "unit": "$/oz", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "..."}},
      {{"name": "Wheat", "value": "...", "unit": "¢/bu", "change": "...", "change_num": 0.0, "direction": "...", "analysis": "..."}}
    ]
  }},

  "section7_positioning": {{
    "title": "Market Positioning & Sentiment",
    "narrative": "True sentiment analysis beyond headlines. VIX level and what it implies. Options market structure: put/call ratio interpretation, skew, term structure. Derivatives pricing of tail risk. Institutional flow signals from ETF data. Complacency or fear — and why. (8-10 lines)"
  }},

  "section8_email_intelligence": {{
    "title": "Email Intelligence — Daily Market Watch",
    "summary": "Synthesis of today's email intelligence: key themes, important stories, second-order implications. Connect email content to market moves observed above. Surface what the emails reveal that open-source data doesn't. (5-7 lines, or note if no emails)",
    "key_stories": [
      {{"title": "Story title from email", "body": "2-3 line analysis connecting this story to markets", "relevance": "high|medium"}}
    ]
  }},

  "section9_synthesis": {{
    "title": "Daily Synthesis & Outlook",
    "regime": "risk_on|risk_off|transition|stagflation|goldilocks",
    "global_view": "Holistic synthesis: interconnection between rates/equities/dollar/commodities today, institutional flows, macro backdrop, tail risks, what to watch tomorrow. This is the centerpiece. (10-12 lines minimum)",
    "tomorrow_watch": "Key events and data to monitor on {tomorrow_str} — be specific about timing and expected market impact (4-5 lines)",
    "strategy": [
      {{"type": "Tactical", "recommendation": "...", "rationale": "3-4 line rationale", "timeframe": "1-3 days", "conviction": "high|medium|low"}},
      {{"type": "Risk Watch", "recommendation": "...", "rationale": "...", "timeframe": "...", "conviction": "..."}},
      {{"type": "Opportunity", "recommendation": "...", "rationale": "...", "timeframe": "...", "conviction": "..."}}
    ]
  }}
}}

IMPORTANT:
- Use REAL data from the market context above — do NOT invent prices or yields
- Write long, analytical paragraphs — not bullet lists
- Total report: ~15,000–20,000 characters
- ALL TEXT IN ENGLISH"""

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
#  GÉNÉRATION PDF
# ══════════════════════════════════════════════════════════════════════════════
# Color palette (matching weekly brief)
BG      = colors.HexColor("#07070D")
BG2     = colors.HexColor("#0D0D16")
GOLD    = colors.HexColor("#B8965A")
GOLD_LT = colors.HexColor("#D4AF7A")
LIGHT   = colors.HexColor("#E8E8F4")
TEXT    = colors.HexColor("#C8C8D8")
MUTED   = colors.HexColor("#525268")
GREEN   = colors.HexColor("#3DB87A")
RED     = colors.HexColor("#C85454")
YELLOW  = colors.HexColor("#C89A3D")
BLUE    = colors.HexColor("#5B7FD4")
BORDER  = colors.HexColor("#1E1E2E")


def ps(name: str, **kwargs) -> ParagraphStyle:
    defaults = dict(fontName="Helvetica", textColor=TEXT, fontSize=9,
                    leading=14, spaceAfter=6, spaceBefore=2, leftIndent=0)
    defaults.update(kwargs)
    return ParagraphStyle(name, **defaults)


def hr(c=BORDER, w=0.5):
    return HRFlowable(width="100%", thickness=w, color=c, spaceAfter=8, spaceBefore=4)


def tbl(data, col_widths, row_colors=None):
    t = Table(data, colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), BG2),
        ("TEXTCOLOR",  (0, 0), (-1, 0), GOLD),
        ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",   (0, 0), (-1, 0), 7),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BG, BG2]),
        ("GRID",       (0, 0), (-1, -1), 0.3, BORDER),
        ("LEFTPADDING",  (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING",   (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
    ]
    t.setStyle(TableStyle(style))
    return t


def generate_pdf(report: dict) -> bytes:
    print("[4/6] Génération du PDF...")
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                            leftMargin=14*mm, rightMargin=14*mm,
                            topMargin=12*mm, bottomMargin=12*mm)

    S_H1 = ps("h1", fontName="Helvetica-Bold", fontSize=18, textColor=GOLD_LT, spaceAfter=2)
    S_H2 = ps("h2", fontName="Helvetica-Bold", fontSize=13, textColor=GOLD, spaceAfter=4, spaceBefore=10)
    S_H3 = ps("h3", fontName="Helvetica-Bold", fontSize=10, textColor=LIGHT, spaceAfter=3, spaceBefore=6)
    S_BD = ps("bd", fontSize=8.5, textColor=TEXT, leading=13)
    S_BL = ps("bl", fontSize=8.5, textColor=LIGHT, fontName="Helvetica-Bold")
    S_MU = ps("mu", fontSize=7.5, textColor=MUTED, leading=12)
    S_GD = ps("gd", fontName="Helvetica-Bold", fontSize=8, textColor=GREEN)
    S_RD = ps("rd", fontName="Helvetica-Bold", fontSize=8, textColor=RED)

    story = []

    def sec(num: int, title: str):
        story.append(Spacer(1, 8))
        story.append(Paragraph(
            f'<font color="#B8965A" size="7">0{num} ·</font>  <font size="13" color="#D4AF7A">{title}</font>',
            ps("sh", fontName="Helvetica-Bold", fontSize=13, textColor=GOLD_LT, spaceBefore=8, spaceAfter=4)))
        story.append(hr(GOLD, 0.5))

    # ── HEADER
    temp = report.get("market_temperature", "neutral")
    temp_label = {"risk_on": "RISK ON ▲", "risk_off": "RISK OFF ▼", "neutral": "NEUTRAL ◆"}.get(temp, temp.upper())
    temp_color = GREEN if temp == "risk_on" else RED if temp == "risk_off" else YELLOW

    story.append(Paragraph(
        f'<font color="#B8965A" size="8" fontName="Helvetica">DAILY MARKET BRIEF · INTELLIGENCE NOTE</font>',
        ps("sup", fontSize=8, textColor=GOLD, fontName="Helvetica", spaceAfter=4)))
    story.append(Paragraph(
        f'<font size="22" color="#E8E8F4" fontName="Helvetica-Bold">Daily <font color="#B8965A">Market</font> Brief</font>',
        ps("title", fontSize=22, textColor=LIGHT, fontName="Helvetica-Bold", spaceAfter=2)))
    story.append(Paragraph(f'{DATE_FR}', ps("date", fontSize=9, textColor=MUTED, spaceAfter=2)))
    story.append(Paragraph(f'Market Regime: <font color="#{("3DB87A" if temp=="risk_on" else "C85454" if temp=="risk_off" else "C89A3D")}">{temp_label}</font> — {report.get("market_temperature_label", "")}',
                            ps("regime", fontSize=9, fontName="Helvetica-Bold", textColor=MUTED, spaceAfter=4)))
    story.append(hr(GOLD, 1))

    # S1 - Overview
    s1 = report.get("section1_overview", {})
    sec(1, s1.get("title", "Global Market Overview & Sentiment"))
    if s1.get("headline"):
        story.append(Paragraph(s1["headline"], S_BL))
        story.append(Spacer(1, 4))
    for para in s1.get("paragraphs", []):
        story.append(Paragraph(str(para), S_BD))
        story.append(Spacer(1, 3))

    # S2 - Macro
    s2 = report.get("section2_macro", {})
    sec(2, s2.get("title", "Macro-Economic Environment"))
    for para in s2.get("paragraphs", []):
        story.append(Paragraph(str(para), S_BD))
        story.append(Spacer(1, 3))

    # S3 - Equities
    story.append(PageBreak())
    s3 = report.get("section3_equities", {})
    sec(3, s3.get("title", "Equity Markets"))

    # Indices table
    indices = s3.get("indices", [])
    if indices:
        data_tbl = [["Index", "Price", "1D Chg", "YTD"]]
        for idx in indices:
            chg = idx.get("change", "")
            chg_color = GREEN if str(chg).startswith("+") else RED
            data_tbl.append([
                idx.get("name", ""), idx.get("value", ""),
                Paragraph(chg, ps("ic", fontSize=8, textColor=chg_color, fontName="Helvetica-Bold")),
                idx.get("ytd", "—")
            ])
        story.append(tbl(data_tbl, [55*mm, 28*mm, 25*mm, 25*mm]))
        story.append(Spacer(1, 6))

    # Sector heatmap
    sectors = s3.get("sector_performance", [])
    if sectors:
        story.append(Paragraph("Sector Performance", S_H3))
        sec_rows = [["Sector", "Change", "Sector", "Change"]]
        for i in range(0, len(sectors), 2):
            row = []
            for j in range(2):
                if i + j < len(sectors):
                    s = sectors[i + j]
                    chg = s.get("change", "")
                    c = GREEN if s.get("direction") == "up" else RED
                    row.extend([s.get("sector", ""),
                                Paragraph(chg, ps("sc", fontSize=8, textColor=c, fontName="Helvetica-Bold"))])
                else:
                    row.extend(["", ""])
            sec_rows.append(row)
        story.append(tbl(sec_rows, [45*mm, 22*mm, 45*mm, 22*mm]))
        story.append(Spacer(1, 6))

    # Regional analysis
    for region_key, region_label in [("us", "United States"), ("europe", "Europe"), ("asia", "Asia")]:
        reg = s3.get(region_key, {})
        if reg:
            dir_color = GREEN if reg.get("direction") == "bullish" else RED if reg.get("direction") == "bearish" else YELLOW
            story.append(Paragraph(
                f'<font color="#{("3DB87A" if reg.get("direction")=="bullish" else "C85454" if reg.get("direction")=="bearish" else "C89A3D")}">{region_label} — {reg.get("direction", "").upper()}</font>',
                ps("rl", fontSize=9, fontName="Helvetica-Bold", textColor=dir_color, spaceAfter=2)))
            if reg.get("headline"):
                story.append(Paragraph(reg["headline"], S_BL))
            story.append(Paragraph(str(reg.get("body", "")), S_BD))
            if reg.get("key_driver"):
                story.append(Paragraph(f"→ Key driver: {reg['key_driver']}", S_MU))
            if reg.get("risk"):
                story.append(Paragraph(f"⚠ Risk: {reg['risk']}", ps("rsk", fontSize=7.5, textColor=RED, spaceAfter=5)))
            story.append(Spacer(1, 4))

    # S4 - Fixed Income
    story.append(PageBreak())
    s4 = report.get("section4_fixed_income", {})
    sec(4, s4.get("title", "Fixed Income & Rates"))
    yc = s4.get("yield_curve", {})
    if yc:
        yc_data = [["Tenor", "Yield", "Spread 2s10s"]]
        yc_data.append(["US 2Y", f"{yc.get('us_2y', '—')}%", yc.get("spread_2_10", "—")])
        yc_data.append(["US 10Y", f"{yc.get('us_10y', '—')}%", ""])
        yc_data.append(["US 30Y", f"{yc.get('us_30y', '—')}%", ""])
        story.append(tbl(yc_data, [40*mm, 30*mm, 63*mm]))
        story.append(Spacer(1, 4))
        if yc.get("interpretation"):
            story.append(Paragraph(yc["interpretation"], S_BD))
    story.append(Paragraph(str(s4.get("narrative", "")), S_BD))

    # S5 - FX
    s5 = report.get("section5_forex", {})
    sec(5, s5.get("title", "Foreign Exchange"))
    dxy = s5.get("dxy", {})
    if dxy:
        story.append(Paragraph(
            f'DXY: <font fontName="Helvetica-Bold">{dxy.get("value", "")}</font> '
            f'<font color="{"#3DB87A" if str(dxy.get("change","")).startswith("+") else "#C85454"}">{dxy.get("change", "")}</font> — {dxy.get("interpretation", "")}',
            ps("dxy", fontSize=9, textColor=TEXT, spaceAfter=6)))
    story.append(Paragraph(str(s5.get("narrative", "")), S_BD))
    story.append(Spacer(1, 4))

    # FX pairs table
    pairs = s5.get("pairs", [])
    if pairs:
        fx_data = [["Pair", "Rate", "Chg", "Support", "Resist.", "Analysis"]]
        for p in pairs:
            chg = p.get("change", "")
            c = GREEN if str(chg).startswith("+") else RED
            fx_data.append([
                Paragraph(p.get("pair", ""), ps("fp", fontSize=8, fontName="Helvetica-Bold", textColor=LIGHT)),
                p.get("value", ""),
                Paragraph(chg, ps("fc", fontSize=8, textColor=c, fontName="Helvetica-Bold")),
                p.get("support", "—"), p.get("resistance", "—"),
                Paragraph(str(p.get("analysis", ""))[:200], ps("fa", fontSize=7, textColor=TEXT))
            ])
        story.append(tbl(fx_data, [18*mm, 18*mm, 14*mm, 14*mm, 14*mm, 55*mm]))

    # S6 - Commodities
    story.append(PageBreak())
    s6 = report.get("section6_commodities", {})
    sec(6, s6.get("title", "Commodities & Alternative Assets"))
    story.append(Paragraph(str(s6.get("narrative", "")), S_BD))
    story.append(Spacer(1, 4))

    items = s6.get("items", [])
    if items:
        cmd_data = [["Commodity", "Price", "Change", "Analysis"]]
        for it in items:
            chg = it.get("change", "")
            c = GREEN if str(chg).startswith("+") else RED
            cmd_data.append([
                Paragraph(f'{it.get("name", "")} <font color="#525268" size="7">{it.get("unit", "")}</font>',
                          ps("cn", fontSize=8.5, textColor=LIGHT, fontName="Helvetica-Bold")),
                it.get("value", ""),
                Paragraph(chg, ps("cc", fontSize=8, textColor=c, fontName="Helvetica-Bold")),
                Paragraph(str(it.get("analysis", ""))[:180], ps("ca", fontSize=7.5, textColor=TEXT))
            ])
        story.append(tbl(cmd_data, [28*mm, 20*mm, 16*mm, 69*mm]))

    # S7 - Positioning
    s7 = report.get("section7_positioning", {})
    sec(7, s7.get("title", "Market Positioning & Sentiment"))
    story.append(Paragraph(str(s7.get("narrative", "")), S_BD))

    # S8 - Email Intelligence
    s8 = report.get("section8_email_intelligence", {})
    sec(8, s8.get("title", "Email Intelligence — Daily Market Watch"))
    story.append(Paragraph(str(s8.get("summary", "")), S_BD))
    for story_item in s8.get("key_stories", []):
        rel = story_item.get("relevance", "medium")
        c = GREEN if rel == "high" else YELLOW
        story.append(Spacer(1, 4))
        story.append(Paragraph(
            f'<font color="{"#3DB87A" if rel=="high" else "#C89A3D"}" fontName="Helvetica-Bold" size="8">● {story_item.get("title", "")}</font>',
            ps("si", fontSize=8, spaceAfter=2)))
        story.append(Paragraph(str(story_item.get("body", "")), S_BD))

    # S9 - Synthesis
    story.append(PageBreak())
    s9 = report.get("section9_synthesis", {})
    sec(9, s9.get("title", "Daily Synthesis & Outlook"))

    regime = s9.get("regime", "")
    regime_label = {"risk_on": "RISK ON", "risk_off": "RISK OFF", "transition": "TRANSITION",
                    "stagflation": "STAGFLATION", "goldilocks": "GOLDILOCKS"}.get(regime, regime.upper())
    story.append(Paragraph(
        f'Regime: <font fontName="Helvetica-Bold" color="#B8965A">{regime_label}</font>',
        ps("rm", fontSize=9, textColor=MUTED, spaceAfter=4)))
    story.append(Paragraph(str(s9.get("global_view", "")), S_BD))
    story.append(Spacer(1, 6))

    if s9.get("tomorrow_watch"):
        story.append(Paragraph("📅 Tomorrow — Key Events & Data", S_H3))
        story.append(Paragraph(str(s9["tomorrow_watch"]), S_BD))
        story.append(Spacer(1, 6))

    # Strategy cards
    for st in s9.get("strategy", []):
        conviction = st.get("conviction", "medium")
        c = GREEN if conviction == "high" else YELLOW if conviction == "medium" else MUTED
        story.append(Paragraph(
            f'<font fontName="Helvetica-Bold" size="7" color="{"#3DB87A" if conviction=="high" else "#C89A3D" if conviction=="medium" else "#525268"}">{st.get("type", "").upper()} · {conviction.upper()} CONVICTION</font>',
            ps("stt", fontSize=7, spaceAfter=2)))
        story.append(Paragraph(str(st.get("recommendation", "")), S_BL))
        story.append(Paragraph(str(st.get("rationale", "")), S_BD))
        story.append(hr(GREEN, 0.3))

    # Footer
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        f"Daily Market Brief · Generated {DATE_FR} · Sources: yfinance, FRED, Finnhub, Gmail (Daily Market Watch) · Claude AI Analysis · Not investment advice",
        ps("ft", fontSize=6.5, textColor=MUTED, alignment=TA_CENTER)))
    story.append(hr(GOLD, 0.5))

    doc.build(story)
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
