"""
NSE Market Perception Report
Requires: pip install yfinance pandas xlsxwriter
"""
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import warnings, time, os
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
file_path = "StockList/nse500list.csv"
INTER_DELAY = 0.5   # seconds between tickers

# ─────────────────────────────────────────────
# FETCH ONE TICKER — let yfinance manage session
# ─────────────────────────────────────────────
def fetch_ticker(symbol, start, end, max_retries=3):
    """Returns pd.Series of Close prices or None."""
    for attempt in range(max_retries):
        try:
            t    = yf.Ticker(symbol)          # NO session= param
            hist = t.history(start=start, end=end,
                             interval='1d', auto_adjust=True,
                             raise_errors=False)
            if hist is not None and not hist.empty and 'Close' in hist.columns:
                s = hist['Close'].dropna()
                s.index = s.index.tz_localize(None)
                if len(s) > 10:
                    return s
            wait = 4 + attempt * 4
            time.sleep(wait)
        except Exception as e:
            wait = 4 + attempt * 6
            print(f"  [{symbol}] attempt {attempt+1} error: {e} — retry in {wait}s")
            time.sleep(wait)
    return None

# ─────────────────────────────────────────────
# 1. LOAD TICKERS
# ─────────────────────────────────────────────
df_stocks = pd.read_csv(file_path)
df_stocks.columns = df_stocks.columns.str.strip()
col_name  = 'Symbol' if 'Symbol' in df_stocks.columns else df_stocks.columns[0]

SYMBOL_MAP = {'TI': 'TIINDIA'}

raw_symbols = [str(s).strip() for s in df_stocks[col_name]]
tickers     = [(SYMBOL_MAP.get(s, s) + '.NS') for s in raw_symbols]
tickers     = [t for t in tickers if 'DUMMYALCAR' not in t]
tickers     = list(dict.fromkeys(tickers))

# ─────────────────────────────────────────────
# 2. DATE RANGE
# ─────────────────────────────────────────────
end_date   = datetime.now()
start_date = end_date - timedelta(days=450)

# ─────────────────────────────────────────────
# 3. PASS 1 — download all tickers
# ─────────────────────────────────────────────
data_dict    = {}
failed_pass1 = []

print(f"Pass 1: Downloading {len(tickers)} tickers...\n")

for i, symbol in enumerate(tickers, 1):
    if i % 50 == 0:
        print(f"  [{i:>3}/{len(tickers)}] ok={len(data_dict)}  failed={len(failed_pass1)}")

    result = fetch_ticker(symbol, start_date, end_date)
    if result is not None:
        data_dict[symbol] = result
    else:
        failed_pass1.append(symbol)

    time.sleep(INTER_DELAY)

print(f"\nPass 1 done: {len(data_dict)} ok | {len(failed_pass1)} failed")

# ─────────────────────────────────────────────
# 4. PASS 2 — retry after 30s cooldown
# ─────────────────────────────────────────────
if failed_pass1:
    print(f"\nPass 2: Cooling down 30s then retrying {len(failed_pass1)} tickers...")
    time.sleep(30)
    failed_pass2 = []

    for symbol in failed_pass1:
        result = fetch_ticker(symbol, start_date, end_date, max_retries=4)
        if result is not None:
            data_dict[symbol] = result
            print(f"  Recovered: {symbol}")
        else:
            failed_pass2.append(symbol)
        time.sleep(1.5)

    # ── PASS 3 ────────────────────────────────
    if failed_pass2:
        print(f"\nPass 3: Final attempt for {len(failed_pass2)} after 60s cooldown...")
        time.sleep(60)
        perm_failed = []

        for symbol in failed_pass2:
            result = fetch_ticker(symbol, start_date, end_date, max_retries=5)
            if result is not None:
                data_dict[symbol] = result
                print(f"  Recovered: {symbol}")
            else:
                perm_failed.append(symbol)
            time.sleep(2.0)

        if perm_failed:
            print(f"\n  Permanently skipped ({len(perm_failed)}): {perm_failed}")
        else:
            print("  All recovered in Pass 3!")
    else:
        print("  All recovered in Pass 2!")

# ─────────────────────────────────────────────
# 5. BUILD ALIGNED DATAFRAME
# ─────────────────────────────────────────────
data = pd.DataFrame(data_dict)
data.sort_index(inplace=True)

valid_tickers = list(data.columns)
print(f"\nProceeding with {len(valid_tickers)} valid tickers out of {len(tickers)} requested.\n")

# ─────────────────────────────────────────────
# 6. WEEKLY LOGIC (strict Friday)
# ─────────────────────────────────────────────
IST       = ZoneInfo("Asia/Kolkata")
now_ist   = datetime.now(IST)
nse_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

weekly_all = data.resample('W-FRI').last()

if now_ist.weekday() < 4 or (now_ist.weekday() == 4 and now_ist < nse_close):
    weekly_all = weekly_all.iloc[:-1]

target_weeks = weekly_all.tail(4).index.tolist()
target_weeks.reverse()

print("=== Weekly closing dates being used ===")
for week_date in target_weeks:
    subset      = data[data.index <= week_date]
    actual_date = subset.index[-1]
    day_name    = actual_date.strftime('%A')
    flag = "" if actual_date.weekday() == 4 else \
           f"  <- holiday fallback (Friday was {week_date.strftime('%d-%b')})"
    print(f"  Week {week_date.strftime('%Y-%m-%d')} -> actual close: "
          f"{actual_date.strftime('%Y-%m-%d')} ({day_name}){flag}")
print()

# ─────────────────────────────────────────────
# 7. CALCULATION LOOP
# ─────────────────────────────────────────────
results_dict = {'Ticker': [t.replace('.NS', '') for t in valid_tickers]}
rs_cols      = []
breadth_data = []

for i, week_date in enumerate(target_weeks):
    date_str    = week_date.strftime('%Y-%m-%d')
    subset_data = data[data.index <= week_date].copy()

    if len(subset_data) < 90:
        print(f"Warning: Not enough data for week {date_str}, skipping.")
        continue

    ret_90       = (subset_data.iloc[-1] / subset_data.iloc[-90]) - 1
    ret_50       = (subset_data.iloc[-1] / subset_data.iloc[-50]) - 1
    ret_20       = (subset_data.iloc[-1] / subset_data.iloc[-20]) - 1
    weighted_ret = (ret_90 * 0.25) + (ret_50 * 0.25) + (ret_20 * 0.50)
    rs_score     = (weighted_ret.rank(pct=True) * 100).round(2)

    if i == 0:
        results_dict[f'Price_{date_str}']   = subset_data.iloc[-1].values.round(2)
        results_dict[f'Ret_90d_{date_str}'] = (ret_90  * 100).round(2).values
        results_dict[f'Ret_50d_{date_str}'] = (ret_50  * 100).round(2).values
        results_dict[f'Ret_20d_{date_str}'] = (ret_20  * 100).round(2).values

    col_rs = f'RS_Score_{date_str}'
    results_dict[col_rs] = rs_score.values
    rs_cols.append(col_rs)

    ema20         = subset_data.ewm(span=20,  adjust=False).mean().iloc[-1]
    ema50         = subset_data.ewm(span=50,  adjust=False).mean().iloc[-1]
    ema200        = subset_data.ewm(span=200, adjust=False).mean().iloc[-1]
    current_price = subset_data.iloc[-1]
    n             = len(valid_tickers)

    breadth_data.append({
        'Week_Ending':           date_str,
        'Stocks_Above_20EMA_%':  round((current_price > ema20).sum()  / n * 100, 2),
        'Stocks_Above_50EMA_%':  round((current_price > ema50).sum()  / n * 100, 2),
        'Stocks_Above_200EMA_%': round((current_price > ema200).sum() / n * 100, 2),
    })

# ─────────────────────────────────────────────
# 8. PREPARE DATAFRAMES
# ─────────────────────────────────────────────
df_master     = pd.DataFrame(results_dict).sort_values(by=rs_cols[0], ascending=False)
df_consistent = df_master[(df_master[rs_cols] >= 80).all(axis=1)]
df_recent     = df_master[(df_master[rs_cols[:2]] >= 80).all(axis=1)]
df_breadth    = pd.DataFrame(breadth_data)

# ─────────────────────────────────────────────
# 9. EXPORT — XlsxWriter formatting
# ─────────────────────────────────────────────
base_name   = "NSE_500_Perception_Report"
output_dir  = "Report/RSReport"
output_file = os.path.join(output_dir, f'{base_name}.xlsx')
os.makedirs(output_dir, exist_ok=True)
counter = 1
while True:
    try:
        if os.path.exists(output_file):
            open(output_file, 'r+b').close()
        break
    except PermissionError:
        output_file = os.path.join(output_dir, f'{base_name}_{counter}.xlsx')
        counter += 1
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:

    def apply_styles(df, sheet_name, add_tv_links=False):
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        wb  = writer.book
        ws  = writer.sheets[sheet_name]
        ws.freeze_panes(1, 1)

        base        = {'font_name': 'Arial', 'font_size': 10, 'border': 1}
        hdr_fmt     = wb.add_format({**base, 'bold': True,  'bg_color': '#1F4E79', 'font_color': '#FFFFFF'})
        default_fmt = wb.add_format({**base})
        blue_bg     = wb.add_format({**base, 'bg_color': '#DEEAF6'})
        green_font  = wb.add_format({**base, 'font_color': '#006100'})
        red_font    = wb.add_format({**base, 'font_color': '#FF0000'})
        rs_high     = wb.add_format({**base, 'bold': True,  'bg_color': '#00B050', 'font_color': '#FFFFFF'})
        link_fmt    = wb.add_format({**base, 'font_color': '#0070C0', 'underline': True})

        for ci, cn in enumerate(df.columns):
            ws.write(0, ci, cn, hdr_fmt)

        for idx, col in enumerate(df.columns):
            if idx == 0 or 'Price_' in col:
                ws.set_column(idx, idx, 15, blue_bg)
            elif 'Ret_' in col:
                ws.set_column(idx, idx, 14, default_fmt)
                ws.conditional_format(1, idx, len(df), idx,
                    {'type': 'cell', 'criteria': '>=', 'value': 0, 'format': green_font})
                ws.conditional_format(1, idx, len(df), idx,
                    {'type': 'cell', 'criteria': '<',  'value': 0, 'format': red_font})
            elif 'RS_Score' in col:
                ws.set_column(idx, idx, 16, default_fmt)
                ws.conditional_format(1, idx, len(df), idx,
                    {'type': 'cell', 'criteria': '>', 'value': 80, 'format': rs_high})
            else:
                ws.set_column(idx, idx, 14, default_fmt)

        if add_tv_links:
            tv_col = len(df.columns)
            ws.write(0, tv_col, 'TradingView Chart', hdr_fmt)
            ws.set_column(tv_col, tv_col, 22, default_fmt)
            for row_idx, ticker in enumerate(df['Ticker'], start=1):
                symbol   = str(ticker).replace('.NS', '').strip()
                tv_url   = f'https://www.tradingview.com/chart/?symbol=NSE%3A{symbol}'
                ws.write_url(row_idx, tv_col, tv_url, link_fmt, 'View Chart')

    apply_styles(df_master,     'All Stocks Analysis', add_tv_links=True)
    apply_styles(df_consistent, 'RS Above 80 (4 Weeks)')
    apply_styles(df_recent,     'RS Above 80 (Last 2 Weeks)')

    df_breadth.to_excel(writer, sheet_name='Market Breadth', index=False)
    bws = writer.sheets['Market Breadth']
    bb  = {'font_name': 'Arial', 'font_size': 10, 'border': 1}
    b_h = writer.book.add_format({**bb, 'bold': True, 'bg_color': '#1F4E79', 'font_color': '#FFFFFF'})
    b_d = writer.book.add_format({**bb})
    for ci, cn in enumerate(df_breadth.columns):
        bws.write(0, ci, cn, b_h)
    bws.set_column(0, len(df_breadth.columns) - 1, 25, b_d)

print(f"\nSuccessfully generated: {output_file}")
print(f"   All Stocks:         {len(df_master)}")
print(f"   Consistent RS>=80:  {len(df_consistent)}")
print(f"   Recent 2-wk RS>=80: {len(df_recent)}")
