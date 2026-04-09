"""
NIFTY 750 ACCUMULATION SCREENER - FAST VERSION
===============================================
- Direct NSE bhavcopy fetch using nselib
- 35 days of data only
- Volume Surge > 1.4x
- Sends TEXT-ONLY alerts with mini bar chart to Telegram (private chat + group)
- No CSV file output
"""

# ============================================
# INSTALL & IMPORT
# ============================================

import subprocess
import sys

def install_packages():
    try:
        import nselib, pandas, numpy
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "nselib", "pandas", "numpy", "requests", "-q"])

install_packages()

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import requests
import warnings
warnings.filterwarnings('ignore')

# ============================================
# CONFIGURATION
# ============================================

PRICE_CHANGE_LIMIT = 3.0      # ±3%
VOLUME_SURGE_MIN = 1.40       # 1.4x (updated)
VOLUME_LOOKBACK_DAYS = 7
VOLUME_PREV_SETS = 4
TOP_STOCKS = 5

# ============================================
# NIFTY 750 SYMBOLS (complete list - truncated for brevity, but you must keep your full list)
# ============================================

# NOTE: Use your full 750-symbol list here. For space, I show a few lines.
# Replace this with your complete NIFTY_SYMBOLS list from earlier.

NIFTY_SYMBOLS = [
    'HDFCBANK', 'RELIANCE', 'ICICIBANK', 'SBIN', 'BHARTIARTL', 'LATENTVIEW',
    'INFY', 'ETERNAL', 'LT', 'TCS', 'OLAELEC', 'DIXON', 'AXISBANK', 'VEDL',
    'BSE', 'M&M', 'VMART', 'ADANIPOWER', 'GRSE', 'INDIGO', 'HCLTECH',
    'HINDCOPPER', 'BEL', 'TMCV', 'ADANIGREEN', 'KOTAKBANK', 'COFORGE',
    'BAJFINANCE', 'IDEA', 'TATACHEM', 'HINDALCO', 'MAZDOCK', 'MCX', 'DMART',
    'SUNPHARMA', 'TITAN', 'SHRIRAMFIN', 'ONGC', 'NTPC', 'HAL', 'KAYNES',
    'MARUTI', 'TATASTEEL', 'ITC', 'PERSISTENT', 'ASHOKLEY', 'POWERGRID',
    'EICHERMOT', 'ADANIPORTS', 'SAIL', 'DRREDDY', 'JIOFIN', 'ZAGGLE',
    'ASIANPAINT', 'DLF', 'GROWW', 'TRENT', 'NATIONALUM', 'HEROMOTOCO',
    'WAAREEENER', 'CIPLA', 'BAJAJFINSV', 'CANBK', 'BPCL', 'BANKBARODA',
    'TATAPOWER', 'PFC', 'TECHM', 'ULTRACEMCO', 'PGEL', 'LODHA', 'ADANIENT',
    'HDFCLIFE', 'TORNTPHARM', 'WIPRO', 'CUMMINSIND', 'TVSMOTOR', 'BLUESTARCO',
    'IOC', 'COALINDIA', 'LAURUSLABS', 'CHOLAFIN', 'VOLTAS', 'DIVISLAB', 'LTM',
    'HINDUNILVR', 'DATAPATTNS', 'MOTHERSON', 'COCHINSHIP', 'OIL', 'POWERINDIA',
    'ACUTAAS', 'LUPIN', 'SWIGGY', 'TMPV', 'AMBER', 'BOSCHLTD', 'GVT&D',
    'CHENNPETRO', 'TATACONSUM', 'POLYCAB', 'SBILIFE', 'SUZLON', 'HDFCAMC',
    'PAGEIND', 'RECLTD', 'SAMMAANCAP', 'SOLARINDS', 'JSWSTEEL', 'UNIONBANK',
    'APOLLOHOSP', 'HINDPETRO', 'AUROPHARMA', 'BRITANNIA', 'PRESTIGE',
    'LGEINDIA', 'PNB', 'CDSL', 'KARURVYSYA', 'KEI', 'HINDZINC', 'BAJAJ-AUTO',
    'INDHOTEL', 'RBLBANK', 'PAYTM', 'MARICO', 'GAIL', 'NMDC', 'MUTHOOTFIN',
    'BIOCON', 'FORCEMOT', 'ASTRAL', 'VMM', 'ICICIGI', 'HYUNDAI', 'GLENMARK',
    'BHEL', 'IDBI', 'INDUSINDBK', 'PREMIERENE', 'NETWEB', 'ANGELONE',
    'TEJASNET', 'HSCL', 'MAXHEALTH', 'AUBANK', 'GODREJPROP', 'SUPREMEIND',
    'SRF', 'IRFC', 'YESBANK', 'GODREJCP', 'BDL', 'BANDHANBNK', 'MPHASIS',
    'JINDALSTEL', 'ABB', 'BHARATFORG', 'BLS', 'CPPLUS', 'UNOMINDA', 'NAUKRI',
    'ADANIENSOL', 'NESTLEIND', 'REDINGTON', 'RVNL', 'INDUSTOWER', 'IDFCFIRSTB',
    'MANAPPURAM', 'ICICIAMC', 'NYKAA', 'JSWENERGY', 'VBL', 'FEDERALBNK',
    'EIEL', 'RPOWER', 'CGPOWER', 'CUPID', 'POLICYBZR', 'INDIANB', 'HEXT',
    'WEBELSOLAR', 'LLOYDSME', 'MFSL', 'GODFRYPHLP', 'GRASIM', 'GMDCLTD',
    'MRPL', 'BANKINDIA', 'HFCL', 'JUBLFOOD', 'KPITTECH', 'LTF', 'MAHABANK',
    'ATHERENERG', 'OBEROIRLTY', 'TATAELXSI', 'UNITDSPR', 'MANKIND', 'LICI',
    'PFOCUS', 'PIDILITIND', 'HAVELLS', 'NAVINFLUOR', 'KALYANKJIL', 'UJJIVANSFB',
    'APARINDS', 'CAMS', 'OFSS', 'ABCAPITAL', 'ANANTRAJ', 'MRF', 'HBLENGINE',
    'NATCOPHARM', 'TORNTPOWER', 'IREDA', 'APLAPOLLO', 'NBCC', 'JBMA', 'BSOFT',
    'SONACOMS', 'PATANJALI', 'FORTIS', 'ACMESOLAR', 'LICHSGFIN', 'PRAJIND',
    'ANANDRATHI', 'ZYDUSLIFE', 'INOXWIND', 'RADICO', 'APOLLO', 'NAM-INDIA',
    'AMBUJACEM', 'ENGINERSIN', 'PNBHOUSING', 'UPL', 'WOCKPHARMA', 'SAGILITY',
    'IFCI', 'JPPOWER', 'HEG', 'QPOWER', 'SIEMENS', 'ASTRAMICRO', 'GPIL',
    'KTKBANK', 'GMRAIRPORT', 'STLTECH', 'ICICIPRULI', 'KNRCON', 'ATGL',
    'TARIL', 'LENSKART', 'GRAVITA', 'SIGNATURE', 'NAZARA', 'NHPC', 'SOUTHBANK',
    'APTUS', 'PETRONET', 'ZEEL', 'JSLL', 'PHOENIXLTD', 'ENRIN', 'PIIND', 'KRN',
    'CARBORUNIV', 'NTPCGREEN', 'TIINDIA', 'EMCURE', 'SCI', 'SAILIFE', 'EXIDEIND',
    'M&MFIN', 'SYRMA', 'TDPOWERSYS', 'DELHIVERY', 'MOTILALOFS', 'CONCOR',
    'TTML', 'CRISIL', 'JSL', 'NLCINDIA', 'PARADEEP', '360ONE', 'OLECTRA',
    'BELRISE', 'LINDEINDIA', 'CGCL', 'COROMANDEL', 'MSUMI', 'MTARTECH',
    'JAYNECOIND', 'HUDCO', 'JINDALSAW', 'IRCTC', 'JSWINFRA', 'SBICARD', 'CUB',
    'GRAPHITE', 'HAPPSTMNDS', 'TATATECH', 'BAJAJHFL', 'COLPAL', 'TIMETECHNO',
    'NEWGEN', 'THERMAX', 'SOBHA', 'DABUR', 'SHREECEM', 'CROMPTON', 'BAJAJHLDNG',
    'IEX', 'GESHIP', 'CARTRADE', 'BEML', 'IRCON', 'ESCORTS', 'PCBL',
    'DEEPAKFERT', 'ZENTEC', 'NH', 'ECLERX', 'POLYMED', 'SAPPHIRE', 'MEESHO',
    'JWL', 'DEVYANI', 'PARAS', 'SARDAEN', 'NUVAMA', 'AVANTIFEED', 'JAINREC',
    'ITCHOTELS', 'JBCHEPHARM', 'AARTIIND', 'BALKRISIND', 'IIFL', 'KFINTECH',
    'POONAWALLA', 'WELCORP', 'FIRSTCRY', 'AIIL', 'GOKEX', 'SWANCORP', 'J&KBANK',
    'IGL', 'ALKEM', 'GRANULES', 'TITAGARH', 'AEQUS', 'HCC', 'VTL', 'LEMONTREE',
    'SUNDARMFIN', 'BORORENEW', 'PCJEWELLER', 'FIVESTAR', 'SHAILY', 'SCHAEFFLER',
    'BHARTIHEXA', 'KPIGREEN', 'TATACOMM', 'IXIGO', 'SWSOLAR', 'RAIN', 'GABRIEL',
    'UBL', 'ABSLAMC', 'KIRLOSENG', 'PPLPHARMA', 'SMLMAH', 'EDELWEISS',
    'SHILPAMED', 'AEGISLOG', 'SANSERA', 'CYIENT', 'RENUKA', 'AKZOINDIA',
    'ASTERDM', 'URBANCO', 'MEDANTA', 'SHAKTIPUMP', 'AETHER', 'PIRAMALFIN',
    'FINCABLES', 'LLOYDSENGG', 'AFFLE', 'CHAMBLFERT', 'KEC', 'ARE&M',
    'ELECTCAST', 'AWL', 'TEXRAIL', 'VOLTAMP', 'CHOICEIN', 'ANURAS', 'BALRAMCHIN',
    'SJVN', 'IPCALAB', 'JKTYRE', 'AZAD', 'WAAREERTL', 'VIKRAMSOLR', 'ATLANTAELE',
    'MGL', 'MIDHANI', 'TATACAP', 'ZFCVINDIA', 'YATHARTH', 'WABAG', 'NEULANDLAB',
    'BALUFORGE', 'RAILTEL', 'EPL', 'BRIGADE', 'STARHEALTH', 'PWL', 'ARVINDFASN',
    'KAJARIACER', 'TANLA', 'CREDITACC', 'IRB', 'GLAXO', 'ACC', 'ELGIEQUIP',
    'CESC', 'SHARDACROP', 'NCC', 'BATAINDIA', '3MINDIA', 'AXISCADES',
    'METROPOLIS', 'SONATSOFTW', 'POWERMECH', 'TI', 'PTCIL', 'HOMEFIRST',
    'GODREJIND', 'PINELABS', 'RCF', 'JMFINANCIL', 'ANTHEM', 'EMMVEE', 'HONAUT',
    'TATAINVEST', 'V2RETAIL', 'PRIVISCL', 'LALPATHLAB', 'OSWALPUMPS',
    'ZENSARTECH', 'FSL', 'GILLETTE', 'LTTS', 'JAMNAAUTO', 'SHRIPISTON',
    'DALBHARAT', 'ARVIND', 'SYNGENE', 'CONCORDBIO', 'RATEGAIN', 'EIDPARRY',
    'CASTROLIND', 'CEATLTD', 'STAR', 'ABFRL', 'DCBBANK', 'LUMAXTECH', 'EMAMILTD',
    'ASHAPURMIN', 'CLEAN', 'SKYGOLD', 'TECHNOE', 'TRIDENT', 'INDIAMART',
    'LTFOODS', 'SUNTV', 'ACE', 'RAINBOW', 'HONASA', 'FACT', 'TRANSRAILL',
    'SANDUMA', 'DEEPAKNTR', 'THANGAMAYL', 'APOLLOTYRE', 'TRIVENI', 'ONESOURCE',
    'PVRINOX', 'KSB', 'JUBLPHARMA', 'INTELLECT', 'CSBBANK', 'CENTRALBK',
    'UCOBANK', 'CEMPRO', 'AAVAS', 'GSFC', 'SAMHI', 'CHOLAHLDNG', 'KITEX',
    'INOXGREEN', 'IKS', 'ABREL', 'BLUESTONE', 'CCL', 'MMTC', 'GLAND', 'ELECON',
    'GSPL', 'MOIL', 'ASHOKA', 'TENNIND', 'WELSPUNLIV', 'JKCEMENT', 'GPPL',
    'IOB', 'DIACABS', 'NETWORK18', 'JYOTICNC', 'LLOYDSENT', 'AJANTPHARM',
    'ABDL', 'ENDURANCE', 'RELIGARE', 'BERGEPAINT', 'ITI', 'BLUEJET', 'HDBFS',
    'BANCOINDIA', 'GREAVESCOT', 'KRBL', 'RTNPOWER', 'ZYDUSWELL', 'GOKULAGRO',
    'PFIZER', 'IMFA', 'MANORAMA', 'EMBDL', 'CMSINFO', 'EQUITASBNK', 'SHAREINDIA',
    'CCAVENUE', 'MAHSEAMLES', 'INDIAGLYCO', 'TMB', 'BIRLACORPN', 'TIMKEN',
    'GICRE', 'GNFC', 'GRWRHITECH', 'RKFORGE', 'ALOKINDS', 'NAVA', 'CIGNITITEC',
    'USHAMART', 'KSCL', 'DBREALTY', 'SPARC', 'MSTCLTD', 'KIMS', 'AARTIPHARM',
    'TRITURBINE', 'PARKHOSPS', 'FLUOROCHEM', 'JSWCEMENT', 'AEGISVOPAK',
    'MAPMYINDIA', 'DYNAMATECH', 'KPIL', 'TIPSMUSIC', 'PTC', 'BALAMINES',
    'RITES', 'ABBOTINDIA', 'INDIGOPNTS', 'RBA', 'AURIONPRO', 'SAREGAMA',
    'FEDFINA', 'ELLEN', 'AVALON', 'RAMCOCEM', 'CRAMC', 'CAPLIPOINT', 'GMRP&UI',
    'MASTEK', 'SKIPPER', 'VIJAYA', 'JAIBALAJI', 'IONEXCHANG', 'ANUP', 'DOMS',
    'BAYERCROP', 'ALKYLAMINE', 'KPRMILL', 'AIAENG', 'PRICOLLTD', 'RRKABEL',
    'DATAMATICS', 'MANYAVAR', 'IGIL', 'GAEL', 'WHIRLPOOL', 'MARKSANS', 'NSLNISP',
    'KIRLOSBROS', 'CRAFTSMAN', 'NFL', 'JKPAPER', 'EIHOTEL', 'HEMIPROP', 'ATUL',
    'INDGN', 'COHANCE', 'LOTUSDEV', 'BECTORFOOD', 'AWFIS', 'RALLIS', 'AKUMS',
    'HGINFRA', 'FINPIPE', 'UTIAMC', 'IIFLCAPS', 'JUBLINGREA', 'RTNINDIA',
    'MINDACORP', 'JYOTHYLAB', 'ROUTE', 'HERITGFOOD', 'FIEMIND', 'ABLBL',
    'OPTIEMUS', 'CANFINHOME', 'RAYMONDLSL', 'SURYAROSNI', 'INOXINDIA', 'NEOGEN',
    'BLACKBUCK', 'BBOX', 'VIYASH', 'AADHARHFC', 'SENCO', 'INDIACEM',
    'THOMASCOOK', 'PNCINFRA', 'REFEX', 'DBL', 'GODIGIT', 'SCHNEIDER', 'LXCHEM',
    'SAATVIKGL', 'BLUEDART', 'VGUARD', 'PNGJL', 'JUSTDIAL', 'SUPRIYA',
    'STYRENIX', 'ASAHIINDIA', 'TEGA', 'ACI', 'CELLO', 'GHCL', 'SUDARSCHEM',
    'CIEINDIA', 'BBTC', 'RUBICON', 'TBOTEK', 'JKLAKSHMI', 'DCMSHRIRAM', 'SBFC',
    'VAIBHAVGBL', 'BIKAJI', 'ICIL', 'HCG', 'NIACL', 'SUMICHEM', 'VARROC',
    'ENTERO', 'CAMPUS', 'SHYAMMETL', 'RELAXO', 'GODREJAGRO', 'MAHSCOOTER',
    'AHLUCONT', 'EMIL', 'CORONA', 'THYROCARE', 'ORKLAINDIA', 'JSFB', 'AFCONS',
    'CANHLIFE', 'KANSAINER', 'APLLTD', 'IFBIND', 'SUNTECK', 'KIRLPNU', 'NESCO',
    'GMMPFAUDLR', 'CENTURYPLY', 'ALIVUS', 'AVL', 'EUREKAFORB', 'AARTIDRUGS',
    'SFL', 'SPLPETRO', 'TARC', 'PGIL', 'CRIZAC', 'CHALET', 'SKFINDIA',
    'ASKAUTOLTD', 'BAJAJELEC', 'PICCADIL', 'TSFINV', 'TRAVELFOOD', 'WESTLIFE',
    'THELEELA', 'ORIENTCEM', 'ADVENZYMES', 'VIPIND', 'SUBROS', 'PRUDENT',
    'ETHOSLTD', 'INDIASHLTR', 'WEWORK', 'CERA', 'REDTAPE', 'SAFARI', 'RHIM',
    'ERIS', 'TVSSCS', 'STARCEMENT', 'PURVA', 'NUVOCO', 'JLHL', 'CAPILLARY',
    'SKFINDUS', 'QUESS', 'GALLANTT', 'PRSMJOHNSN', 'NIVABUPA', 'MEDPLUS',
    'UTLSOLAR', 'WAKEFIT', 'AGARWALEYE', 'WELENT', 'SUDEEPPHRM', 'STYL',
    'SMARTWORKS'
]

NIFTY_SYMBOLS_SET = set(NIFTY_SYMBOLS)
print(f"✓ Loaded {len(NIFTY_SYMBOLS)} NIFTY 750 symbols")

# ============================================
# FETCH BHAVCOPY FOR A SINGLE DATE
# ============================================

from nselib import capital_market

def fetch_bhavcopy_for_date(trade_date):
    try:
        date_str = trade_date.strftime('%d-%m-%Y')
        df = capital_market.bhav_copy_with_delivery(trade_date=date_str)
        if df is None or len(df) == 0:
            return None
        df.columns = df.columns.str.upper().str.strip()
        symbol_col = 'SYMBOL' if 'SYMBOL' in df.columns else 'SYMBOL1'
        series_col = 'SERIES' if 'SERIES' in df.columns else 'SERIES1'
        close_col = 'CLOSE' if 'CLOSE' in df.columns else 'CLOSE_PRICE'
        volume_col = 'TTL_TRD_QNTY' if 'TTL_TRD_QNTY' in df.columns else 'TOTTRDQTY'
        result = pd.DataFrame()
        result['SYMBOL'] = df[symbol_col].astype(str).str.strip().str.upper()
        result['SERIES'] = df[series_col].astype(str).str.strip().str.upper()
        result['CLOSE_PRICE'] = pd.to_numeric(df[close_col], errors='coerce')
        result['TTL_TRD_QNTY'] = pd.to_numeric(df[volume_col], errors='coerce')
        if 'DELIV_PER' in df.columns:
            result['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
        elif 'DELIV_QTY' in df.columns:
            delivery_qty = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
            result['DELIV_PER'] = (delivery_qty / result['TTL_TRD_QNTY']) * 100
            result['DELIV_PER'] = result['DELIV_PER'].fillna(0)
        else:
            result['DELIV_PER'] = 0
        result['DATE1'] = trade_date
        result = result[result['SERIES'] == 'EQ']
        result = result[result['SYMBOL'].isin(NIFTY_SYMBOLS_SET)]
        result = result[result['CLOSE_PRICE'].notna()]
        result = result[result['TTL_TRD_QNTY'].notna()]
        return result[['SYMBOL', 'DATE1', 'CLOSE_PRICE', 'TTL_TRD_QNTY', 'DELIV_PER']]
    except Exception:
        return None

# ============================================
# GET LAST 35 TRADING DATES
# ============================================

def get_last_n_trading_dates(n=35):
    from nselib import trading_holiday_calendar
    holidays_df = trading_holiday_calendar()
    holidays = set()
    if holidays_df is not None and len(holidays_df) > 0:
        date_col = 'TRADING_DATE' if 'TRADING_DATE' in holidays_df.columns else 'DATE'
        if date_col in holidays_df.columns:
            holidays = set(pd.to_datetime(holidays_df[date_col]).dt.date)
    trading_dates = []
    current_date = datetime.now().date()
    days_checked = 0
    while len(trading_dates) < n and days_checked < 70:
        days_checked += 1
        check_date = current_date - timedelta(days=days_checked)
        if check_date.weekday() >= 5:
            continue
        if check_date in holidays:
            continue
        trading_dates.append(check_date)
    return sorted(trading_dates, reverse=True)

# ============================================
# BUILD DATABASE (35 DAYS)
# ============================================

def build_database():
    print("\n" + "=" * 80)
    print("BUILDING NIFTY 750 DATABASE (FAST MODE)")
    print("=" * 80)
    print(f"📌 Target: {len(NIFTY_SYMBOLS)} NIFTY 750 stocks")
    print(f"📌 Period: Last 35 trading days")
    trading_dates = get_last_n_trading_dates(35)
    print(f"\n📅 Trading dates to fetch: {len(trading_dates)}")
    all_data = []
    success_dates = 0
    for i, trade_date in enumerate(trading_dates, 1):
        print(f"\r[{i}/{len(trading_dates)}] Fetching {trade_date.strftime('%Y-%m-%d')}...", end=" ")
        df = fetch_bhavcopy_for_date(trade_date)
        if df is not None and len(df) > 0:
            all_data.append(df)
            success_dates += 1
            print(f"✓ {len(df)} stocks", end="")
        else:
            print(f"✗ No data", end="")
    print()
    if not all_data:
        print("\n❌ No data fetched")
        return None
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(['DATE1', 'SYMBOL'])
    combined_df = combined_df.drop_duplicates(subset=['SYMBOL', 'DATE1'])
    print("\n" + "=" * 80)
    print("DATABASE BUILD COMPLETE")
    print("=" * 80)
    print(f"   Total rows: {len(combined_df):,}")
    print(f"   Unique symbols: {combined_df['SYMBOL'].nunique()}")
    print(f"   Date range: {combined_df['DATE1'].min()} to {combined_df['DATE1'].max()}")
    return combined_df

# ============================================
# RUN ACCUMULATION SCREENER
# ============================================

def screen_stocks(df):
    print("\n" + "=" * 80)
    print("RUNNING ACCUMULATION SCREENER")
    print("=" * 80)
    print(f"FILTERS:")
    print(f"   1. 20-Day Price Change: ±{PRICE_CHANGE_LIMIT}%")
    print(f"   2. Volume Surge: Last {VOLUME_LOOKBACK_DAYS}d > {VOLUME_SURGE_MIN}x of previous 28d")
    print(f"   3. Delivery: Current > 50% AND Last 5d avg > Previous 5d avg")
    results = []
    symbols = df['SYMBOL'].unique()
    print(f"\nScreening {len(symbols)} stocks...")
    for i, symbol in enumerate(symbols):
        if (i+1) % 100 == 0:
            print(f"   Progress: {i+1}/{len(symbols)}")
        stock = df[df['SYMBOL'] == symbol].copy().sort_values('DATE1')
        if len(stock) < 35:
            continue
        if len(stock) < 21:
            continue
        price_20d_ago = stock.iloc[-21]['CLOSE_PRICE']
        current_price = stock.iloc[-1]['CLOSE_PRICE']
        price_change = ((current_price - price_20d_ago) / price_20d_ago) * 100
        if abs(price_change) > PRICE_CHANGE_LIMIT:
            continue
        last_7_vol = stock['TTL_TRD_QNTY'].iloc[-VOLUME_LOOKBACK_DAYS:].mean()
        prev_volumes = []
        for s in range(1, VOLUME_PREV_SETS + 1):
            start = -(VOLUME_LOOKBACK_DAYS + (s * VOLUME_LOOKBACK_DAYS))
            end = -(s * VOLUME_LOOKBACK_DAYS)
            if end == 0:
                end = None
            avg = stock['TTL_TRD_QNTY'].iloc[start:end].mean()
            prev_volumes.append(avg)
        prev_28_avg = np.mean(prev_volumes)
        vol_surge = last_7_vol / prev_28_avg if prev_28_avg > 0 else 1
        if vol_surge <= VOLUME_SURGE_MIN:
            continue
        current_delivery = stock.iloc[-1]['DELIV_PER']
        if current_delivery <= 50:
            continue
        if len(stock) < 10:
            continue
        last_5_delivery = stock['DELIV_PER'].iloc[-5:].mean()
        prev_5_delivery = stock['DELIV_PER'].iloc[-10:-5].mean()
        if last_5_delivery <= prev_5_delivery:
            continue
        results.append({
            'SYMBOL': symbol,
            'DATE': stock.iloc[-1]['DATE1'],
            'LTP': round(current_price, 2),
            'PRICE_CHANGE_PCT': round(price_change, 2),
            'VOLUME_SURGE': round(vol_surge, 2),
            'CURRENT_DELIVERY': round(current_delivery, 2),
            'LAST_5_DELIVERY': round(last_5_delivery, 2),
            'PREV_5_DELIVERY': round(prev_5_delivery, 2),
            'LAST_7_VOL': int(last_7_vol),
            'PREV_28_VOL': int(prev_28_avg),
        })
    results_df = pd.DataFrame(results)
    if len(results_df) > 0:
        results_df = results_df.sort_values('VOLUME_SURGE', ascending=False)
    return results_df

# ============================================
# TEXT CHART HELPER
# ============================================

def make_volume_bar(surge_value, max_bar_length=10):
    """Create a simple text bar chart for volume surge"""
    if surge_value >= 3.0:
        filled = max_bar_length
    else:
        filled = int((surge_value - 1.0) / 2.0 * max_bar_length)
        filled = max(0, min(filled, max_bar_length))
    bar = "█" * filled + "░" * (max_bar_length - filled)
    return bar

# ============================================
# TELEGRAM ALERT (TEXT ONLY, NO CSV)
# ============================================

def send_telegram_alert(results_df):
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_ids_str = os.environ.get('TELEGRAM_CHAT_IDS')
    if not bot_token or not chat_ids_str:
        print("Telegram credentials not set. Skipping alert.")
        return
    chat_ids = [cid.strip() for cid in chat_ids_str.split(',')]
    
    if results_df is None or len(results_df) == 0:
        message = "📊 *NIFTY 750 Accumulation Screener*\n\n✅ No stocks passed the filters today."
    else:
        top_stocks = results_df.head(TOP_STOCKS)
        lines = [
            "📊 *NIFTY 750 ACCUMULATION SCREENER*",
            f"📅 {top_stocks.iloc[0]['DATE'].strftime('%d-%b-%Y')}",
            f"🎯 Volume Surge > {VOLUME_SURGE_MIN}x | Price ±{PRICE_CHANGE_LIMIT}% | Delivery >50% & rising",
            "",
            "*🔥 TOP CANDIDATES*",
            ""
        ]
        for idx, row in top_stocks.iterrows():
            bar = make_volume_bar(row['VOLUME_SURGE'])
            lines.append(f"*{row['SYMBOL']}*  ₹{row['LTP']:.2f}")
            lines.append(f"   Price: {row['PRICE_CHANGE_PCT']:+.2f}%")
            lines.append(f"   Volume: {bar}  {row['VOLUME_SURGE']:.2f}x")
            lines.append(f"   Delivery: {row['CURRENT_DELIVERY']}%  (5d avg: {row['LAST_5_DELIVERY']}% ↑ from {row['PREV_5_DELIVERY']}%)")
            lines.append("")
        lines.append(f"✅ Found *{len(results_df)}* stocks. Top {TOP_STOCKS} shown.")
        message = "\n".join(lines)
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {'parse_mode': 'Markdown'}
    for chat_id in chat_ids:
        payload['chat_id'] = chat_id
        payload['text'] = message
        try:
            resp = requests.post(url, json=payload)
            resp.raise_for_status()
            print(f"✅ Telegram alert sent to {chat_id}")
        except Exception as e:
            print(f"❌ Failed to send to {chat_id}: {e}")

# ============================================
# MAIN EXECUTION
# ============================================

def main():
    print("\n" + "=" * 80)
    print("🚀 NIFTY 750 ACCUMULATION SCREENER - TEXT ALERT MODE")
    print("=" * 80)
    print("⚡ 35 days data | Volume Surge > 1.4x | Telegram text chart")
    df = build_database()
    if df is None or len(df) == 0:
        print("\n❌ Failed to build database.")
        return
    results = screen_stocks(df)
    # No CSV saving, only Telegram alert
    send_telegram_alert(results)
    print("\n" + "=" * 80)
    print("✅ PROCESS COMPLETE - Alert sent to Telegram")
    print("=" * 80)

if __name__ == "__main__":
    main()
