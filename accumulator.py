"""
NIFTY 750 ACCUMULATION SCREENER - FAST VERSION
===============================================
- Direct NSE bhavcopy fetch using nselib
- 35 days of data only (faster execution)
- No GDrive mounting - saves directly to Colab temp storage
- Output CSV downloads automatically

FILTERS:
1. Price change over 20 days: between -3% and +3%
2. Volume Surge: Last 7 days avg > 1.50x of previous 28 days (4 sets of 7)
3. Delivery: Current > 50% AND Last 5-day avg > Previous 5-day avg
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

OUTPUT_FILE = "accumulation_signals.csv"
TEMP_DB_FILE = "nifty750_temp_data.csv"

# Screener parameters
PRICE_CHANGE_LIMIT = 3.0      # ±3%
VOLUME_SURGE_MIN = 1.50       # 1.5x
VOLUME_LOOKBACK_DAYS = 7      # Last 7 days
VOLUME_PREV_SETS = 4          # 4 sets of 7 days = 28 days (instead of 35)
TOP_STOCKS = 5

# ============================================
# NIFTY 750 SYMBOLS (hardcoded from Excel)
# ============================================

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
    """
    Fetch bhavcopy with delivery data for a specific date
    Returns DataFrame with columns: SYMBOL, SERIES, CLOSE_PRICE, TTL_TRD_QNTY, DELIV_PER, DATE1
    """
    try:
        date_str = trade_date.strftime('%d-%m-%Y')
        df = capital_market.bhav_copy_with_delivery(trade_date=date_str)

        if df is None or len(df) == 0:
            return None

        # Standardize column names
        df.columns = df.columns.str.upper().str.strip()

        # Map expected columns
        symbol_col = 'SYMBOL' if 'SYMBOL' in df.columns else 'SYMBOL1'
        series_col = 'SERIES' if 'SERIES' in df.columns else 'SERIES1'
        close_col = 'CLOSE' if 'CLOSE' in df.columns else 'CLOSE_PRICE'
        volume_col = 'TTL_TRD_QNTY' if 'TTL_TRD_QNTY' in df.columns else 'TOTTRDQTY'

        # Create standardized dataframe
        result = pd.DataFrame()
        result['SYMBOL'] = df[symbol_col].astype(str).str.strip().str.upper()
        result['SERIES'] = df[series_col].astype(str).str.strip().str.upper()
        result['CLOSE_PRICE'] = pd.to_numeric(df[close_col], errors='coerce')
        result['TTL_TRD_QNTY'] = pd.to_numeric(df[volume_col], errors='coerce')

        # Handle delivery percentage
        if 'DELIV_PER' in df.columns:
            result['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
        elif 'DELIV_QTY' in df.columns:
            delivery_qty = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
            result['DELIV_PER'] = (delivery_qty / result['TTL_TRD_QNTY']) * 100
            result['DELIV_PER'] = result['DELIV_PER'].fillna(0)
        else:
            result['DELIV_PER'] = 0

        result['DATE1'] = trade_date

        # Filter EQ series only
        result = result[result['SERIES'] == 'EQ']

        # Filter NIFTY 750 symbols only
        result = result[result['SYMBOL'].isin(NIFTY_SYMBOLS_SET)]

        # Remove rows with missing critical data
        result = result[result['CLOSE_PRICE'].notna()]
        result = result[result['TTL_TRD_QNTY'].notna()]

        return result[['SYMBOL', 'DATE1', 'CLOSE_PRICE', 'TTL_TRD_QNTY', 'DELIV_PER']]

    except Exception as e:
        # Silently skip errors (holidays, missing data)
        return None

# ============================================
# GET LAST 35 TRADING DATES
# ============================================

def get_last_n_trading_dates(n=35):
    """
    Get last n trading dates by checking bhavcopy availability
    """
    from nselib import trading_holiday_calendar

    # Get trading holidays
    holidays_df = trading_holiday_calendar()

    holidays = set()
    if holidays_df is not None and len(holidays_df) > 0:
        date_col = 'TRADING_DATE' if 'TRADING_DATE' in holidays_df.columns else 'DATE'
        if date_col in holidays_df.columns:
            holidays = set(pd.to_datetime(holidays_df[date_col]).dt.date)

    trading_dates = []
    current_date = datetime.now().date()

    # Go back up to 70 days to find 35 trading days
    days_checked = 0
    while len(trading_dates) < n and days_checked < 70:
        days_checked += 1
        check_date = current_date - timedelta(days=days_checked)

        # Skip weekends
        if check_date.weekday() >= 5:
            continue

        # Skip holidays
        if check_date in holidays:
            continue

        trading_dates.append(check_date)

    return sorted(trading_dates, reverse=True)

# ============================================
# BUILD DATABASE (35 DAYS)
# ============================================

def build_database():
    """Fetch bhavcopy for last 35 trading days and build database"""

    print("\n" + "=" * 80)
    print("BUILDING NIFTY 750 DATABASE (FAST MODE)")
    print("=" * 80)
    print(f"📌 Target: {len(NIFTY_SYMBOLS)} NIFTY 750 stocks")
    print(f"📌 Period: Last 35 trading days (approx 7 weeks)")
    print("=" * 80)

    # Get trading dates
    trading_dates = get_last_n_trading_dates(35)
    print(f"\n📅 Trading dates to fetch: {len(trading_dates)}")
    print(f"   Range: {trading_dates[-1]} to {trading_dates[0]}")

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

    print()  # New line after loop

    if not all_data:
        print("\n❌ No data fetched from any date")
        return None

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Sort by date and symbol
    combined_df = combined_df.sort_values(['DATE1', 'SYMBOL'])

    # Remove duplicates
    before = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['SYMBOL', 'DATE1'])

    print("\n" + "=" * 80)
    print("DATABASE BUILD COMPLETE")
    print("=" * 80)
    print(f"   Total rows: {len(combined_df):,}")
    print(f"   Unique symbols: {combined_df['SYMBOL'].nunique()}")
    print(f"   Date range: {combined_df['DATE1'].min()} to {combined_df['DATE1'].max()}")
    print(f"   Successful dates: {success_dates}/{len(trading_dates)}")

    return combined_df

# ============================================
# RUN ACCUMULATION SCREENER
# ============================================

def screen_stocks(df):
    """Apply accumulation filters"""

    print("\n" + "=" * 80)
    print("RUNNING ACCUMULATION SCREENER")
    print("=" * 80)
    print(f"FILTERS:")
    print(f"   1. 20-Day Price Change: ±{PRICE_CHANGE_LIMIT}%")
    print(f"   2. Volume Surge: Last {VOLUME_LOOKBACK_DAYS}d > {VOLUME_SURGE_MIN}x of previous 28d")
    print(f"   3. Delivery: Current > 50% AND Last 5d avg > Previous 5d avg")
    print("-" * 80)

    results = []
    symbols = df['SYMBOL'].unique()
    print(f"\nScreening {len(symbols)} stocks...")

    for i, symbol in enumerate(symbols):
        if (i + 1) % 100 == 0:
            print(f"   Progress: {i+1}/{len(symbols)}")

        stock = df[df['SYMBOL'] == symbol].copy()
        stock = stock.sort_values('DATE1')

        # Need at least 35 days of data (reduced from 60)
        if len(stock) < 35:
            continue

        # FILTER 1: PRICE CHANGE ±3% OVER 20 DAYS
        # Need at least 21 rows for 20-day lookback
        if len(stock) < 21:
            continue

        price_20d_ago = stock.iloc[-21]['CLOSE_PRICE']
        current_price = stock.iloc[-1]['CLOSE_PRICE']
        price_change = ((current_price - price_20d_ago) / price_20d_ago) * 100

        if abs(price_change) > PRICE_CHANGE_LIMIT:
            continue

        # FILTER 2: VOLUME SURGE (> 1.50x)
        # Need enough data for volume calculation
        if len(stock) < 35:
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

        # FILTER 3: DELIVERY > 50% AND INCREASING TREND
        current_delivery = stock.iloc[-1]['DELIV_PER']

        if current_delivery <= 50:
            continue

        # Need at least 10 days for 5-day averages
        if len(stock) < 10:
            continue

        last_5_delivery = stock['DELIV_PER'].iloc[-5:].mean()
        prev_5_delivery = stock['DELIV_PER'].iloc[-10:-5].mean()

        if last_5_delivery <= prev_5_delivery:
            continue

        # STOCK PASSED ALL FILTERS
        latest = stock.iloc[-1]

        results.append({
            'SYMBOL': symbol,
            'DATE': latest['DATE1'],
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
# TELEGRAM ALERT FUNCTION
# ============================================

def send_telegram_alert(results_df):
    """
    Send accumulation screener results to a Telegram group.
    Uses TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from environment variables.
    """
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')

    if not bot_token or not chat_id:
        print("Telegram credentials not set. Skipping alert.")
        return

    if results_df is None or len(results_df) == 0:
        message = "📊 *NIFTY 750 Accumulation Screener*\n\n✅ No stocks passed the filters today."
    else:
        # Create a formatted message with top results
        top_stocks = results_df.head(5)
        message_lines = [
            "📊 *NIFTY 750 Accumulation Screener*",
            f"📅 Date: {top_stocks.iloc[0]['DATE'].strftime('%Y-%m-%d')}",
            "",
            "*🔥 Top Accumulation Candidates:*",
            ""
        ]

        for idx, row in top_stocks.iterrows():
            stock_line = (
                f"*{row['SYMBOL']}* (LTP: ₹{row['LTP']:.2f})\n"
                f"└ Price Change: {row['PRICE_CHANGE_PCT']:+.2f}% | "
                f"Vol Surge: {row['VOLUME_SURGE']:.2f}x | "
                f"Delivery: {row['CURRENT_DELIVERY']}%"
            )
            message_lines.append(stock_line)

        message_lines.append(f"\n✅ Found *{len(results_df)}* stocks. Top 5 shown.")
        message = "\n".join(message_lines)

    # Send the message via Telegram API
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print("✅ Telegram alert sent successfully!")
    except Exception as e:
        print(f"❌ Failed to send Telegram alert: {e}")

# ============================================
# DISPLAY RESULTS
# ============================================

def display_results(df):
    """Display top accumulation candidates"""

    if df is None or len(df) == 0:
        print("\n" + "=" * 100)
        print("❌ NO STOCKS FOUND")
        print("=" * 100)
        print("\nTry adjusting filters:")
        print(f"   - Increase PRICE_CHANGE_LIMIT (currently ±{PRICE_CHANGE_LIMIT}%)")
        print(f"   - Decrease VOLUME_SURGE_MIN (currently {VOLUME_SURGE_MIN}x)")
        print(f"   - Lower delivery threshold (currently >50%)")
        return

    top = df.head(TOP_STOCKS)

    print("\n" + "=" * 130)
    print(f"🔥 TOP {TOP_STOCKS} ACCUMULATION CANDIDATES")
    print("=" * 130)

    print(f"\n{'NO':<4} {'DATE':<12} {'STOCK':<14} {'LTP':<10} {'PRICE CHG%':<12} {'VOL SURGE':<12} {'DEL%':<8} {'DEL(5d)':<10} {'DEL(prev)':<10}")
    print("-" * 130)

    for idx, row in top.iterrows():
        date_str = row['DATE'].strftime('%d-%b-%Y')
        vol_str = f"{row['VOLUME_SURGE']:.2f}x"
        if row['VOLUME_SURGE'] >= 2.0:
            vol_str = f"🔥 {vol_str}"

        print(f"{idx+1:<4} {date_str:<12} {row['SYMBOL']:<14} {row['LTP']:<10.2f} {row['PRICE_CHANGE_PCT']:+.2f}%{'':<6} {vol_str:<12} {row['CURRENT_DELIVERY']:<8} {row['LAST_5_DELIVERY']:<10} {row['PREV_5_DELIVERY']:<10}")

    print("-" * 130)
    print("\n🔥 = Volume Surge > 2.0x (Strong accumulation)")
    print("DEL(5d) = Last 5 days average delivery")
    print("DEL(prev) = Previous 5 days average delivery")

    # Detailed view
    print("\n" + "=" * 130)
    print("📊 DETAILED VIEW")
    print("=" * 130)

    for idx, row in top.iterrows():
        print(f"\n{idx+1}. {row['SYMBOL']}")
        print(f"   📅 Date: {row['DATE'].strftime('%d-%b-%Y')}")
        print(f"   💰 LTP: Rs.{row['LTP']:.2f} | 20-day Change: {row['PRICE_CHANGE_PCT']:+.2f}%")
        print(f"   📈 Volume: Last 7d: {row['LAST_7_VOL']:,} | Prev 28d: {row['PREV_28_VOL']:,} | Surge: {row['VOLUME_SURGE']:.2f}x")
        print(f"   📦 Delivery: Current: {row['CURRENT_DELIVERY']}% | Last 5d: {row['LAST_5_DELIVERY']}% | Prev 5d: {row['PREV_5_DELIVERY']}%")

    print("\n" + "=" * 130)
    print(f"✅ Found {len(df)} stocks passing all filters. Top {TOP_STOCKS} shown.")
    print("=" * 130)

# ============================================
# SAVE RESULTS (NO DOWNLOAD FOR AUTOMATION)
# ============================================

def save_results(df):
    """Save results to CSV (no automatic download for GitHub Actions)"""
    if df is None or len(df) == 0:
        return False

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n💾 Results saved: {OUTPUT_FILE}")
    file_size = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"   File size: {file_size:.2f} KB")
    return True

# ============================================
# MAIN EXECUTION
# ============================================

def main():
    print("\n" + "=" * 80)
    print("🚀 NIFTY 750 ACCUMULATION SCREENER - FAST MODE")
    print("=" * 80)
    print("⚡ 35 days of data | Direct NSE fetch | Telegram Alert")
    print("=" * 80)

    # Step 1: Build database from bhavcopy (35 days)
    df = build_database()

    if df is None or len(df) == 0:
        print("\n❌ Failed to build database. Exiting.")
        return

    # Step 2: Run screener
    results = screen_stocks(df)

    # Step 3: Display results
    display_results(results)

    # Step 4: Save results CSV
    if results is not None and len(results) > 0:
        save_results(results)
    else:
        print("\n⚠️ No stocks passed the filters. No output file created.")

    # Step 5: Send Telegram alert
    send_telegram_alert(results)

    print("\n" + "=" * 80)
    print("✅ PROCESS COMPLETE")
    print("=" * 80)

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    main()
