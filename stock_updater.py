"""
Automated Stock Data Updater for Supabase
Downloads NASDAQ and NYSE data from FTP, cleans it, and updates Supabase weekly
"""

import ftplib
import pandas as pd
import io
from datetime import datetime
import os
from supabase import create_client, Client
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============= CONFIGURATION =============
# Load from environment variables (from .env file)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Validate environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Missing environment variables!")
    logger.error("Please create a .env file with:")
    logger.error("SUPABASE_URL=your-supabase-url")
    logger.error("SUPABASE_KEY=your-supabase-key")
    raise ValueError("Missing required environment variables: SUPABASE_URL and SUPABASE_KEY")

logger.info(f"Loaded Supabase URL: {SUPABASE_URL[:30]}...")

STOCK_TICKERS_TABLE = "stock_tickers"  # Reference table for valid symbols
STOCKS_TABLE = "stocks"  # Live price data table

FTP_HOST = "ftp.nasdaqtrader.com"
FTP_USER = "anonymous"
FTP_PASS = "anonymous"
FTP_DIRECTORY = "/SymbolDirectory/"
NASDAQ_FILE = "nasdaqlisted.txt"
OTHER_FILE = "otherlisted.txt"

# ============= DATA CLEANING FUNCTIONS =============

def is_derivative(row):
    """Check if a stock is a derivative security"""
    symbol = str(row.get('Symbol', ''))
    
    if symbol == 'nan' or symbol == '' or pd.isna(row.get('Symbol')):
        return True
    
    security_name_raw = row.get('Security Name', row.get('Company Name', ''))
    
    if pd.isna(security_name_raw):
        security_name = ''
    else:
        security_name = str(security_name_raw).lower()
    
    # Multi-character symbols ending in U, R, W (likely derivatives)
    if len(symbol) > 3 and symbol[-1] in ['U', 'R', 'W']:
        return True
    
    # Security name keywords
    derivative_keywords = [
        '- unit', '- right', '- warrant',
        ' units', ' rights', ' warrants',
        'depositary', 'adr', 'adrs',
        'each representing'
    ]
    
    return any(keyword in security_name for keyword in derivative_keywords)


def clean_company_name(security_name):
    """
    Extract clean company name from security name
    Examples:
      "Apple Inc. Common Stock" -> "Apple Inc."
      "Microsoft Corporation - Class A Common Stock" -> "Microsoft Corporation"
      "Berkshire Hathaway Inc. Class B" -> "Berkshire Hathaway Inc."
      "Artius II Acquisition Inc. - Class A Ordinary Shares" -> "Artius II Acquisition Inc."
    """
    if not security_name or pd.isna(security_name):
        return ''
    
    name = str(security_name).strip()
    
    # Remove everything after and including these patterns
    patterns_to_remove = [
        ' - Common Stock',
        ' - Class A Common Stock',
        ' - Class B Common Stock',
        ' - Class C Common Stock',
        ' - Common Shares',
        ' - Ordinary Shares',
        ' - Class A Ordinary Shares',
        ' - Class B Ordinary Shares',
        ' - Class C Ordinary Shares',
        ' Common Stock',
        ' Common Shares',
        ' Ordinary Shares',
        ' Class A Common Stock',
        ' Class B Common Stock',
        ' Class C Common Stock',
        ' Class A Common Shares',
        ' Class B Common Shares',
        ' Class C Common Shares',
        ' Class A Ordinary Shares',
        ' Class B Ordinary Shares',
        ' Class C Ordinary Shares',
        ', Common Stock',
        ' - Class A',
        ' - Class B',
        ' - Class C',
        ' Class A',
        ' Class B', 
        ' Class C',
        ' -',  # Remove trailing hyphen
    ]
    
    # Apply each pattern
    for pattern in patterns_to_remove:
        if pattern in name:
            # Split by pattern and take first part
            name = name.split(pattern)[0].strip()
    
    # Remove any trailing commas, hyphens, or extra spaces
    name = name.rstrip(',-').strip()
    
    return name


def clean_stock_data(df, exchange_name):
    """Clean stock data by removing ETFs, derivatives, and test issues"""
    
    # Debug: Print columns and their types
    logger.info(f"Cleaning {exchange_name} data with columns: {list(df.columns)}")
    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"Column dtypes: {df.dtypes.to_dict()}")
    
    # Check for duplicate column names
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        logger.warning(f"Duplicate columns found: {duplicate_cols}")
        # Remove duplicate columns, keep first
        df = df.loc[:, ~df.columns.duplicated()]
        logger.info(f"After removing duplicates: {list(df.columns)}")
    
    # Ensure 'Symbol' column exists
    if 'Symbol' not in df.columns:
        logger.error(f"'Symbol' column not found. Available columns: {list(df.columns)}")
        raise ValueError(f"'Symbol' column missing in {exchange_name} data")
    
    # Debug: Check what df['Symbol'] returns
    symbol_col = df['Symbol']
    logger.info(f"Symbol column type: {type(symbol_col)}")
    logger.info(f"Symbol column shape: {symbol_col.shape if hasattr(symbol_col, 'shape') else 'N/A'}")
    
    # Remove rows with empty or NaN symbols first
    df = df.dropna(subset=['Symbol']).copy()
    
    # Fix: Ensure we're working with a Series
    if isinstance(df['Symbol'], pd.DataFrame):
        logger.error("df['Symbol'] is a DataFrame, not a Series!")
        logger.error(f"Columns in df['Symbol']: {df['Symbol'].columns.tolist()}")
        # Take the first column if it's a DataFrame
        symbol_col = df['Symbol'].iloc[:, 0]
    else:
        symbol_col = df['Symbol']
    
    # Now apply string operations
    symbol_series = symbol_col.astype(str).str.strip()
    df = df[symbol_series != ''].copy()
    
    # Standardize column names
    if 'Company Name' in df.columns and 'Security Name' not in df.columns:
        df['Security Name'] = df['Company Name']
    
    # Add exchange column
    df['exchange'] = exchange_name
    
    original_count = len(df)
    stats = {}
    
    # Remove ETFs (only if column exists)
    if 'ETF' in df.columns:
        df_clean = df[df['ETF'] != 'Y'].copy()
        stats['ETFs removed'] = original_count - len(df_clean)
    else:
        df_clean = df.copy()
        if 'Security Name' in df_clean.columns:
            before = len(df_clean)
            df_clean = df_clean[~df_clean['Security Name'].str.contains('ETF', case=False, na=False)]
            stats['ETFs removed'] = before - len(df_clean)
        else:
            stats['ETFs removed'] = 0
    
    # Remove test issues
    if 'Test Issue' in df_clean.columns:
        before = len(df_clean)
        df_clean = df_clean[df_clean['Test Issue'] != 'Y']
        stats['Test issues removed'] = before - len(df_clean)
    else:
        stats['Test issues removed'] = 0
    
    # Remove NextShares
    if 'NextShares' in df_clean.columns:
        before = len(df_clean)
        df_clean = df_clean[df_clean['NextShares'] != 'Y']
        stats['NextShares removed'] = before - len(df_clean)
    else:
        stats['NextShares removed'] = 0
    
    # Remove derivatives
    before = len(df_clean)
    df_clean = df_clean[~df_clean.apply(is_derivative, axis=1)].copy()
    stats['Derivatives removed'] = before - len(df_clean)
    
    # Keep only common stocks
    if 'Security Name' in df_clean.columns:
        before = len(df_clean)
        # Fix: Use proper string contains
        mask = df_clean['Security Name'].astype(str).str.contains(
            'Common Stock|Ordinary Shares|Class A Ordinary|Common Shares', 
            case=False, 
            na=False,
            regex=True
        )
        df_clean = df_clean[mask]
        stats['Non-common stocks removed'] = before - len(df_clean)
    else:
        stats['Non-common stocks removed'] = 0
    
    # Remove preferred stocks
    before = len(df_clean)
    df_clean = df_clean[~df_clean['Symbol'].astype(str).str.contains(r'[\$\^]', na=False, regex=True)]
    if before - len(df_clean) > 0:
        stats['Preferred stocks removed'] = before - len(df_clean)
    
    # Remove duplicates
    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['Symbol'], keep='first')
    stats['Duplicates removed'] = before - len(df_clean)
    
    return df_clean, stats, original_count


# ============= FTP DOWNLOAD FUNCTIONS =============

def download_ftp_file(ftp_host, ftp_user, ftp_pass, ftp_dir, filename):
    """Download file from FTP server"""
    logger.info(f"Connecting to FTP server: {ftp_host}")
    
    try:
        ftp = ftplib.FTP(ftp_host)
        ftp.login(ftp_user, ftp_pass)
        ftp.cwd(ftp_dir)
        
        logger.info(f"Downloading {filename}...")
        
        data = io.BytesIO()
        ftp.retrbinary(f'RETR {filename}', data.write)
        ftp.quit()
        
        data.seek(0)
        content = data.read().decode('utf-8')
        
        logger.info(f"Successfully downloaded {filename}")
        return content
        
    except Exception as e:
        logger.error(f"Error downloading {filename}: {str(e)}")
        raise


def parse_nasdaq_file(content):
    """Parse NASDAQ pipe-delimited file"""
    lines = content.strip().split('\n')
    
    # Find header line (skip file header comments)
    header_idx = 0
    for i, line in enumerate(lines):
        if 'Symbol|' in line or 'ACT Symbol|' in line or 'NASDAQ Symbol|' in line:
            header_idx = i
            break
    
    # Parse data
    data = []
    headers = lines[header_idx].strip().split('|')
    
    # Debug: print headers
    logger.info(f"Raw file headers: {headers}")
    logger.info(f"Number of headers: {len(headers)}")
    
    for line in lines[header_idx + 1:]:
        if line.strip() and not line.startswith('File'):
            values = line.strip().split('|')
            if len(values) == len(headers):
                data.append(values)
            else:
                logger.warning(f"Skipping line with {len(values)} values (expected {len(headers)})")
    
    df = pd.DataFrame(data, columns=headers)
    
    logger.info(f"Created DataFrame with shape: {df.shape}")
    logger.info(f"DataFrame columns: {list(df.columns)}")
    
    # Check for duplicate columns before renaming
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        logger.warning(f"Duplicate columns in parsed data: {duplicate_cols}")
    
    # Standardize column names - PRIORITY ORDER:
    # 1. Use NASDAQ Symbol (most standardized for APIs)
    # 2. Fall back to Symbol (for nasdaqlisted.txt)
    # 3. Fall back to ACT Symbol (legacy)
    
    if 'NASDAQ Symbol' in df.columns:
        # For otherlisted.txt - use NASDAQ Symbol as primary
        if 'Symbol' not in df.columns:
            df['Symbol'] = df['NASDAQ Symbol']
            logger.info("Using 'NASDAQ Symbol' as primary Symbol column")
        else:
            # If both exist, NASDAQ Symbol takes priority
            df['Symbol'] = df['NASDAQ Symbol']
            logger.info("NASDAQ Symbol takes priority over existing Symbol column")
    elif 'ACT Symbol' in df.columns and 'Symbol' not in df.columns:
        # Fall back to ACT Symbol if NASDAQ Symbol doesn't exist
        df['Symbol'] = df['ACT Symbol']
        logger.info("Using 'ACT Symbol' as Symbol (fallback)")
    
    # Keep original symbols as backup columns for reference
    if 'ACT Symbol' in df.columns and 'ACT Symbol' != 'Symbol':
        df['act_symbol_backup'] = df['ACT Symbol']
    if 'CQS Symbol' in df.columns:
        df['cqs_symbol_backup'] = df['CQS Symbol']
    
    logger.info(f"Final columns after standardization: {list(df.columns)}")
    logger.info(f"Parsed {len(df)} rows")
    
    return df


# ============= MAIN PROCESSING FUNCTIONS =============

def diagnose_file_structure(content, filename):
    """Diagnose file structure for debugging"""
    logger.info(f"\n{'='*60}")
    logger.info(f"DIAGNOSING {filename}")
    logger.info(f"{'='*60}")
    
    lines = content.strip().split('\n')
    logger.info(f"Total lines: {len(lines)}")
    logger.info(f"\nFirst 10 lines:")
    for i, line in enumerate(lines[:10]):
        logger.info(f"{i}: {line[:100]}...")
    
    # Find header
    for i, line in enumerate(lines):
        if '|' in line and any(keyword in line.lower() for keyword in ['symbol', 'name', 'security']):
            logger.info(f"\nPotential header at line {i}:")
            logger.info(f"{line}")
            logger.info(f"\nColumns: {line.split('|')}")
            break


def fetch_and_clean_nasdaq_data():
    """Fetch and clean NASDAQ listed stocks"""
    logger.info("=" * 50)
    logger.info("Processing NASDAQ data...")
    
    content = download_ftp_file(FTP_HOST, FTP_USER, FTP_PASS, FTP_DIRECTORY, NASDAQ_FILE)
    
    # Diagnose file structure
    diagnose_file_structure(content, NASDAQ_FILE)
    
    df = parse_nasdaq_file(content)
    
    logger.info(f"Downloaded {len(df)} NASDAQ records")
    
    # Clean the data
    df_clean, stats, original = clean_stock_data(df, 'NASDAQ')
    
    logger.info(f"NASDAQ: {original} → {len(df_clean)} stocks")
    for key, value in stats.items():
        if value > 0:
            logger.info(f"  - {key}: {value}")
    
    return df_clean


def fetch_and_clean_nyse_data():
    """Fetch and clean NYSE stocks from otherlisted.txt"""
    logger.info("=" * 50)
    logger.info("Processing NYSE data...")
    
    content = download_ftp_file(FTP_HOST, FTP_USER, FTP_PASS, FTP_DIRECTORY, OTHER_FILE)
    
    # Diagnose file structure
    diagnose_file_structure(content, OTHER_FILE)
    
    df = parse_nasdaq_file(content)
    
    # Filter only NYSE stocks (Exchange column should be 'N' for NYSE)
    if 'Exchange' in df.columns:
        logger.info(f"Before NYSE filter: {len(df)} records")
        df = df[df['Exchange'] == 'N'].copy()
        logger.info(f"After NYSE filter: {len(df)} records")
    else:
        logger.warning(f"'Exchange' column not found. Available columns: {list(df.columns)}")
        logger.warning("Proceeding without exchange filtering...")
    
    # Clean the data
    df_clean, stats, original = clean_stock_data(df, 'NYSE')
    
    logger.info(f"NYSE: {original} → {len(df_clean)} stocks")
    for key, value in stats.items():
        if value > 0:
            logger.info(f"  - {key}: {value}")
    
    return df_clean


def update_supabase_stocks(df):
    """Update Supabase stock_tickers table"""
    logger.info("=" * 50)
    logger.info("Updating Supabase stock_tickers table...")
    
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Delete all existing records from stock_tickers
        logger.info("Deleting old ticker records...")
        supabase.table(STOCK_TICKERS_TABLE).delete().neq('Symbol', '').execute()
        
        # Replace all NaN values with None/empty strings
        df = df.fillna({
            'Symbol': '',
            'Security Name': '',
            'Company Name': '',
            'Market Category': '',
            'Test Issue': 'N',
            'Financial Status': '',
            'Round Lot Size': 100,
            'ETF': 'N',
            'NextShares': 'N',
            'exchange': 'NASDAQ'
        })
        
        # Prepare data for insertion - map to stock_tickers schema
        records = []
        for _, row in df.iterrows():
            # Get values and handle any remaining NaN
            symbol = str(row.get('Symbol', ''))
            if symbol == 'nan' or symbol == '':
                continue  # Skip invalid symbols
            
            # Convert Round Lot Size safely
            try:
                round_lot = row.get('Round Lot Size', 100)
                if pd.isna(round_lot) or round_lot == '':
                    round_lot = 100
                else:
                    round_lot = int(float(round_lot))
            except (ValueError, TypeError):
                round_lot = 100
            
            # Get security name and clean it for company name
            security_name = str(row.get('Security Name', '')) if pd.notna(row.get('Security Name')) else ''
            company_name = clean_company_name(security_name)
            
            record = {
                'Symbol': symbol,
                'Security Name': security_name,  # Keep original for reference
                'Company Name': company_name,  # Clean name without "Common Stock" suffix
                'Market Category': str(row.get('Market Category', '')) if pd.notna(row.get('Market Category')) else '',
                'Test Issue': str(row.get('Test Issue', 'N')) if pd.notna(row.get('Test Issue')) else 'N',
                'Financial Status': str(row.get('Financial Status', '')) if pd.notna(row.get('Financial Status')) else '',
                'Round Lot Size': round_lot,
                'ETF': str(row.get('ETF', 'N')) if pd.notna(row.get('ETF')) else 'N',
                'NextShares': str(row.get('NextShares', 'N')) if pd.notna(row.get('NextShares')) else 'N',
                'exchange': str(row.get('exchange', 'NASDAQ')) if pd.notna(row.get('exchange')) else 'NASDAQ'
            }
            records.append(record)
        
        logger.info(f"Prepared {len(records)} valid records for insertion")
        
        # Insert in batches of 1000 (Supabase limit)
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            supabase.table(STOCK_TICKERS_TABLE).insert(batch).execute()
            total_inserted += len(batch)
            logger.info(f"Inserted {total_inserted}/{len(records)} ticker records...")
        
        logger.info(f"✅ Successfully updated {total_inserted} tickers in stock_tickers table")
        logger.info(f"📝 Note: stocks table should be updated by your price fetcher separately")
        
        # Optional: Clean up stocks table - remove delisted stocks
        try:
            logger.info("Cleaning up delisted stocks from stocks table...")
            
            # Get all valid symbols from stock_tickers
            valid_symbols = [r['Symbol'] for r in records]
            
            # Count how many stocks will be removed
            existing_stocks = supabase.table(STOCKS_TABLE).select('symbol', count='exact').execute()
            
            if existing_stocks.count:
                logger.info(f"Found {existing_stocks.count} stocks in stocks table")
                
                # Note: Supabase doesn't support NOT IN with large lists via API
                # So we'll log this and suggest manual cleanup
                logger.info(f"✅ {len(valid_symbols)} valid tickers in stock_tickers")
                logger.info(f"⚠️  To remove delisted stocks from 'stocks' table, run this SQL in Supabase:")
                logger.info(f"    DELETE FROM stocks WHERE symbol NOT IN (SELECT \"Symbol\" FROM stock_tickers);")
        except Exception as e:
            logger.warning(f"Could not check stocks table: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error updating Supabase: {str(e)}")
        raise


def main():
    """Main execution function"""
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info(f"Starting stock data update at {start_time}")
    logger.info("=" * 50)
    
    try:
        # Fetch and clean NASDAQ data
        nasdaq_df = fetch_and_clean_nasdaq_data()
        
        # Fetch and clean NYSE data
        nyse_df = fetch_and_clean_nyse_data()
        
        # Combine dataframes
        logger.info("=" * 50)
        logger.info("Combining data...")
        combined_df = pd.concat([nasdaq_df, nyse_df], ignore_index=True)
        
        logger.info(f"Total stocks after cleaning: {len(combined_df)}")
        logger.info(f"NASDAQ: {len(nasdaq_df)}, NYSE: {len(nyse_df)}")
        
        # Save to CSV for backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"stocks_backup_{timestamp}.csv"
        combined_df.to_csv(backup_file, index=False)
        logger.info(f"Backup saved to {backup_file}")
        
        # Update Supabase
        update_supabase_stocks(combined_df)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 50)
        logger.info(f"✅ Update completed successfully in {duration:.2f} seconds")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Update failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()