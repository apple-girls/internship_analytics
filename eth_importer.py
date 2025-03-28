import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import time 
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

MIN_DATE = datetime(2025, 1, 1)
unix_min_date = int(MIN_DATE.timestamp())
MAX_WORKERS = 3  
BATCH_SIZE = 5 
MAX_WALLETS = 17  

CRYPTO_PRICES = {}
LAST_UPDATE = None

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def get_wallets_from_db(limit=MAX_WALLETS):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT address 
                FROM wallets 
                LIMIT %s
            """, (limit,))
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении кошельков: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_crypto_prices():
    """Получаем актуальные курсы через CoinGecko API"""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': 'ethereum,bitcoin,tether',
        'vs_currencies': 'usd',
        'precision': 8
    }
    response = requests.get(url, params=params)
    return response.json() if response.status_code == 200 else None

def get_crypto_prices_cached():
    """Кешируем курсы на 5 минут"""
    global LAST_UPDATE, CRYPTO_PRICES
    if not LAST_UPDATE or (datetime.now() - LAST_UPDATE) > timedelta(minutes=5):
        CRYPTO_PRICES = get_crypto_prices()
        LAST_UPDATE = datetime.now()
    return CRYPTO_PRICES

def calculate_volumes(amount, token_symbol, prices):
    if not prices:
        return None, None
    
    usdt_volume = None
    btc_volume = None
    
    try:    
        if str(token_symbol).upper() == 'USDT':
            usdt_volume = float(amount)
        else:
            eth_price = prices.get('ethereum', {}).get('usd')
            btc_price = prices.get('bitcoin', {}).get('usd')
            
            if eth_price:
                usd_value = float(amount) * float(eth_price)
                usdt_volume = usd_value
                
                if btc_price:
                    btc_volume = usd_value / float(btc_price)
    
    except Exception as e:
        return None, None
    
    return usdt_volume, btc_volume

def get_token_transactions(wallet_address):
    api_key = os.getenv('API_KEY')
    
    url = f"https://api.etherscan.io/api?module=account&action=tokentx&address={wallet_address}&starttimestamp={unix_min_date}&apikey={api_key}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data['status'] != '1':
            error_msg = data.get('message', 'Unknown error')
            print(f"[{wallet_address}] Etherscan error: {error_msg}")
            return pd.DataFrame() 
            
        df = pd.DataFrame(data['result'])

        df['amount'] = df['value'].astype(float) / (10 ** df['tokenDecimal'].astype(float))
        df['timestamp'] = pd.to_datetime(df['timeStamp'], unit='s')
        df['direction'] = df.apply( 
            lambda x: (
            'outgoing' if x['from'].lower() == wallet_address.lower() else
            'incoming' if x['to'].lower() == wallet_address.lower() else
            'internal' if x['to'].lower() == x['from'] else
            'NA'
        ), axis=1
        )
        df['tx_type'] = df.apply(
            lambda x: (
                'nft_transfer' if 'tokenID' in x or ('tokenName' in x and 'NFT' in str(x['tokenName']).upper())
                else 'token_transfer' if str(x.get('token_symbol', '')).upper() != 'ETH'
                else 'eth_transfer'  
            ),
            axis=1
        )
        
        return df
        
    except Exception as e:
        return pd.DataFrame()
    
def determine_tx_type(tx):
    if 'tokenID' in tx and pd.notna(tx['tokenID']):
        return 'nft_transfer'
    
    if 'tokenDecimal' in tx and pd.notna(tx['tokenDecimal']):
        return 'token_transfer'
    
    if tx.get('contractAddress') or tx.get('isContractCreation', False):
        return 'contract_creation'
    
    if tx.get('input', '0x') != '0x':
        return 'contract_interaction'
    
    return 'eth_transfer'

def fetch_wallet_transactions(wallet_address):
    """Улучшенная загрузка транзакций с обработкой ошибок"""
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_transactions_with_retry(address):
        token_txs = get_token_transactions(address)
        eth_txs = get_eth_transactions(address)

        token_txs['tx_source'] = 'token'
        eth_txs['tx_source'] = 'eth'

        combined = pd.concat([token_txs, eth_txs], ignore_index=True)
        if not combined.empty:
            combined['wallet_address'] = address
        return combined

    try:
        time.sleep(0.3) 
        
        df = get_transactions_with_retry(wallet_address)
            
        return df
        
    except Exception as e:
        return pd.DataFrame()

def get_eth_transactions(wallet_address):
    api_key = os.getenv('API_KEY')
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={wallet_address}&starttimestamp={unix_min_date}&apikey={api_key}"
    
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        
        if data['status'] != '1':
            print(f"[ETH] Ошибка для {wallet_address}: {data.get('message')}")
            return pd.DataFrame()
            
        df = pd.DataFrame(data['result'])
        df['tokenDecimal'] = 18
        
        df['amount'] = df['value'].astype(float) / 10**18  # конвертируем wei в ETH
        df['direction'] = df.apply( 
            lambda x: (
            'outgoing' if x['from'].lower() == wallet_address.lower() else
            'incoming' if x['to'].lower() == wallet_address.lower() else
            'internal' if x['to'].lower() == x['from'] else
            'NA'
        ), axis=1
        )
        df['token_symbol'] = 'ETH'
        df['contract_address'] = 'NA' # для ETH нет контракта, поэтому ставим как NA
        df['wallet_address'] = wallet_address   
        df['tx_type'] = df.apply(
            lambda x: (
                'contract_creation' if pd.notna(x.get('contractAddress')) or pd.isna(x['to'])
                else 'contract_interaction' if x['input'] != '0x'
                else 'eth_transfer'
            ),
            axis=1
        )
        return df
        
    except Exception as e:
        return pd.DataFrame()

def process_wallets(wallet_addresses):
    prices = get_crypto_prices_cached()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        token_futures = [executor.submit(get_token_transactions, addr) for addr in wallet_addresses]
        eth_futures = [executor.submit(get_eth_transactions, addr) for addr in wallet_addresses]
        
        results = [f.result() for f in token_futures + eth_futures]
    
    all_transactions = pd.concat([r for r in results if not r.empty], ignore_index=True)

    all_transactions = all_transactions[all_transactions['timestamp'] >= MIN_DATE]

    all_transactions['amount'] = all_transactions['value'].astype(float) / (10 ** all_transactions['tokenDecimal'].astype(float))
    all_transactions['timestamp'] = pd.to_datetime(all_transactions['timeStamp'], unit='s')
    tracked_wallets = set(wallets[:MAX_WALLETS]) 

    if 'direction' not in all_transactions.columns:
        all_transactions['direction'] = 'NA'

    needs_update = (all_transactions['direction'] == 'NA')
    all_transactions.loc[needs_update, 'direction'] = all_transactions[needs_update].apply(
        lambda x: (
            'internal' if (x['from'].lower() in tracked_wallets and 
                        x['to'].lower() in tracked_wallets) else
            'outgoing' if x['from'].lower() == x['wallet_address'].lower() else
            'incoming' if x['to'].lower() == x['wallet_address'].lower() else
            'NA'
        ),
        axis=1
    )

    for _, row in all_transactions.iterrows():
        if pd.notna(row.get('token_symbol')) and str(row['token_symbol']).upper() == 'ETH':
            if row['tx_type'] == 'token_transfer': 
                if pd.notna(row.get('contractAddress')) or pd.isna(row.get('to')):
                    row['tx_type'] = 'contract_creation'
                elif row.get('input', '0x') != '0x':
                    row['tx_type'] = 'contract_interaction'
                else:
                    row['tx_type'] = 'eth_transfer'

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tx_hash FROM transactions")
            existing_hashes = {row[0] for row in cur.fetchall()}
            
            inserted = 0
            skipped = 0
            
            for _, row in all_transactions.iterrows():
                if pd.to_datetime(row['timeStamp'], unit='s') < MIN_DATE:
                    skipped += 1
                    continue
                     
                if row['hash'] in existing_hashes:
                    skipped += 1
                    continue
                    
                try:
                    usdt_vol, btc_vol = calculate_volumes(
                        amount=float(row['amount']),
                        token_symbol=row['tokenSymbol'],
                        prices=prices
                    )
                    
                    cur.execute("""
                        INSERT INTO transactions (
                            blockchain_id, tx_hash, timestamp, from_address, to_address,
                            direction, token_symbol, contract_address, amount, gas_fee,
                            tx_type, is_suspicious, usdt_volume, btc_volume
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (tx_hash) DO NOTHING
                    """, (
                        1, 
                        row['hash'], 
                        row['timestamp'], 
                        row['from'], 
                        row['to'],
                        row['direction'], 
                        row['tokenSymbol'], 
                        'NA' if row['tokenSymbol'] == 'ETH' else row['contractAddress'],
                        float(row['amount']), 
                        (float(row.get('gasUsed', 0)) * float(row.get('gasPrice', 0))) / 10**18,
                        'eth_transfer' if row['tokenSymbol'] == 'ETH' else row['tx_type'], 
                        False, 
                        usdt_vol, 
                        btc_vol
                    ))
                    
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
                        
                except Exception as e:
                    conn.rollback()
                    continue
            
            conn.commit()
            print(f"добавлено: {inserted}, пропущено: {skipped}")
            
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    wallets = get_wallets_from_db()
    process_wallets(wallets[:MAX_WALLETS])