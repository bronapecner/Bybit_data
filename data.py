import pandas as pd
import ccxt
import asyncio
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

# Načtení proměnných prostředí ze souboru .env
load_dotenv()

async def get_historical_data(client, symbol, timeframe, limit=300):
    """
    Asynchronní stahování historických dat.
    """
    now = datetime.now()
    timeframes = {
        '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '1d': 86400, '1w': 604800
    }
   
    interval_in_seconds = timeframes[timeframe]
    since_time = now - timedelta(seconds=interval_in_seconds * limit)
    since = int(since_time.timestamp() * 1000)
    until = int(now.timestamp() * 1000)
   
    all_data = []
   
    while since < until:
       
        ohlcv = await asyncio.to_thread(client.fetch_ohlcv, symbol, timeframe, since, 1000)
       
        time.sleep(1)
        if not ohlcv:
            break
       
        since = ohlcv[-1][0] + 1
        all_data += ohlcv
   
    df = pd.DataFrame(all_data, columns=['date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['date'] = pd.to_datetime(df['date'], unit='ms')
   
    return df

# Získání API klíčů z proměnných prostředí
apikey2 = os.getenv("APIKEY")
secret2 = os.getenv("SECRET")

# Vytvoření klienta s API klíči načtenými z .env
client = ccxt.bybit({
    'apiKey': apikey2,
    'secret': secret2,
    'enableRateLimit': True,
    'timeout': 30000,
    'options': {
        'defaultType': 'UNIFIED',  
        'createMarketBuyOrderRequiresPrice': False
     },
     'headers': {
         'Connection': 'keep-alive',
       
     }
})

# Klíčová část pro spuštění a ověření funkčnosti
async def main():
    timeframes_to_fetch = ['15m', '1h', '4h', '1d', '1w']
    symbol = 'BTC/USDT'  # Příklad symbolu pro Bitcoin
    all_dataframes = {}

    for timeframe in timeframes_to_fetch:
        print(f"Stahuji data pro {symbol} na časovém rámci {timeframe}...")
        try:
            df = await get_historical_data(client, symbol, timeframe)
            all_dataframes[timeframe] = df
            print(f"Data pro {timeframe} byla úspěšně stažena.")
        except Exception as e:
            print(f"Chyba při stahování dat pro {timeframe}: {e}")
    
    # Zde můžete pracovat s jednotlivými DataFrames
    for tf, df in all_dataframes.items():
        print(f"\nDataFrame pro {tf} (prvních 5 řádků):")
        print(df.head()) # Vypíše prvních 5 řádků DataFrame
        print(f"Počet řádků v DataFrame pro {tf}: {len(df)}") # Vypíše počet stažených řádků
        print("-" * 30) # Přidá oddělovací čáru pro přehlednost

if __name__ == "__main__":
    asyncio.run(main())