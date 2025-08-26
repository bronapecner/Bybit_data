import pandas as pd
import ccxt
import asyncio
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Načtení proměnných prostředí ze souboru .env
load_dotenv()

async def get_historical_data(client, symbol, timeframe, start_date_str):
    """
    Asynchronní stahování historických dat od zadaného data až po současnost.
    """
    # Převod startovního data na timestamp v milisekundách, který vyžaduje CCXT
    since = int(datetime.strptime(start_date_str, "%Y-%m-%d").timestamp() * 1000)
    
    # Koncový čas je teď
    until = int(datetime.now().timestamp() * 1000)
    
    all_data = []
    
    print(f"Stahuji data pro {symbol} ({timeframe}) od {start_date_str}...")
    
    while since < until:
        try:
            # Používáme to_thread, aby se blokující volání spustilo v samostatném vlákně
            ohlcv = await asyncio.to_thread(client.fetch_ohlcv, symbol, timeframe, since, 1000)
            
            # Pokud API vrátí prázdný seznam, znamená to, že už nejsou žádná další data
            if not ohlcv:
                print("Dosáhli jsme konce dostupných dat.")
                break
            
            # Aktualizujeme 'since' pro další iteraci na časové razítko poslední svíčky + 1ms
            since = ohlcv[-1][0] + 1
            all_data.extend(ohlcv)
            
            # Informace o průběhu stahování
            last_date_fetched = datetime.fromtimestamp(ohlcv[-1][0] / 1000).strftime('%Y-%m-%d')
            print(f"  -> Staženo {len(all_data)} záznamů, poslední datum: {last_date_fetched}")

            # Zpomalení požadavků, abychom nepřetížili API burzy
            await asyncio.sleep(client.rateLimit / 1000)

        except Exception as e:
            print(f"Nastala chyba při stahování dat: {e}. Zkouším znovu za 10 sekund...")
            await asyncio.sleep(10) # Pauza před opakováním v případě chyby
            
    if not all_data:
        print("Nepodařilo se stáhnout žádná data.")
        return pd.DataFrame()

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
    'enableRateLimit': True, # Důležité pro automatické řízení počtu požadavků
    'options': {
        'defaultType': 'spot', # Nebo 'linear' pro futures. Pro BTC/USDT je 'spot' běžnější.
    },
})

async def main():
    # --- ZDE NASTAVTE SVÉ PARAMETRY ---
    symbol = 'SOL/USDT'
    start_date = '2021-07-01'
    timeframes_to_fetch = ['15m', '1h', '4h', '1d', '1w']
    output_directory = '/Users/bronapecner/Downloads/trading temp/backtesting'
    # ------------------------------------

    # Zajištění, že cílová složka existuje. Pokud ne, vytvoří se.
    os.makedirs(output_directory, exist_ok=True)
    print(f"Data budou uložena do složky: {output_directory}\n")

    for timeframe in timeframes_to_fetch:
        try:
            df = await get_historical_data(client, symbol, timeframe, start_date)
            
            if not df.empty:
                # Vytvoření názvu souboru, např. BTCUSDT_15m.csv
                # Nahrazení "/" v symbolu, aby nevznikla podsložka
                filename = f"{symbol.replace('/', '')}_{timeframe}.csv"
                filepath = os.path.join(output_directory, filename)
                
                # Uložení DataFrame do CSV souboru bez indexu
                df.to_csv(filepath, index=False)
                
                print(f"Data pro {timeframe} byla úspěšně uložena do souboru: {filepath}")
                print(f"   Celkem staženo {len(df)} řádků.\n")
            else:
                print(f"Pro timeframe {timeframe} nebyla stažena žádná data.\n")

        except Exception as e:
            print(f"Kritická chyba při zpracování timeframe {timeframe}: {e}\n")

if __name__ == "__main__":
    # Nastavení politiky pro event loop na Windows, pokud by byl problém
    # if os.name == 'nt':
    #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())