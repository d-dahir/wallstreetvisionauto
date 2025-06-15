import yfinance as yf
import pandas as pd
from tqdm import tqdm
import os

def get_market_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="15d", interval="1d")
        if hist.empty:
            return None
            
        valid_days = hist[hist['Volume'] > 0].copy()
        if len(valid_days) < 5:
            return None
            
        valid_days['Prev Close'] = valid_days['Close'].shift(1)
        tr = [max(row['High'] - row['Low'], abs(row['High'] - row['Prev Close']), abs(row['Low'] - row['Prev Close'])) 
              for _, row in valid_days.iterrows() if not pd.isna(row['Prev Close'])]
        
        last_day = valid_days.iloc[-1]
        current_price = last_day['Close']
        atr_window = min(14, len(tr))
        atr = pd.Series(tr[-atr_window:]).mean()
        atr_pct = (atr / current_price) * 100 if current_price else 0
        
        return {
            'Current Price': current_price,
            'Daily Volume': last_day['Volume'] * current_price,
            '5d Avg Volume': valid_days['Volume'].iloc[-5:].mean() * current_price,
            'ATR %': atr_pct,
            'Market Cap': stock.info.get('marketCap', None)
        }
    except Exception as e:
        print(f"⚠️ Error processing {ticker}: {str(e)[:100]}")
        return None

def main():
    print("\n=== Fetching Market Data ===")
    try:
        # Load new entries only
        new_entries = pd.read_csv('openinsider_trades_v3.csv')
        if os.path.exists('insider_trades_with_market_data.csv'):
            existing_data = pd.read_csv('insider_trades_with_market_data.csv')
            # Filter only new tickers not already processed
            new_entries = new_entries[~new_entries['Ticker'].isin(existing_data['Ticker'])]
        
        if new_entries.empty:
            print("🔄 No new tickers to process")
            return

        print(f"Processing {len(new_entries)} new tickers...")
        market_data = []
        for ticker in tqdm(new_entries['Ticker'].unique(), desc="Fetching"):
            data = get_market_data(ticker) or {
                'Current Price': None,
                'Daily Volume': None,
                '5d Avg Volume': None,
                'ATR %': None,
                'Market Cap': None
            }
            market_data.append(data)

        # Merge market data with new entries
        market_df = pd.DataFrame(market_data, index=new_entries['Ticker'].unique())
        new_data_with_market = new_entries.merge(market_df, left_on='Ticker', right_index=True, how='left')
        
        # Append to existing data or create new file
        if os.path.exists('insider_trades_with_market_data.csv'):
            updated_data = pd.concat([pd.read_csv('insider_trades_with_market_data.csv'), new_data_with_market], ignore_index=True)
        else:
            updated_data = new_data_with_market
        updated_data.to_csv('insider_trades_with_market_data.csv', index=False)

        # Apply filters for qualified trades
        qualified = updated_data[
            (updated_data['Daily Volume'].notna()) &
            (updated_data['Daily Volume'] >= 30_000_000) &
            (updated_data['ATR %'].between(7, 20))
        ]
        qualified.to_csv('insider_trades_variant2.csv', index=False)
        print(f"✅ Updated market data. Qualified trades: {len(qualified)}")

    except Exception as e:
        print(f"❌ Critical error: {e}")

if __name__ == "__main__":
    main()