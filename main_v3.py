import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

def fetch_openinsider_data():
    url = "http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=7&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&vl=100&vh=&ocl=5&och=&sic1=-1&sicl=100&sich=9999&isofficer=1&iscob=1&isceo=1&ispres=1&iscoo=1&iscfo=1&isgc=1&isvp=1&isdirector=1&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=1000&page=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tinytable'})
        
        if not table:
            raise ValueError("Table not found")

        headers = [th.text.strip().replace('\xa0', ' ') for th in table.find_all('th')][:-4]
        rows = [[td.text.strip() for td in tr.find_all('td')][:len(headers)] for tr in table.find_all('tr')[1:]]
        df = pd.DataFrame(rows, columns=headers)
        df.columns = df.columns.str.replace('\xa0', ' ')

        # Filtering
        if 'X' in df.columns:
            df = df[~df['X'].isin(['A', 'D'])]
        df['Value'] = df['Value'].str.replace(r'[\$,+]', '', regex=True).astype(float)
        df = df[df['Value'] >= 100_000]
        df['Qty'] = df['Qty'].str.replace('+', '').str.replace(',', '').astype(int)
        df['Price'] = df['Price'].str.replace('$', '').astype(float)
        
        return df

    except Exception as e:
        print(f"❌ Error in fetch_openinsider_data: {e}")
        return None

def update_csv(new_data, filename='openinsider_trades_v3.csv'):
    try:
        if os.path.exists(filename):
            existing_data = pd.read_csv(filename)
            # Check for new entries using Filing Date + Ticker + Insider Name as a unique key
            new_data['key'] = new_data['Filing Date'] + new_data['Ticker'] + new_data['Insider Name']
            existing_data['key'] = existing_data['Filing Date'] + existing_data['Ticker'] + existing_data['Insider Name']
            new_entries = new_data[~new_data['key'].isin(existing_data['key'])]
            new_entries = new_entries.drop(columns=['key'])
            existing_data = existing_data.drop(columns=['key'])
            
            if not new_entries.empty:
                updated_data = pd.concat([existing_data, new_entries], ignore_index=True)
                updated_data.to_csv(filename, index=False)
                print(f"✅ Added {len(new_entries)} new entries to {filename}")
                return new_entries
            else:
                print("🔄 No new entries found")
                return pd.DataFrame()
        else:
            new_data.to_csv(filename, index=False)
            print(f"📁 Created new file: {filename}")
            return new_data
    except Exception as e:
        print(f"❌ Failed to update CSV: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    print("\n=== Fetching OpenInsider Data ===")
    data = fetch_openinsider_data()
    if data is not None:
        new_entries = update_csv(data)
        print(f"Total new entries: {len(new_entries)}")