import discord
import pandas as pd
import asyncio
from datetime import datetime
import os

BOT_TOKEN = os.environ['DISCORD_BOT_TOKEN']
QUALIFIED_CHANNEL_ID = 1378848876605210685
DISQUALIFIED_CHANNEL_ID = 1379214297674289344

def create_qualified_embed(row):
    """Rich embed for qualified trades"""
    embed = discord.Embed(
        title=f"📈 Qualified Insider Buy: {row['Ticker']}",
        color=0x00ff00
    )
    embed.add_field(name="Company", value=row['Company Name'], inline=True)
    embed.add_field(name="Insider", value=f"{row['Insider Name']} ({row['Title']})", inline=True)
    embed.add_field(name="Amount", value=f"${row['Value']:,.0f}", inline=False)
    embed.add_field(name="Metrics", value=f"""
    • Price: ${row['Price']:.2f} → ${row['Current Price']:.2f}
    • Volume: ${row['Daily Volume']/1e6:.1f}M
    • ATR: {row['ATR %']:.1f}%
    • Market Cap: ${row['Market Cap']/1e9:.1f}B
    """)
    embed.set_footer(text=f"Filed: {datetime.strptime(row['Filing Date'], '%Y-%m-%d %H:%M:%S').strftime('%b %d %H:%M')}")
    return embed

def create_disqualified_embed(row):
    """Rich embed for research candidates"""
    embed = discord.Embed(
        title=f"⚠️ Disqualified: {row['Ticker']}",
        color=0xff9900
    )
    
    reasons = []
    if pd.isna(row.get('Current Price')):
        reasons.append("No market data available")
    else:
        if pd.isna(row.get('Daily Volume')) or row['Daily Volume'] < 30_000_000:
            vol_value = 0 if pd.isna(row.get('Daily Volume')) else row['Daily Volume']
            reasons.append(f"Volume ${vol_value/1e6:.1f}M (needs $30M+)")
            
        if pd.isna(row.get('ATR %')) or not 7 <= row['ATR %'] <= 20:
            atr_value = 0 if pd.isna(row.get('ATR %')) else row['ATR %']
            reasons.append(f"ATR {atr_value:.1f}% (needs 7-20%)")
    
    embed.add_field(name="Reasons", value="\n".join(reasons) if reasons else "Unknown", inline=False)
    embed.add_field(name="Insider", value=f"{row['Insider Name']} (${row['Value']:,.0f})", inline=True)
    embed.add_field(name="Filed", value=datetime.strptime(row['Filing Date'], '%Y-%m-%d %H:%M:%S').strftime('%b %d'), inline=True)
    
    if not pd.isna(row.get('Current Price')):
        embed.add_field(name="Market Data", value=f"""
        Price: ${row.get('Current Price', 0):.2f}
        Volume: ${row.get('Daily Volume', 0)/1e6:.1f}M
        ATR: {row.get('ATR %', 0):.1f}%
        """, inline=False)
    
    return embed

async def send_alerts():
    try:
        all_trades = pd.read_csv('insider_trades_with_market_data.csv')
        qualified = pd.read_csv('insider_trades_variant2.csv')
        
        # Find new entries since last run
        last_run_qualified = pd.read_csv('last_qualified.csv') if os.path.exists('last_qualified.csv') else pd.DataFrame()
        new_qualified = qualified[~qualified['Ticker'].isin(last_run_qualified['Ticker'])] if not last_run_qualified.empty else qualified
        qualified.to_csv('last_qualified.csv', index=False)

        disqualified = all_trades[~all_trades['Ticker'].isin(qualified['Ticker'])]
        last_run_disqualified = pd.read_csv('last_disqualified.csv') if os.path.exists('last_disqualified.csv') else pd.DataFrame()
        new_disqualified = disqualified[~disqualified['Ticker'].isin(last_run_disqualified['Ticker'])] if not last_run_disqualified.empty else disqualified
        disqualified.to_csv('last_disqualified.csv', index=False)

        intents = discord.Intents.default()
        client = discord.Client(intents=intents)
        await client.login(BOT_TOKEN)
        qualified_channel = await client.fetch_channel(QUALIFIED_CHANNEL_ID)
        research_channel = await client.fetch_channel(DISQUALIFIED_CHANNEL_ID)

        for _, row in new_qualified.iterrows():
            await qualified_channel.send(embed=create_qualified_embed(row))
            await asyncio.sleep(1)

        for _, row in new_disqualified.iterrows():
            await research_channel.send(embed=create_disqualified_embed(row))
            await asyncio.sleep(1)
            
    except Exception as e:
        print(f"❌ Discord error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    print("Starting dual-channel alerts...")
    asyncio.run(send_alerts())
