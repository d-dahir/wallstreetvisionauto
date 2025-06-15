import discord
import pandas as pd
import asyncio
from datetime import datetime
import os

BOT_TOKEN = os.environ['DISCORD_BOT_TOKEN']  # From GitHub Secrets
QUALIFIED_CHANNEL_ID = 1378848876605210685   # Replace with your channel ID
DISQUALIFIED_CHANNEL_ID = 1379214297674289344

def create_qualified_embed(row):
    embed = discord.Embed(
        title=f"📈 Qualified Insider Buy: {row['Ticker']}",
        color=0x00ff00,
        url=f"https://openinsider.com/search?q={row['Ticker']}"
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
    embed.set_footer(text=f"Filed: {row['Filing Date']}")
    return embed

async def send_alerts():
    try:
        qualified = pd.read_csv('insider_trades_variant2.csv')
        all_trades = pd.read_csv('insider_trades_with_market_data.csv')
        
        # Find new entries since last run
        last_run_qualified = pd.read_csv('last_qualified.csv') if os.path.exists('last_qualified.csv') else pd.DataFrame()
        new_qualified = qualified[~qualified['Ticker'].isin(last_run_qualified['Ticker'])] if not last_run_qualified.empty else qualified
        qualified.to_csv('last_qualified.csv', index=False)

        disqualified = all_trades[~all_trades['Ticker'].isin(qualified['Ticker'])]
        last_run_disqualified = pd.read_csv('last_disqualified.csv') if os.path.exists('last_disqualified.csv') else pd.DataFrame()
        new_disqualified = disqualified[~disqualified['Ticker'].isin(last_run_disqualified['Ticker'])] if not last_run_disqualified.empty else disqualified
        disqualified.to_csv('last_disqualified.csv', index=False)

        # Initialize Discord client
        intents = discord.Intents.default()
        client = discord.Client(intents=intents)
        await client.login(BOT_TOKEN)
        qualified_channel = await client.fetch_channel(QUALIFIED_CHANNEL_ID)
        research_channel = await client.fetch_channel(DISQUALIFIED_CHANNEL_ID)

        # Send alerts only for new entries
        for _, row in new_qualified.iterrows():
            await qualified_channel.send(embed=create_qualified_embed(row))
            await asyncio.sleep(1)

        for _, row in new_disqualified.iterrows():
            embed = discord.Embed(
                title=f"⚠️ Research Candidate: {row['Ticker']}",
                description=f"**Reason:** Low volume/ATR",
                color=0xff9900,
                url=f"https://openinsider.com/search?q={row['Ticker']}"
            )
            await research_channel.send(embed=embed)
            await asyncio.sleep(1)

    except Exception as e:
        print(f"❌ Discord error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    print("\n=== Starting Discord Alerts ===")
    asyncio.run(send_alerts())