import asyncio
import json
import requests
from aiohttp import ClientSession
from soliscloud_api import SoliscloudAPI
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from soliscloud_api.helpers import Helpers
import configcentral

# Function to get today's date
def get_today_date():
    """Returns today's date in YYYY-MM-DD format."""
    return datetime.now().strftime("%Y-%m-%d")

# Function to get the current month in YYYY-MM format
def get_current_month():
    """Returns the current month in YYYY-MM format."""
    return datetime.now().strftime("%Y-%m")

# Telegram bot
TOKEN = configcentral.TELEGRAM_TOKEN
CHAT_ID = configcentral.CHAT_ID

# InfluxDB Configuration
url = configcentral.INFLUX_URL
token = configcentral.INFLUX_TOKEN
org = configcentral.INFLUX_ORG
bucket = configcentral.INFLUX_BUCKET

# Initialize InfluxDB Client
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

async def retry_operation(func, retries, max_retries, message):
    """A helper function to retry operations."""
    while retries <= max_retries:
        try:
            return await func()
        except (asyncio.TimeoutError, SoliscloudAPI.SolisCloudError) as e:
            retries += 1
            print(f"⚠️ Retry {retries}/{max_retries} due to error: {e} - {message}")
            send_telegram_message(f"⚠️ Retry {retries}/{max_retries} due to error: {e} - {message}")
            await asyncio.sleep(5)  # Optional delay between retries
    print(f"❌ Max retries reached for {message}.")
    send_telegram_message(f"❌ Max retries reached for [INFLUXDB : INVERTER_DAILY]{message}.")
    return None

def send_telegram_message(message):
    """Send a notification via Telegram bot."""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("📩 Telegram Notification Sent")
        else:
            print("⚠️ Telegram Failed [INFLUXDB : INVERTER_DAILY]")
    except Exception as e:
        print(f"🚨 Telegram Error [INFLUXDB : INVERTER_DAILY]: {e}")

async def insert_inverter_data(station_data_list):
    """Insert filtered data (only today's records) into InfluxDB."""
    try:
        for station_data in station_data_list:
            point = Point("inverter_daily") \
                .tag("inverter_id", station_data.get("inverter_id")) \
                .tag("station_name", station_data.get("station_name")) \
                .tag("dateStr", station_data.get("dateStr")) \
                .field("energy", station_data.get("energy")) \
                .field("money", station_data.get("money")) \
                .field("moneyStr", station_data.get("moneyStr")) \
                .field("energyStr", station_data.get("energyStr"))

            # Write to InfluxDB
            write_api.write(bucket=bucket, record=point)
            print(f"✅ Data inserted for Inverter ID: {station_data.get('inverter_id')} on {station_data.get('dateStr')}")

    except Exception as e:
        print(f"❌ Error inserting data: {e}")

async def fetch_all_station(api_key, api_secret):
    """Fetch inverter data for the current month, then filter today's data."""
    current_month = get_current_month()
    today_date = get_today_date()
    retries = 0
    max_retries = 10
    total_inverters = 0

    async with ClientSession() as websession:
        try:
            soliscloud = SoliscloudAPI('https://soliscloud.com:13333', websession)
            inverter_ids = await Helpers.get_inverter_ids(soliscloud, api_key, api_secret)

            if not inverter_ids:
                print("❌ No inverters found.")
                return

            for inverter_id in inverter_ids:
                inverter_detail = await retry_operation(
                    lambda: soliscloud.inverter_detail(api_key, api_secret, inverter_id=inverter_id),
                    retries,
                    max_retries,
                    f"Inverter detail for {inverter_id}",
                )

                if inverter_detail is None:
                    print(f"⚠️ No details found for Inverter ID: {inverter_id}. Skipping...")
                    continue

                #print(f"📡 API Response for Inverter ID {inverter_id}:")
                #print(json.dumps(inverter_detail, indent=2))

                station_name = inverter_detail.get("stationName")
                print(f"\n📡 Fetching Inverter Data for ID: {inverter_id}, Name: {station_name}")

                print(f"🔄 Fetching monthly data for {current_month}...")

                inverter_month_data = await soliscloud.inverter_month(
                    api_key, api_secret,
                    currency="MYR",
                    month=current_month,
                    inverter_id=inverter_id
                )

                if not inverter_month_data:
                    print(f"⚠️ No data returned for {current_month}. Skipping...")
                    continue

                # Filtering only today's data
                todays_records = [
                    {
                        "inverter_id": inverter_id,
                        "station_name": station_name,
                        "dateStr": record.get("dateStr"),
                        "money": float(record.get("money")),
                        "moneyStr": record.get("moneyStr"),
                        "energy": float(record.get("energy")),
                        "energyStr": record.get("energyStr", "")
                    }
                    for record in inverter_month_data if record.get("dateStr") == today_date
                ]

                if not todays_records:
                    print(f"⚠️ No data available for today ({today_date}). Skipping...")
                    continue

                print(f"✅ Found {len(todays_records)} records for today ({today_date}).")

                # Insert today's data into InfluxDB
                await retry_operation(
                    lambda: insert_inverter_data(todays_records),
                    retries,
                    max_retries,
                    f"Insert data for Inverter ID {inverter_id} and Date {today_date}"
                )

                total_inverters += 1
                print(f"🎯 Total Inverters Processed: {total_inverters}")

        except Exception as e:
            print(f"🚨 General Error: {e}")
            send_telegram_message(f"🚨 General Error: {e}")

async def main():
    """Main function to fetch API credentials and initiate data collection."""
    with open('config.json', 'r') as file:
        data = json.load(file)

    api_key = data['key']
    api_secret = data['secret'].encode('utf-8')

    await fetch_all_station(api_key, api_secret)
    print("✅ DONE")

if __name__ == '__main__':
    asyncio.run(main())
