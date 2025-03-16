import asyncio
import json
import requests
import configcentral
import mysql.connector
from aiohttp import ClientSession
from soliscloud_api import SoliscloudAPI
from datetime import datetime
from soliscloud_api.helpers import Helpers
import configcentral

def get_month_list(start_year=2024):
    """Generate a list of months from start_year to the current month in YYYY-MM format."""
    current_year = datetime.now().year
    current_month = datetime.now().month
    months = [f"{year}-{month:02d}" for year in range(start_year, current_year + 1) for month in range(1, 13)
              if not (year == current_year and month > current_month)]
    return months

# Access the variables
TOKEN = configcentral.TELEGRAM_TOKEN
CHAT_ID = configcentral.CHAT_ID

# MySQL Database Configuration
MYSQL_HOST = configcentral.MYSQL_HOST
MYSQL_USER = configcentral.MYSQL_USER
MYSQL_PASSWORD = configcentral.MYSQL_PASSWORD
MYSQL_DATABASE = configcentral.MYSQL_DATABASE

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("📩 Telegram Notification Sent")
        else:
            print("⚠️ Telegram Failed")
    except Exception as e:
        print(f"🚨 Telegram Error: {e}")

# Function to Insert Data into MySQL
async def insert_inverter_data(station_data_list):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = connection.cursor()

        for station_data in station_data_list:
            sql = """
            INSERT INTO inverter_daily (inverter_id, station_name, dateStr, energy, money, moneyStr, energyStr)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                station_data.get("inverter_id"),
                station_data.get("station_name"),
                station_data.get("dateStr"),
                station_data.get("energy"),
                station_data.get("money"),
                station_data.get("moneyStr"),
                station_data.get("energyStr"),
            )
            cursor.execute(sql, values)
            connection.commit()
            print(f"✅ Data inserted for Inverter ID: {station_data.get('inverter_id')}")

        cursor.close()
        connection.close()
    except Exception as e:
        print(f"❌ Error inserting data into MySQL: {e}")

async def fetch_all_station(api_key, api_secret):
    month_list = get_month_list()
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
                inverter_detail = await soliscloud.inverter_detail(api_key, api_secret, inverter_id=inverter_id)
                if inverter_detail is None:
                    print(f"⚠️ No details found for Inverter ID: {inverter_id}. Skipping...")
                    continue

                station_name = inverter_detail.get("stationName")
                print(f"\n📡 Fetching Inverter Details for Station ID: {inverter_id}, Name: {station_name}")

                for month in month_list:
                    month_retry = 0
                    while month_retry <= max_retries:
                        try:
                            print(f"🔄 Fetching data for {month}...")
                            inverter_month_data = await soliscloud.inverter_month(
                                api_key, api_secret,
                                currency="MYR",
                                month=month,
                                inverter_id=inverter_id
                            )

                            if not inverter_month_data:
                                print(f"⚠️ Warning: No data returned for {month}. Skipping...")
                                break
                            print(json.dumps(inverter_month_data, indent=2))

                            extracted_records = []
                            for record in inverter_month_data:
                                extracted_records.append({
                                    "inverter_id": inverter_id,
                                    "station_name": station_name,
                                    "dateStr": record.get("dateStr"),
                                    "money": float(record.get("money")),
                                    "moneyStr": record.get("moneyStr"),
                                    "energy": float(record.get("energy")),
                                    "energyStr": record.get("energyStr", "")
                                })
                            
                            if extracted_records:
                                await insert_inverter_data(extracted_records)

                            break  # exit retry loop on successful fetch

                        except Exception as e:
                            print(f"❌ Error fetching data for {month}: {e}")
                            if month_retry < max_retries:
                                month_retry += 1
                                print(f"⚠️ Retrying {month_retry}/{max_retries} due to error: {e}")
                                await asyncio.sleep(5)
                            else:
                                print(f"❌ Max retries reached for {month}. Skipping this month.")
                                break

                total_inverters += 1
                print(f"🎯 Total Inverters Fetched: {total_inverters}")

        except Exception as e:
            print(f"🚨 General Error: {e}")
            send_telegram_message(f"🚨 General Error: {e}")

async def main():
    with open('config.json', 'r') as file:
        data = json.load(file)

    api_key = data['key']
    api_secret = data['secret'].encode('utf-8')  

    await fetch_all_station(api_key, api_secret)
    print("DONE")

if __name__ == '__main__':
    asyncio.run(main())
