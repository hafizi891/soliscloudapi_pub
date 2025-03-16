import asyncio
import json
import requests
import configcentral
from aiohttp import ClientSession
from soliscloud_api import SoliscloudAPI
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime,timedelta
from soliscloud_api.helpers import Helpers

def get_day_list(start_year=2024):
    """Generate a list of daily dates from start_year until today in YYYY-MM-DD format."""
    start_date = datetime(start_year, 1, 1)  # Start from January 1st of the given year
    today = datetime.now()  # Get today's date

    # Generate a list of dates between start_date and today
    days = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
            for i in range((today - start_date).days + 1)]
    
    return days


# Access the variables
TOKEN = configcentral.TELEGRAM_TOKEN
CHAT_ID = configcentral.CHAT_ID

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
    send_telegram_message(f"❌ Max retries reached for {message}.")
    return None

def send_telegram_message(message):
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
            print("⚠️ Telegram Failed")
    except Exception as e:
        print(f"🚨 Telegram Error: {e}")

# Function to Insert Data into InfluxDB
async def insert_inverter_data(station_data_list):
    #print("INSERT FUNCTION")
    
    #print(json.dumps(station_data_list, indent=2))
    
    try:
        for station_data in station_data_list:  # Loop through each record in the list
            point = Point("Inverter_Daily") \
                .tag("inverter_id", station_data.get("inverter_id")) \
                .tag("station_name", station_data.get("stationname")) \
                .tag("timeStr", station_data.get("timeStr")) \
                .tag("recorded_time", station_data.get("time")) \
                .field("pac", station_data.get("pac")) \
                .field("eToday", float(station_data.get("eToday"))) \
                .field("eTotal", float(station_data.get("eTotal"))) \
                
            # Write to InfluxDB
            write_api.write(bucket=bucket, record=point)
            print(f"✅ Data inserted for Inverter ID: {station_data.get('inverter_id')}")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")


async def fetch_all_station(api_key, api_secret):
    month_list = get_day_list()
    retries = 0
    max_retries = 3
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
                    f"Inverter detail for {inverter_id}"
                )

                if inverter_detail is None:
                    print(f"⚠️ No details found for Inverter ID: {inverter_id}. Skipping...")
                    continue

                station_name = inverter_detail.get("StationName")
                print(f"\n📡 Fetching Inverter Details data for Station ID: {inverter_id}, Name: {station_name}")

                for month in month_list:
                    month_retry = 0
                    while month_retry <= max_retries:
                        try:
                            print(f"🔄 Fetching data for {month}...")
                            inverter_month_data = await soliscloud.inverter_day(
                                api_key, api_secret,
                                currency="MYR",
                                time=month,
                                time_zone=8,
                                inverter_id=inverter_id
                            )

                            if not inverter_month_data:
                                print(f"⚠️ Warning: No data returned for {month}. Skipping...")
                                break
                            #print(json.dumps(inverter_month_data, indent=2))

                            extracted_records = []
                            for record in inverter_month_data:
                                #print(f"📄 Processing record: {record}")
                                extracted_records.append({
                                    "inverter_id": inverter_id,
                                    "station_name": station_name,
                                    "time": record.get("time"),
                                    "timeStr": record.get("timeStr"),
                                    "pac": float(record.get("pac")),
                                    "eToday": record.get("eToday"),
                                    "eTotal": record.get("eTotal")
                                })
                            
                            if extracted_records:
                                await retry_operation(
                                    
                                    lambda: insert_inverter_data(extracted_records),
                                    retries,
                                    max_retries,
                                    f"Insert data for Inverter ID {inverter_id} and Years {month}"
                                )

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

                    if month_retry >= max_retries:
                        send_telegram_message(f"❌ Max retries reached for {month}. Skipping this month.")

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
