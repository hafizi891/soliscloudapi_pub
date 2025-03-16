import asyncio
import json
import requests
import configcentral
from aiohttp import ClientSession
from soliscloud_api import SoliscloudAPI
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime




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
def insert_station_data(station_data):
    try:
        point = Point("station_detail_list") \
            .tag("station_id", station_data.get("id")) \
            .tag("stationName", station_data.get("stationName")) \
            .tag("orgCode", station_data.get("orgCode")) \
            .tag("createDateStr",station_data.get("createDateStr")) \
            .tag("fisPowerTimeStr",station_data.get("fisPowerTimeStr")) \
            .tag("dataTimestamp",station_data.get("dataTimestamp")) \
            .tag("createDateStr",station_data.get("createDateStr")) \
            .tag("picUrl",station_data.get("picUrl")) \
            .field("DailyPowerGen", float(station_data.get("dayEnergy", 0))) \
            .field("monthCarbonDioxide", float(station_data.get("mountCarbonDioxide", 0))) \
            .field("MonthlyPowerGen", float(station_data.get("monthEnergy", 0))) \
            .field("YearlyPowerGen", float(station_data.get("yearEnergy", 0))) \
            .field("TotalPowerGen", float(station_data.get("allEnergy", 0))) \
            .field("FullPowerHours", float(station_data.get("fullHour", 0))) \
            .field("installerEmail", station_data.get("installerEmail")) \
            .field("region", station_data.get("regionStr")) \
            .field("city", station_data.get("cityStr")) \
            .field("installerMobile", station_data.get("installerMobile")) \
            .field("capacity", float(station_data.get("capacity"))) \
            .field("country", station_data.get("countryStr")) \
            .field("county", station_data.get("countyStr")) \
            .field("state", int(station_data.get("state"))) \
            .field("power", float(station_data.get("power"))) \
            .field("dayIncome",station_data.get("dayIncome")) \
            .field("monthInCome",station_data.get("monthInCome")) \
            .field("yearInCome",station_data.get("yearInCome")) \
            .field("allInCome",station_data.get("allInCome")) \
            .field("createDateStr",station_data.get("createDateStr")) \
            .field("homeLoadEnergy",station_data.get("homeLoadEnergy")) \
            .field("homeLoadMonthEnergy",station_data.get("homeLoadMonthEnergy")) \
            .field("homeLoadMonthEnergy",station_data.get("homeLoadMonthEnergy")) \
            .field("weather",station_data.get("weather")) \
            .field("sr",station_data.get("sr")) \
            .field("ss",station_data.get("ss")) \
            .field("weatherType",station_data.get("weatherType")) \
            .field("weatherUpdateDateStr",station_data.get("weatherUpdateDateStr")) \
            .field("condTxtD",station_data.get("condTxtD")) \
            .field("condTxtN",station_data.get("condTxtN")) \
            
            
        # Write to InfluxDB
        write_api.write(bucket=bucket, record=point)
        print(f"✅ Data inserted for Station ID: {station_data.get('id')}")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")

async def fetch_all_station(api_key, api_secret):
    async with ClientSession() as websession:
        try:
            soliscloud = SoliscloudAPI('https://soliscloud.com:13333', websession)
            page_no = 1
            total_inverters = 0
            max_retries = 10

            while True:
                retries = 0
                success = False

                while retries < max_retries:
                    try:
                        await asyncio.sleep(0.5)  # Limit requests to 2 per second

                        station_list = await soliscloud.station_detail_list(api_key, api_secret, page_no=page_no, page_size=100)

                        if not station_list:
                            success = True
                            break

                        total_inverters += len(station_list)

                        for record in station_list:
                            #print("Station:",record)
                            print("✅ Record Fetched")
                            print(json.dumps(record, indent=2))
                            insert_station_data(record)

                        page_no += 1
                        retries = 0  # Reset retries after success
                        success = True

                        if len(station_list) < 100:
                            break

                    except (asyncio.TimeoutError, SoliscloudAPI.SolisCloudError) as e:
                        retries += 1
                        print(f"⚠️ Retry {retries}/{max_retries} due to: {e}")
                        send_telegram_message(f"⚠️ Retry {retries}/{max_retries} due to: {e} : Function Fetch All Station Detail List")
                        await asyncio.sleep(5)  # Optional delay between retries

                    except Exception as e:
                        print(f"❌ Unexpected Error: {e}")
                        send_telegram_message(f"❌ Unexpected Error: {e}")
                        return

                if not success:
                    print("❌ Max retries reached. Stopping API requests.")
                    send_telegram_message("❌ Max retries reached. Stopping API requests.")
                    break

                if len(station_list) < 100:
                    break

            print(f"\n🎯 Total Inverters Fetched: {total_inverters}")

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
        #await asyncio.sleep(180)

if __name__ == '__main__':
    asyncio.run(main())
