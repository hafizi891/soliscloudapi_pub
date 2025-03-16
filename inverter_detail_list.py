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
def insert_inverter_data(inverter_data):
    try:
        point = Point("inverter_detail_list") \
            .tag("inverter_id", inverter_data.get("id")) \
            .tag("model", inverter_data.get("model")) \
            .tag("stationName", inverter_data.get("stationName")) \
            .tag("stationId", str(inverter_data.get("stationId"))) \
            .tag("sn", str(inverter_data.get("sn"))) \
            .tag("sno", str(inverter_data.get("sno"))) \
            .tag("collectorsn", str(inverter_data.get("collectorsn"))) \
            .tag("collectorId", str(inverter_data.get("collectorId"))) \
            .tag("dataTimestampStr", str(inverter_data.get("dataTimestampStr"))) \
            .tag("inverterMeterModel", int(inverter_data.get("inverterMeterModel"))) \
            .field("pac", float(inverter_data.get("pac", 0))) \
            .tag("timeStr", inverter_data.get("timeStr")) \
            .field("pacPec", float(inverter_data.get("pacPec", 0))) \
            .field("eToday", float(inverter_data.get("eToday", 0))) \
            .field("eMonth", float(inverter_data.get("eMonth", 0))) \
            .field("eYear", float(inverter_data.get("eYear", 0))) \
            .field("eTotal", float(inverter_data.get("eTotal", 0))) \
            .field("fullHour", float(inverter_data.get("fullHour", 0))) \
            .field("timeZone", float(inverter_data.get("timeZone", 0))) \
            .field("power", float(inverter_data.get("power", 0))) \
            .field("dcBus", float(inverter_data.get("dcBus", 0))) \
            .field("porwerPercent", float(inverter_data.get("porwerPercent", 0))) \
            .field("apparentPower", float(inverter_data.get("apparentPower", 0))) \
            .field("dcPac", float(inverter_data.get("dcPac", 0))) \
            .field("tempName", inverter_data.get("tempName")) \
            .field("state", inverter_data.get("state")) \
            .field("acOutputType", int(inverter_data.get("acOutputType", 0))) \
            .field("dcInputType", int(inverter_data.get("dcInputType", 0))) \
            .field("currentState", inverter_data.get("currentState")) \
            .field("allInCome", inverter_data.get("allInCome")) \
            .field("updateShelfEndTimeStr", inverter_data.get("updateShelfEndTimeStr")) \
            .field("shelfState", inverter_data.get("shelfSstate")) \
            .field("alarmState", inverter_data.get("alarmState")) \
            .field("stateExceptionFlag", inverter_data.get("stateExceptionFlag")) \
            .field("gridPurchasedTodayEnergy", inverter_data.get("gridPurchasedTodayEnergy")) \
            .field("pacPec", inverter_data.get("pacPec")) \
            .field("gridSellTodayEnergy", inverter_data.get("gridSellTodayEnergy")) \
            .field("uAc1", inverter_data.get("uAc1")) \
            .field("uAc2", inverter_data.get("uAc2")) \
            .field("uAc3", inverter_data.get("uAc3"))\
            .field("iAc1", inverter_data.get("iAc1")) \
            .field("iAc2", inverter_data.get("iAc2")) \
            .field("iAc3", inverter_data.get("iAc3")) \
            .field("uPv1", inverter_data.get("uPv1")) \
            .field("iPv1", inverter_data.get("iPv1")) \
            .field("insulationResistance", inverter_data.get("insulationResistance")) \
            .field("iLeakLimt", inverter_data.get("iLeakLimt")) \
            .field("inverterTemperature", inverter_data.get("inverterTemperature")) \
            .field("powerFactor", inverter_data.get("powerFactor")) \
            .field("fac", inverter_data.get("fac")) \
            .time(datetime.utcnow(), WritePrecision.S)

        # Write to InfluxDB
        write_api.write(bucket=bucket, record=point)
        print(f"✅ Data inserted for Inverter ID: {inverter_data.get('id')}")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")

async def fetch_all_inverters(api_key, api_secret):
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

                        inverter_list = await soliscloud.inverter_detail_list(api_key, api_secret, page_no=page_no, page_size=100)

                        if not inverter_list:
                            success = True
                            break

                        total_inverters += len(inverter_list)

                        for record in inverter_list:
                            print("✅ Record Fetched")
                            #print(json.dumps(record, indent=2))
                            insert_inverter_data(record)

                        page_no += 1
                        retries = 0  # Reset retries after success
                        success = True

                        if len(inverter_list) < 100:
                            break

                    except (asyncio.TimeoutError, SoliscloudAPI.SolisCloudError) as e:
                        retries += 1
                        print(f"⚠️ Retry {retries}/{max_retries} due to: {e}")
                        send_telegram_message(f"⚠️ Retry {retries}/{max_retries} due to: {e} :: Inverter Detail List")
                        await asyncio.sleep(5)  # Optional delay between retries

                    except Exception as e:
                        print(f"❌ Unexpected Error: {e}")
                        send_telegram_message(f"❌ Unexpected Error: {e}")
                        return

                if not success:
                    print("❌ Max retries reached. Stopping API requests.")
                    send_telegram_message("❌ Max retries reached. Stopping API requests.")
                    break

                if len(inverter_list) < 100:
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

    
    await fetch_all_inverters(api_key, api_secret)
    print("DONE")
        #await asyncio.sleep(180)

if __name__ == '__main__':
    asyncio.run(main())
