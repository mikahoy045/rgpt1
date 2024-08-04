import csv
import random
import asyncio
import aiohttp
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(override=True)

DATA_PROVIDER_URL = os.getenv("DATA_PROVIDER_URL")+"/events"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "data.csv")
SIMULATION_INTERVAL = (45, 75)

async def read_csv_data():
    with open(CSV_FILE_PATH, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def map_csv_to_post_data(csv_row):
    timestamp = datetime.strptime(csv_row['event_timestamp'], '%Y-%m-%d %H:%M:%S')
    formatted_timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

    return {
        "id": int(csv_row['id']),
        "hotel_id": int(csv_row['hotel_id']),
        "timestamp": formatted_timestamp,
        "rpg_status": int(csv_row['status']),
        "room_id": csv_row['room_reservation_id'],
        "night_of_stay": csv_row['night_of_stay'],
    }

async def send_order(session, order):
    print(f"Sending order to URL: {DATA_PROVIDER_URL}")
    print(f"Sending order: {order}")
    async with session.post(DATA_PROVIDER_URL, json=order) as response:
        response_text = await response.text()
        print(f"Response status: {response.status}")
        print(f"Response headers: {response.headers}")
        print(f"Response body: {response_text}")
        return response_text

async def simulate_orders(csv_data):
    data_index = 0
    data_length = len(csv_data)
    
    async with aiohttp.ClientSession() as session:
        while True:
            num_orders = random.randint(1, 3)
            orders = []
            
            for _ in range(num_orders):
                if data_index >= data_length:
                    print("Reached end of CSV data. Restarting from beginning.")
                    data_index = 0
                    random.shuffle(csv_data)
                
                orders.append(csv_data[data_index])
                data_index += 1
            
            mapped_orders = [map_csv_to_post_data(order) for order in orders]
            tasks = [send_order(session, order) for order in mapped_orders]
            responses = await asyncio.gather(*tasks)
            
            for order, response in zip(mapped_orders, responses):
                print(f"Sent order for hotel_id: {order['hotel_id']}, Response: {response}")
            
            wait_time = random.randint(*SIMULATION_INTERVAL)
            print(f"Waiting for {wait_time} seconds before next simulation...")
            await asyncio.sleep(wait_time)

async def main():
    csv_data = await read_csv_data()
    await simulate_orders(csv_data)

if __name__ == "__main__":
    asyncio.run(main())