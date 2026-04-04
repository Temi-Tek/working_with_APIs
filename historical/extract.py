import asyncio # asynchronous operation library
import aiohttp # the request of asynchronous HTTP requests
import os # operating system interfaces
import json # JSON parsing and manipulation
from datetime import date, timedelta 

# Define the bronze layer: A folder
BRONZE_DIR = "data/bronze"
os.makedirs(BRONZE_DIR, exist_ok=True)

# Limit the concurrent execution to the defined number
limitter = asyncio.Semaphore(3)
async def api_request(session, target_date):
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"
    raw_data_path = os.path.join(BRONZE_DIR, f"{target_date}.json")

    if os.path.exists(raw_data_path):
        print(f"Data for {target_date} already exists. Skip")
        return  # skip if the file already exists
    async with limitter:
        for attempts in range(3):  # retry up to 3 times
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as response:
                    if response.status == 200:
                        resp_json = await response.json()
                        with open(raw_data_path, 'w') as file:
                            data = resp_json.get("data")  # Fixed: dict access instead of function call
                            json.dump(resp_json, file, indent=4)
                        print(f"Data for {target_date} saved successfully.")
                        return
                    elif response.status == 429:  # Too Many Requests
                        print(f"Encountered rate limit for {target_date}. Retrying after a delay...")
                        await asyncio.sleep(5 * (attempts + 1))  # Wait before retrying
                    else:
                        print(f"Failed to fetch data for {target_date}. Status code: {response.status}")
            except Exception as e:
                print(f"Tried making requests for {target_date} in {attempts + 1} attempts: {e}")
                await asyncio.sleep(2)  # Wait before retrying
        print(f"Failed to fetch data for {target_date} after 3 attempts.")  

# To run the asynchronous operations
async def run_operations():
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)

    connector = aiohttp.TCPConnector(limit=10, ssl=False)  # Limit the number of concurrent connections
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [] # this gathers all the tasks and request date
        current = start_date
        while current <= end_date:
            tasks.append(api_request(session, target_date=current))
            current += timedelta(days=1)
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(run_operations())