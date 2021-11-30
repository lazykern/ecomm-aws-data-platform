import requests
import time
import json
import sys
import pandas as pd
from dotenv import dotenv_values

with open("config.json") as conf:
    config = json.load(conf)


def stream_data(data: pd.DataFrame, url: str, sleep_time: int = 0, n_rows: int = 10, continue_from_last: bool = True):
    
    if continue_from_last:
        current_index = config["current_index"]
        data = data.loc[current_index:]

    if n_rows:
        data = data.head(n_rows)
    for i, row in enumerate(data.to_dict(orient="records")):
        response = requests.post(url, json=row)
        print(i+current_index, response.status_code)

        time.sleep(sleep_time)

    config["current_index"] += n_rows

    with open("config.json", "w") as conf:
        conf.write(json.dumps(config, indent=4))


AWS_CONFIG = dotenv_values("aws.env")
API_URL = f"https://{AWS_CONFIG['api_id']}.execute-api.us-east-1.amazonaws.com/prod/ecommerce_db/customers"

df = pd.read_csv("data/data.csv", encoding="ISO-8859-1")

stream_data(df, API_URL, sleep_time=1)
