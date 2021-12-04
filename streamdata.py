import requests
import time
import json
import pandas as pd
from dotenv import dotenv_values
from  aws_utils.my_aws_package import AWS_CONFIG



with open("config.json") as conf:
    config = json.load(conf)


def stream_data(data: pd.DataFrame, url: str, sleep_time: int = 0,
                n_invoice: int = 10, continue_from_last: bool = True):

    invoices = pd.unique(data["InvoiceNo"])

    current_index = 0
    if continue_from_last:
        current_index = config["current_index"]
        invoices = invoices[current_index:]
        


    invoices = invoices[:n_invoice]

    for i, key in enumerate(invoices):
        body_json = data[data["InvoiceNo"] == key].to_dict(orient="records")
        try:
            response = requests.post(url, json=body_json)
            lg = response.status_code
            
        except Exception as e:
            lg = e
            
        print(i+current_index, key, lg)

        if continue_from_last:
            config["current_index"] += 1
        with open("config.json", "w") as conf:
            conf.write(json.dumps(config, indent=4))
        time.sleep(sleep_time)


API_URL = f"https://{AWS_CONFIG['api_id']}.execute-api.us-east-1.amazonaws.com/prod/ecommerce_db"

df = pd.read_csv("data/data.csv", encoding="ISO-8859-1",
                 dtype={"InvoiceNo": str, "CustomerID": str})


stream_data(df, API_URL, sleep_time=60, n_invoice=100)