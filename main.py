import pandas as pd
from data import (
    open_rate,
    click_rate,
    unsubscribed_rate,
    bounce_rate,
    delivery_rate,
    conversion_active_on_site_rate,
    conversion_viewed_product_rate,
    revenue_per_email,
    product_purchase_rate,
    average_order_value,
    new_subscriber_count,
    subscriber_count,
    cutoff_time,
)
from flask import Flask
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Run flask app with one default URL and other with 'append_data' which will be scheduled
app = Flask(__name__)

load_dotenv()

dataset_id = os.environ.get("DATASET_ID", "")
project_id = os.environ.get("PROJECT_ID", "")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "klaviyo-423610-43cd801744ab.json"
table_name = os.environ.get("TABLE_NAME", "")


@app.route("/")
def hello_klaviyo():
    try:
        return "App is running to load Klaviyo data every 24 hours!"
    except Exception as e:
        return str(e)


@app.route("/append_klaviyo_data")
def append_klaviyo_data():
    """Return a friendly HTTP greeting."""
    try:
        
        date = str(cutoff_time.month) + "-" + str(cutoff_time.day) + "-" + str(cutoff_time.year)
        results = {
            "Date": [date, date],
            "Title": ["Active on Site", "Viewed Product"],


            "Open Rate": [
                str(open_rate) + "%",
                str(open_rate) + "%",
            ],
            "Click Rate": [
                str(click_rate) + "%",
                str(click_rate) + "%",
            ],
            "Unsubscribed Rate": [str(unsubscribed_rate) + "%", str(unsubscribed_rate) + "%"],
            "Bounce Rate": [str(bounce_rate) + "%", str(bounce_rate) + "%"],
            "Delivery Rate": [str(delivery_rate) + "%", str(delivery_rate) + "%"],
            "Conversion Rate": [
                str(conversion_active_on_site_rate) + "%",


                str(conversion_viewed_product_rate) + "%",
            ],


            "Revenue Per Email": [str(revenue_per_email), str(revenue_per_email)],
            "Product Purchase Rate": [
                str(product_purchase_rate) + "%",
                str(product_purchase_rate) + "%",
            ],
            "Average Order Value": [
                str(average_order_value),
                str(average_order_value),
            ],
            "New Subscribers": [
                str(new_subscriber_count),
                str(new_subscriber_count),
            ],
            "Subscriber Counts": [str(subscriber_count), str(subscriber_count)],
        }

        klaviyo_df = pd.DataFrame(results)
        client = bigquery.Client()
        
        print("size of dataframe = " + str(klaviyo_df.shape[0]))
        
        job = client.load_table_from_dataframe(
            klaviyo_df, f"{project_id}.{dataset_id}.{table_name}"
        )
        job.result()
        
        print("Total records logged to BigQuery for Klaviyo =  " + str(klaviyo_df.shape[0]))
        return "Klaviyo Results Logged!"
    except Exception as e:
        return str(e)

if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=5007, debug=False)
