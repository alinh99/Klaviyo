import pandas as pd
from flask import Flask
from google.cloud import bigquery
import os
from dotenv import load_dotenv
from data import get_data
from utils import calculate_rate_metric
from datetime import datetime
import tzlocal
import traceback
import sys
import pandas_gbq
import logging
from google.cloud.bigquery_storage import BigQueryReadClient, types

# Run flask app with one default URL and other with 'append_data' which will be scheduled
app = Flask(__name__)

load_dotenv()

dataset_id = os.environ.get("DATASET_ID", "")
project_id = os.environ.get("PROJECT_ID", "")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "klaviyo-423610-43cd801744ab.json"
table_name = os.environ.get("TABLE_NAME", "")
app.config["TIMEOUT"] = 600

@app.route("/")
def hello_klaviyo():
    try:
        return "App is running to load Klaviyo data every 24 hours!"
    except Exception as e:
        return str(e)


def safe_float_conversion(value, default=""):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int_conversion(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


@app.route("/append_klaviyo_data")
def append_klaviyo_data():
    """Return a friendly HTTP greeting."""
    try:
        metrics = get_data()
        delivered_email_count, bounced_email_count, spam_email_count, dropped_email_count, opened_email_count, clicked_email_count, unsubscribed_count, conversion_active_on_site_count, conversion_viewed_product_count, revenue_unique_count, total_revenue_count, total_order_count, delivered_email_unique_count, revenue_count, subscriber_count, new_subscriber_count = metrics

        total_recipients = (
            delivered_email_count
            + bounced_email_count
            + spam_email_count
            + dropped_email_count
        )
        open_rate = str(calculate_rate_metric(opened_email_count, delivered_email_count))
        click_rate = str(calculate_rate_metric(clicked_email_count, delivered_email_count))
        unsubscribed_rate = str(calculate_rate_metric(unsubscribed_count, total_recipients))
        bounce_rate = str(calculate_rate_metric(bounced_email_count, total_recipients))
        delivery_rate = str(calculate_rate_metric(delivered_email_count, total_recipients))
        conversion_active_on_site_rate = str(calculate_rate_metric(
            conversion_active_on_site_count, delivered_email_count
        ))
        conversion_viewed_product_rate = str(calculate_rate_metric(
            conversion_viewed_product_count, delivered_email_count
        ))
        revenue_per_email = str((
            revenue_count / delivered_email_count if delivered_email_count != 0 else 0
        ))
        product_purchase_rate = str(calculate_rate_metric(
            revenue_unique_count, delivered_email_unique_count
        ))
        average_order_value = str((
            total_revenue_count / total_order_count if total_order_count != 0 else 0
        ))
        # open_rate, click_rate, unsubscribed_rate, bounce_rate, delivery_rate, conversion_active_on_site_rate, conversion_viewed_product_rate, revenue_per_email, product_purchase_rate, average_order_value, subscriber_count, new_subscriber_count = dt

        local_timezone = tzlocal.get_localzone()
        current_time = datetime.now(local_timezone)
        date = current_time.strftime("%m-%d-%Y")

        results = {
            "date": [date, date],
            "title": ["Active on Site", "Viewed Product"],
            "open_rate": [open_rate, open_rate],
            "click_rate": [click_rate, click_rate],
            "unsubscribed_rate": [unsubscribed_rate, unsubscribed_rate],
            "bounce_rate": [bounce_rate, bounce_rate],
            "delivery_rate": [delivery_rate, delivery_rate],
            "conversion_rate": [
                conversion_active_on_site_rate,
                conversion_viewed_product_rate,
            ],
            "revenue_per_email": [revenue_per_email, revenue_per_email],
            "product_purchase_rate": [product_purchase_rate, product_purchase_rate],
            "average_order_value": [average_order_value, average_order_value],
            "new_subscribers": [new_subscriber_count, new_subscriber_count],
            "subscriber_counts": [subscriber_count, subscriber_count],
        }

        klaviyo_df = pd.DataFrame(results)

        # client = bigquery.Client()

        print("size of dataframe = " + str(klaviyo_df.shape[0]))

        # job_config = bigquery.LoadJobConfig(
        #     write_disposition="WRITE_APPEND"
        # )

        # job = client.load_table_from_dataframe(
        #     klaviyo_df, f"{project_id}.{dataset_id}.{table_name}", job_config=job_config
        # )
        # job.result()

        pandas_gbq.to_gbq(
            klaviyo_df,
            f"{dataset_id}.{table_name}",
            project_id=f"{project_id}",
            if_exists="append",
        )

        # table = client.get_table(f"{project_id}.{dataset_id}.{table_name}")

        # print(
        #     "Loaded {} rows and {} columns to {}".format(
        #         table.num_rows,
        #         len(table.schema),
        #         f"{project_id}.{dataset_id}.{table_name}",
        #     )
        # )

        print(
            "Total records logged to BigQuery for Klaviyo =  "
            + str(klaviyo_df.shape[0])
        )
        return "Klaviyo Results Logged!"
    except Exception:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(
                "File : %s , Line : %d, Func.Name : %s, Message : %s, Exception type: %s, Exception message: %s"
                % (trace[0], trace[1], trace[2], trace[3], ex_type, ex_value)
            )
        logging.error(
            f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
        )
        return stack_trace

if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
