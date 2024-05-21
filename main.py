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
        client = bigquery.Client()
        metrics = get_data()
        (
            delivered_email_count,
            bounced_email_count,
            spam_email_count,
            dropped_email_count,
            opened_email_count,
            clicked_email_count,
            unsubscribed_count,
            conversion_active_on_site_count,
            conversion_viewed_product_count,
            revenue_unique_count,
            total_revenue_count,
            total_order_count,
            delivered_email_unique_count,
            revenue_count,
            subscriber_count,
            new_subscriber_count,
        ) = metrics

        total_recipients = (
            delivered_email_count
            + bounced_email_count
            + spam_email_count
            + dropped_email_count
        )
        open_rate = str(
            calculate_rate_metric(opened_email_count, delivered_email_count)
        )
        click_rate = str(
            calculate_rate_metric(clicked_email_count, delivered_email_count)
        )
        unsubscribed_rate = str(
            calculate_rate_metric(unsubscribed_count, total_recipients)
        )
        bounce_rate = str(calculate_rate_metric(bounced_email_count, total_recipients))
        delivery_rate = str(
            calculate_rate_metric(delivered_email_count, total_recipients)
        )
        conversion_active_on_site_rate = str(
            calculate_rate_metric(
                conversion_active_on_site_count, delivered_email_count
            )
        )
        conversion_viewed_product_rate = str(
            calculate_rate_metric(
                conversion_viewed_product_count, delivered_email_count
            )
        )
        revenue_per_email = str(
            (revenue_count / delivered_email_count if delivered_email_count != 0 else 0)
        )
        product_purchase_rate = str(
            calculate_rate_metric(revenue_unique_count, delivered_email_unique_count)
        )
        average_order_value = str(
            (total_revenue_count / total_order_count if total_order_count != 0 else 0)
        )

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

        schema = [
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("open_rate", "STRING"),
            bigquery.SchemaField("click_rate", "STRING"),
            bigquery.SchemaField("unsubscribed_rate", "STRING"),
            bigquery.SchemaField("bounce_rate", "STRING"),
            bigquery.SchemaField("delivery_rate", "STRING"),
            bigquery.SchemaField("conversion_rate", "STRING"),
            bigquery.SchemaField("revenue_per_email", "STRING"),
            bigquery.SchemaField("product_purchase_rate", "STRING"),
            bigquery.SchemaField("average_order_value", "STRING"),
            bigquery.SchemaField("new_subscribers", "INTEGER"),
            bigquery.SchemaField("subscriber_counts", "INTEGER"),
        ]

        klaviyo_df = pd.DataFrame(results)
        klaviyo_df = klaviyo_df.drop_duplicates()
        klaviyo_df.drop_duplicates(inplace=True)

        # Check if the table exists, create if not
        table_ref = client.dataset(dataset_id).table(table_name)
        try:
            table = client.get_table(table_ref)
            print(f"Table {table_name} exists.")
            # Check if the existing schema matches the provided schema
            if table.schema == schema:
                print(f"Schema for table {table_name} exists and matches the provided schema.")
            else:
                # Update the table schema if it doesn't match
                table.schema = schema
                client.update_table(table, ["schema"])
                print(f"Updated schema for table {table_name}.")
        except:
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)

            if table.schema == schema:
                print(
                    f"Schema for table {table_name} exists and matches the provided schema."
                )
            else:
                # Update the table schema if it doesn't match
                table.schema = schema
                client.update_table(table, ["schema"])
                print(f"Updated schema for table {table_name}.")

            print(f"Created table {table_name}.")

        temp_table_name = "temp_klaviyo"

        job_config = bigquery.LoadJobConfig()

        # Load data into temp_klaviyo table
        job = client.load_table_from_dataframe(
            klaviyo_df,
            f"{project_id}.{dataset_id}.{temp_table_name}",
            job_config=job_config,
        )
        job.result()
        print(f"Loaded {klaviyo_df.shape[0]} rows into {temp_table_name}")

        # Remove duplicates from klaviyo table
        dedup_query = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_name}` AS
        SELECT DISTINCT * FROM `{project_id}.{dataset_id}.{table_name}`;
        """
        dedup_job = client.query(dedup_query)
        dedup_job.result()
        print(f"Removed duplicates from {table_name}")

        # Merge new data into klaviyo table
        merge_query = f"""
        INSERT INTO `{project_id}.{dataset_id}.{table_name}`
        SELECT * FROM `{project_id}.{dataset_id}.{temp_table_name}`
        WHERE CONCAT(date, ',', title) NOT IN (
            SELECT CONCAT(date, ',', title) FROM `{project_id}.{dataset_id}.{table_name}`
        );
        """
        merge_job = client.query(merge_query)
        merge_job.result()
        print(f"Merged data from {temp_table_name} into {table_name}")

        # Delete the temp_klaviyo table
        client.delete_table(f"{project_id}.{dataset_id}.{temp_table_name}")
        print(f"Deleted table {temp_table_name}")

        return "Klaviyo Results Logged!"
    except Exception as ex:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(
                f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
            )
        logging.error(
            f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
        )
        return str(stack_trace)

if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
