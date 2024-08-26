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
import logging

# Run flask app with one default URL and other with 'append_data' which will be scheduled
app = Flask(__name__)

load_dotenv()

dataset_id = os.environ.get("DATASET_ID", "")
project_id = os.environ.get("PROJECT_ID", "")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "klaviyo-423610-43cd801744ab.json"
table_name = os.environ.get("TABLE_NAME", "")
app.config["TIMEOUT"] = 1200

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
        local_timezone = tzlocal.get_localzone()
        current_time = datetime.now(local_timezone)
        # current_time = datetime(2024, 6, 25)
        date = current_time.strftime("%Y-%m-%d")

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

        print("Load data successfully")
        logging.info("Load data successfully")

        total_recipients = (
            delivered_email_count + bounced_email_count + spam_email_count + dropped_email_count
        )
        open_rate = calculate_rate_metric(opened_email_count, delivered_email_count)
        click_rate = calculate_rate_metric(clicked_email_count, delivered_email_count)
        unsubscribed_rate = calculate_rate_metric(unsubscribed_count, total_recipients)
        bounce_rate = calculate_rate_metric(bounced_email_count, total_recipients)
        delivery_rate = calculate_rate_metric(delivered_email_count, total_recipients)
        conversion_active_on_site_rate = calculate_rate_metric(
            conversion_active_on_site_count, delivered_email_count
        )
        conversion_viewed_product_rate = calculate_rate_metric(
            conversion_viewed_product_count, delivered_email_count
        )
        revenue_per_email = revenue_count / delivered_email_count if delivered_email_count != 0 else 0
        product_purchase_rate = calculate_rate_metric(
            revenue_unique_count, delivered_email_unique_count
        )
        average_order_value = total_revenue_count / total_order_count if total_order_count != 0 else 0

        print("Calculate data successfully")
        logging.info("Calculate data successfully")

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
            bigquery.SchemaField("open_rate", "FLOAT"),
            bigquery.SchemaField("click_rate", "FLOAT"),
            bigquery.SchemaField("unsubscribed_rate", "FLOAT"),
            bigquery.SchemaField("bounce_rate", "FLOAT"),
            bigquery.SchemaField("delivery_rate", "FLOAT"),
            bigquery.SchemaField("conversion_rate", "FLOAT"),
            bigquery.SchemaField("revenue_per_email", "FLOAT"),
            bigquery.SchemaField("product_purchase_rate", "FLOAT"),
            bigquery.SchemaField("average_order_value", "FLOAT"),
            bigquery.SchemaField("new_subscribers", "INTEGER"),
            bigquery.SchemaField("subscriber_counts", "INTEGER"),
        ]

        klaviyo_df = pd.DataFrame(results)

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
        logging.info(f"Loaded {klaviyo_df.shape[0]} rows into {temp_table_name}")

        # Merge the temp_klaviyo table into the main table
        merge_query = f"""
        MERGE `{project_id}.{dataset_id}.{table_name}` T
        USING (
            SELECT
                date,
                title,
                MAX(open_rate) AS open_rate,
                MAX(click_rate) AS click_rate,
                MAX(unsubscribed_rate) AS unsubscribed_rate,
                MAX(delivery_rate) AS delivery_rate,
                MAX(bounce_rate) AS bounce_rate,
                MAX(conversion_rate) AS conversion_rate,
                MAX(revenue_per_email) AS revenue_per_email,
                MAX(product_purchase_rate) AS product_purchase_rate,
                MAX(average_order_value) AS average_order_value,
                MAX(new_subscribers) AS new_subscribers,
                MAX(subscriber_counts) AS subscriber_counts
            FROM `{project_id}.{dataset_id}.{temp_table_name}`
            GROUP BY date, title
        ) S
        ON T.date = S.date AND T.title = S.title
        WHEN MATCHED THEN
            UPDATE SET
                T.open_rate = S.open_rate,
                T.click_rate = S.click_rate,
                T.unsubscribed_rate = S.unsubscribed_rate,
                T.delivery_rate = S.delivery_rate,
                T.bounce_rate = S.bounce_rate,
                T.conversion_rate = S.conversion_rate,
                T.revenue_per_email = S.revenue_per_email,
                T.product_purchase_rate = S.product_purchase_rate,
                T.average_order_value = S.average_order_value,
                T.new_subscribers = S.new_subscribers,
                T.subscriber_counts = S.subscriber_counts
        WHEN NOT MATCHED THEN
            INSERT (date, title, open_rate, click_rate, unsubscribed_rate, delivery_rate, bounce_rate, conversion_rate, revenue_per_email, product_purchase_rate, average_order_value, new_subscribers, subscriber_counts)
            VALUES (S.date, S.title, S.open_rate, S.click_rate, S.unsubscribed_rate, S.delivery_rate, S.bounce_rate, S.conversion_rate, S.revenue_per_email, S.product_purchase_rate, S.average_order_value, S.new_subscribers, S.subscriber_counts);
        """

        merge_job = client.query(merge_query)
        merge_job.result()

        print(f"Merged data from {temp_table_name} into {table_name}")
        logging.info(f"Merged data from {temp_table_name} into {table_name}")

        # Remove duplicates (if any) from the table
        dedup_query = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_name}` AS
        SELECT DISTINCT date, title, open_rate, click_rate, unsubscribed_rate, delivery_rate, bounce_rate, conversion_rate, revenue_per_email, product_purchase_rate, average_order_value, new_subscribers, subscriber_counts
        FROM `{project_id}.{dataset_id}.{table_name}`;
        """
        dedup_job = client.query(dedup_query)
        dedup_job.result()
        print(f"Removed duplicates from {table_name}")
        logging.info(f"Removed duplicates from {table_name}")

        return f"Klaviyo Results Logged on {date}!"
    except Exception:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append(
                f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
            )
        stack_trace_message = "\n".join(stack_trace)
        logging.error(
            f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}"
        )
        return f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}"

if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=False)
