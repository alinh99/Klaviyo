from utils import (
    get_metrics,
    get_pagination_metrics,
    calculate_rate_metric,
)
import os
from dotenv import load_dotenv
import tzlocal
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import traceback
import sys
import logging

load_dotenv()

klaviyo_api_key = os.environ.get("KLAVIYO_API_KEY", "")
klaviyo_url = "https://a.klaviyo.com/api"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

statistic_url = (
    f"{klaviyo_url}/metrics/?fields[metric]=name,updated,created,integration"
)
statistics = get_pagination_metrics(statistic_url, klaviyo_api_key)

report_url = f"{klaviyo_url}/metric-aggregates"



def convert_to_local_timezone(iso: str, local_timezone: ZoneInfo):
    """Convert any timezone to local timezone."""
    if iso is None:
        return None
    try:
        dt = datetime.fromisoformat(iso).astimezone(local_timezone)
        return dt
    except ValueError as e:
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
        print(f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}")
        return f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}"
    except Exception as e:
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
        print(f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}")
        return f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}"


def get_subscribers(subscribers: list, cutoff_datetime: datetime, local_timezone: ZoneInfo) -> int:
    subscriber_today = []
    try:
        for subscriber in subscribers:
            if "attributes" in subscriber:
                subscriber_atributes = subscriber["attributes"]
                if "subscriptions" in subscriber_atributes:
                    subscriber_subscription = subscriber_atributes["subscriptions"]
                    if "email" in subscriber_subscription:
                        subscriber_email = subscriber_subscription["email"]
                        if "marketing" in subscriber_email:
                            subscriber_marketing = subscriber_email["marketing"]
                            if "consent_timestamp" in subscriber_marketing:
                                subscriber_timestamp = subscriber_marketing["consent_timestamp"]
                                if subscriber_timestamp != None:
                                    subscriber_updated_local = convert_to_local_timezone(subscriber_timestamp, local_timezone)
                                    if (
                                        subscriber_updated_local is not None
                                        and subscriber_updated_local <= cutoff_datetime
                                        and subscriber_marketing["consent"] == "SUBSCRIBED"
                                    ):
                                        subscriber_today.append(subscriber)
        return len(subscriber_today)
    except Exception as e:
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
        return 0
    

def get_data() -> list:
    try:
        # Initialize variables
        delivered_email_count = int(0)
        bounced_email_count = int(0)
        spam_email_count = int(0)
        dropped_email_count = int(0)
        opened_email_count = int(0)
        clicked_email_count = int(0)
        unsubscribed_count = int(0)
        conversion_active_on_site_count = int(0)
        conversion_viewed_product_count = int(0)
        revenue_unique_count = int(0)
        total_revenue_count = int(0)
        total_order_count = int(0)
        delivered_email_unique_count = int(0)
        revenue_count = int(0)
        new_subscriber = int(0)
        subscriber_today = int(0)
        subscriber_before_today = int(0)
        new_subscribers = []
        revenues = []
        delivered_emails = []
        delivered_email_uniques = []
        dropped_emails = []
        spam_emails = []
        opened_emails = []
        clicked_emails = []
        bounced_emails = []
        conversion_viewed_products = []
        conversion_active_on_sites = []
        revenue_uniques = []
        total_orders = []
        total_revenues = []
        
        unsubscriber_count_today = 0

        local_timezone = tzlocal.get_localzone()

        current_time = datetime.now(local_timezone)
        cutoff_time = datetime(
            current_time.year,
            current_time.month,
            current_time.day,
            23,
            59,
            59,
            59,
            tzinfo=local_timezone,
        )
        # cutoff_time = datetime(
        #     cutoff_time.
        #     year,
        #     cutoff_time.month,
        #     cutoff_time.day,
        #     23,
        #     59,
        #     59,
        #     59,
        #     tzinfo=local_timezone,
        # )
        
        previous_time = datetime.now(local_timezone)
        cutoff_previous_time = datetime(
            previous_time.year,
            previous_time.month,
            previous_time.day,
            0,
            0,
            0,
            0,
            tzinfo=local_timezone,
        )
        # cutoff_previous_time = datetime(
        #     2024,
        #     6,
        #     25,
        #     0,
        #     0,
        #     0,
        #     0,
        #     tzinfo=local_timezone,
        # )
        

        processed_metric_ids = set()
        for stat in statistics:
            metric_id = stat["id"]

            if metric_id in processed_metric_ids:
                continue
            processed_metric_ids.add(metric_id)

            if stat["attributes"]["name"] == "Received Email":
                delivered_emails.extend(get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key
                ))

                for delivered_email in delivered_emails:
                    if delivered_email["dimensions"] == ["UjjW7L"]:  # ID of report
                        delivered_email_count = sum(delivered_email["measurements"]["count"])

                delivered_email_uniques.extend(get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key

                ))
                for delivered_email_unique in delivered_email_uniques:
                    delivered_email_unique_count = sum(
                        delivered_email_unique["measurements"]["unique"]
                    )
                
                logging.info(f"Get Received Email successfully on {cutoff_time.strftime('%Y-%m-%d')}")
            
            if stat["attributes"]["name"] == "Dropped Email":
                dropped_emails.extend(get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key

                ))
                for dropped_email in dropped_emails:
                    dropped_email_count = sum(dropped_email["measurements"]["count"])
                
                logging.info(f"Get Dropped Email successfully on {cutoff_time.strftime('%Y-%m-%d')}")
            
            if stat["attributes"]["name"] == "Marked Email as Spam":
                spam_emails.extend(get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key

                ))
                for spam_email in spam_emails:
                    spam_email_count = sum(spam_email["measurements"]["count"])
                
                logging.info(f"Get Spam Email successfully on {cutoff_time.strftime('%Y-%m-%d')}")
            
            if stat["attributes"]["name"] == "Opened Email":
                opened_emails.extend(get_metrics(
                    ["$message_send_cohort"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key

                ))
                for opened_email in opened_emails:
                    # if opened_email["dimensions"] == ["UjjW7L"]: # ID of report
                    opened_email_count = sum(opened_email["measurements"]["unique"])
                
                logging.info(f"Get Opened Email successfully on {cutoff_time.strftime('%Y-%m-%d')}")
                
            if stat["attributes"]["name"] == "Clicked Email":
                clicked_emails.extend(get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key

                ))
                for clicked_email in clicked_emails:
                    clicked_email_count = sum(clicked_email["measurements"]["unique"])
                
                logging.info(f"Get Clicked Email successfully on {cutoff_time.strftime('%Y-%m-%d')}")

            if stat["attributes"]["name"] == "Unsubscribed from List":
                unsubscribed_url = f"{klaviyo_url}/profiles/?additional-fields[profile]=subscriptions&fields[profile]=title&page[size]=100"
                unsubscribed_data = get_pagination_metrics(unsubscribed_url, klaviyo_api_key)

                for unsubscribed in unsubscribed_data:
                    unsubscribed_consent = unsubscribed["attributes"]["subscriptions"]["email"]["marketing"]["consent"]
                    if (
                        unsubscribed_consent == "UNSUBSCRIBED"
                    ):
                        unsubscribed_count += 1
                    
                    unsubscribed_consent_timestamp = unsubscribed["attributes"]["subscriptions"]["email"]["marketing"]["consent_timestamp"]
                    if (
                        unsubscribed_consent
                        == "UNSUBSCRIBED"
                    ) and unsubscribed_consent_timestamp != None:
                        unsubscriber_updated_local = convert_to_local_timezone(unsubscribed_consent_timestamp, local_timezone)
                        if unsubscriber_updated_local.strftime("%Y-%m-%d") == cutoff_time.strftime("%Y-%m-%d"):
                            unsubscriber_count_today += 1
                if unsubscriber_count_today != 0:
                    new_subscriber = subscriber_today - subscriber_before_today - unsubscriber_count_today
                
                logging.info(f"Get Unsubscribed successfully on {cutoff_time.strftime('%Y-%m-%d')}")

            if stat["attributes"]["name"] == "Bounced Email":
                bounced_emails.extend(get_metrics(
                    ["Bounce Type"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key
                ))
                for bounced_email in bounced_emails:
                    bounced_email_count = sum(bounced_email["measurements"]["unique"])
                
                logging.info(f"Get Bounced Email successfully on {cutoff_time.strftime('%Y-%m-%d')}")
                
            if stat["attributes"]["name"] == "Viewed Product":
                conversion_viewed_products.extend(get_metrics(
                    ["$attributed_message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key
                ))
                for conversion_viewed_product in conversion_viewed_products:
                    if conversion_viewed_product["dimensions"] != [""]:
                        conversion_viewed_product_count = sum(
                            conversion_viewed_product["measurements"]["unique"]
                        )
                
                logging.info(f"Get Viewed Product successfully on {cutoff_time.strftime('%Y-%m-%d')}")

            if stat["attributes"]["name"] == "Active on Site":
                conversion_active_on_sites.extend(get_metrics(
                    ["$attributed_message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key
                ))
                for conversion_active_on_site in conversion_active_on_sites:
                    if conversion_active_on_site["dimensions"] != [""]:
                        conversion_active_on_site_count = sum(
                            conversion_active_on_site["measurements"]["unique"]
                        )
                
                logging.info(f"Get Active on Site successfully on {cutoff_time.strftime('%Y-%m-%d')}")

            if stat["attributes"]["name"] == "Placed Order":
                revenues.extend( get_metrics(
                    ["$attributed_message", "$attributed_flow"],
                    metric_id,
                    report_url,
                    ["sum_value"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                        'not(equals($attributed_message,""))',
                    ],
                    klaviyo_api_key
                ))
                for revenue in revenues:
                    revenue_count = sum(revenue["measurements"]["sum_value"])
                
                revenue_uniques.extend( get_metrics(
                    ["$attributed_message", "$attributed_flow"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                        'not(equals($attributed_message,""))',
                    ],
                    klaviyo_api_key
                ))
                for revenue_unique in revenue_uniques:
                    revenue_unique_count = sum(revenue_unique["measurements"]["unique"])
                
                logging.info(f"Get revenue successfully on {cutoff_time.strftime('%Y-%m-%d')}")
                
                total_orders.extend( get_metrics(
                    ["$flow"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key
                ))
                for total_order in total_orders:
                    total_order_count = sum(total_order["measurements"]["count"])
                
                logging.info(f"Get total_order successfully on {cutoff_time.strftime('%Y-%m-%d')}")

                total_revenues.extend( get_metrics(
                    ["$flow"],
                    metric_id,
                    report_url,
                    ["sum_value"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        f"less-than(datetime,{cutoff_time.strftime('%Y-%m-%d')})",
                    ],
                    klaviyo_api_key
                ))
                for total_revenue in total_revenues:
                    total_revenue_count = sum(total_revenue["measurements"]["sum_value"])
                
                logging.info(f"Get total_revenue successfully on {cutoff_time.strftime('%Y-%m-%d')}")

            if stat["attributes"]["name"] == "Subscribed to List":
                segment_url = f"{klaviyo_url}/segments/?fields[segment]=name"
                segments = get_pagination_metrics(segment_url, klaviyo_api_key)
                for seg in segments:
                    if "attributes" in seg:
                        seg_attributes = seg["attributes"]
                        if "name" in seg_attributes:
                            seg_name = seg["attributes"]["name"]
                            if seg_name == "All Subscribers Segment":
                                subscriber_id = seg["id"]
                                new_subscriber_url = f"{klaviyo_url}/segments/{subscriber_id}/profiles/?additional-fields[profile]=subscriptions,predictive_analytics&fields[profile]=created,updated,location,email&page[size]=100"
                                new_subscriber_metrics = get_pagination_metrics(
                                    new_subscriber_url, klaviyo_api_key
                                )
                                new_subscribers.extend(new_subscriber_metrics)

                subscriber_before_today = get_subscribers(
                    new_subscribers, cutoff_previous_time, local_timezone
                )
                subscriber_today = get_subscribers(new_subscribers, cutoff_time, local_timezone)
                
                new_subscriber = subscriber_today - subscriber_before_today
                
                logging.info(f"Get subscribers and new_subscribers successfully on {cutoff_time.strftime('%Y-%m-%d')}")
        return [
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
            subscriber_today,
            new_subscriber
        ]
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
        print(f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}")
        return f"Exception type: {ex_type}, Exception message: {ex_value}\nStack trace:\n{stack_trace_message}"