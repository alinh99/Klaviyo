from utils import (
    get_subscribers,
    get_metrics,
    get_pagination_metrics,
    calculate_rate_metric,
)
import os
import pandas as pd
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


statistic_url = (
    f"{klaviyo_url}/metrics/?fields[metric]=name,updated,created,integration"
)
statistics = get_pagination_metrics(statistic_url, klaviyo_api_key)

report_url = f"{klaviyo_url}/metric-aggregates"


def convert_to_local_timezone(iso: str, local_timezone: ZoneInfo):
    """Convert any timezone to local timezone"""
    if iso is None:
        return None
    else:
        dt = datetime.fromisoformat(iso).astimezone(local_timezone)
        return dt


def get_consent_timestamp(subscriber: dict, local_timezone: ZoneInfo):
    """Get Consent timestamp"""
    return convert_to_local_timezone(
        subscriber["attributes"]["subscriptions"]["email"]["marketing"][
            "consent_timestamp"
        ],
        local_timezone,
    )


def get_data() -> list:
    try:
        # Initialize variables
        delivered_email_count = int(0)
        bounced_email_count = int(0)
        spam_email_count = int(0)
        dropped_email_count = int(0)
        opened_email_count = int(0)
        clicked_email_count = int(0)
        unsubscribed_count = 0
        conversion_active_on_site_count = 0
        conversion_viewed_product_count = 0
        revenue_unique_count = int(0)
        total_revenue_count = int(0)
        total_order_count = int(0)
        delivered_email_unique_count = int(0)
        revenue_count = int(0)
        subscriber_count = int(0)
        new_subscriber_count = int(0)
        new_subscriber_data = list()

        local_timezone = tzlocal.get_localzone()
        current_time = datetime.now(local_timezone)
        cutoff_time = datetime(
            current_time.year,
            current_time.month,
            current_time.day,
            0,
            0,
            0,
            tzinfo=local_timezone,
        )

        processed_metric_ids = set()
        for stat in statistics:
            metric_id = stat["id"]

            if metric_id in processed_metric_ids:
                continue
            processed_metric_ids.add(metric_id)

            if stat["attributes"]["name"] == "Received Email":
                delivered_emails = get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )
                if isinstance(delivered_emails, list):
                    for delivered_email in delivered_emails:
                        if isinstance(delivered_email, dict) and "measurements" in delivered_email and "count" in delivered_email["measurements"]:
                            if delivered_email["dimensions"] == ["UjjW7L"]:  # ID of report
                                delivered_email_count += sum(delivered_email["measurements"]["count"])
                        else:
                            logging.error(
                                f"Unexpected delivered_email structure: {delivered_email}"
                            )
                            return f"Unexpected delivered_email structure: {delivered_email}"
                else:
                    logging.error(f"delivered_emails is not a list: {delivered_emails}")
                    return f"delivered_emails is not a list: {delivered_emails}"

                delivered_email_uniques = get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )
                if isinstance(delivered_email_uniques, list):
                    for delivered_email_unique in delivered_email_uniques:
                        if isinstance(delivered_email_unique, dict) and "measurements" in delivered_email_unique and "unique" in delivered_email_unique["measurements"]:
                            delivered_email_unique_count += sum(
                                delivered_email_unique["measurements"]["unique"]
                            )
                        else:
                            logging.error(
                                f"Unexpected delivered_email_unique structure: {delivered_email_unique}"
                            )
                            return f"Unexpected delivered_email_unique structure: {delivered_email_unique}"
                else:
                    logging.error(
                        f"delivered_email_uniques is not a list: {delivered_email_uniques}"
                    )
                    return f"delivered_email_uniques is not a list: {delivered_email_uniques}"

            if stat["attributes"]["name"] == "Dropped Email":
                dropped_emails = get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )

                for dropped_email in dropped_emails:
                    if isinstance(dropped_email, dict) and "measurements" in dropped_email and "count" in dropped_email["measurements"]:
                        dropped_email_count += sum(dropped_email["measurements"]["count"])
                    else:
                        logging.error(
                            f"Unexpected dropped_email structure: {dropped_email}"
                        )
                        return (
                            f"Unexpected dropped_email structure: {dropped_email}"
                        )

            if stat["attributes"]["name"] == "Marked Email as Spam":
                spam_emails = get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key,
                )
                if isinstance(spam_emails, list):
                    for spam_email in spam_emails:
                        if isinstance(spam_email, dict) and "measurements" in spam_email and "count" in spam_email["measurements"]:
                            spam_email_count += sum(spam_email["measurements"]["count"])
                        else:
                            logging.error(
                                f"Unexpected spam_email structure: {spam_email}"
                            )
                            return f"Unexpected spam_email structure: {spam_email}"
                else:
                    logging.error(f"spam_emails is not a list: {spam_emails}")
                    return f"spam_emails is not a list: {spam_emails}"

            if stat["attributes"]["name"] == "Opened Email":
                opened_emails = get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )
                if isinstance(opened_emails, list):
                    for opened_email in opened_emails:
                        if isinstance(opened_email, dict) and "measurements" in opened_email and "unique" in opened_email["measurements"]:
                            if opened_email["dimensions"] == ["UjjW7L"]: # ID of report
                                opened_email_count += sum(opened_email["measurements"]["unique"])
                        else:
                            logging.error(
                                f"Unexpected opened_email structure: {opened_email}"
                            )
                            return f"Unexpected opened_email structure: {opened_email}"
                else:
                    logging.error(f"opened_emails is not a list: {opened_emails}")
                    return f"opened_emails is not a list: {opened_emails}"

            if stat["attributes"]["name"] == "Clicked Email":
                clicked_emails = get_metrics(
                    ["$message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )
                if isinstance(clicked_emails, list):
                    for clicked_email in clicked_emails:
                        if isinstance(clicked_email, dict) and "measurements" in clicked_email and "unique" in clicked_email["measurements"]: 
                            clicked_email_count += sum(clicked_email["measurements"]["unique"])
                        else:
                            logging.error(
                                f"Unexpected clicked_email structure: {clicked_email}"
                            )
                            return (
                                f"Unexpected clicked_email structure: {clicked_email}"
                            )
                else:
                    logging.error(f"clicked_emails is not a list: {clicked_emails}")
                    return f"clicked_emails is not a list: {clicked_emails}"

            if stat["attributes"]["name"] == "Subscribed to List":
                segment_url = f"{klaviyo_url}/segments/?fields[segment]=name"
                segments = get_pagination_metrics(segment_url, klaviyo_api_key)
                for seg in segments:
                    if seg["attributes"]["name"] == "All Subscribers Segment":
                        subscriber_id = seg["id"]
                        subscriber_url = f"{klaviyo_url}/segments/{subscriber_id}/?additional-fields[segment]=profile_count&fields[segment]=name,created,updated&fields[tag]=name&include=tags"
                        subscribers = get_subscribers(subscriber_url, klaviyo_api_key)
                        subscriber_count += subscribers["data"]["attributes"]["profile_count"]

                        new_subscriber_url = f"{klaviyo_url}/segments/{subscriber_id}/profiles/?additional-fields[profile]=subscriptions,predictive_analytics&fields[profile]=created,updated,location,email&page[size]=100"
                        new_subscribers = get_pagination_metrics(
                            new_subscriber_url, klaviyo_api_key
                        )
                        if isinstance(new_subscribers, list):
                            for new_subscriber in new_subscribers:
                                if (
                                    isinstance(new_subscriber, dict) and
                                    "attributes" in new_subscriber and
                                    "subscriptions" in new_subscriber["attributes"] and
                                    "email" in new_subscriber["attributes"]["subscriptions"] and
                                    "marketing" in new_subscriber["attributes"]["subscriptions"]["email"] 
                                    ):
                                    if (new_subscriber["attributes"]["subscriptions"]["email"]["marketing"]["consent"] == "SUBSCRIBED" and
                                    convert_to_local_timezone(
                                        new_subscriber["attributes"]["subscriptions"]["email"]["marketing"]["consent_timestamp"],
                                        local_timezone,
                                    ) is not None and
                                    convert_to_local_timezone(
                                        new_subscriber["attributes"]["subscriptions"]["email"]["marketing"]["consent_timestamp"],
                                        local_timezone,
                                    ) >= cutoff_time):
                                        new_subscriber_data.append(new_subscriber)
                                else:
                                    return f"new_subscriber: {new_subscriber}"
                        else:
                            logging.info(f"new_subscriber_data: {new_subscriber_data}")
                        # sorted_new_subscriber_data = sorted(
                        #     new_subscriber_data, key=lambda subscriber: get_consent_timestamp(subscriber, local_timezone)
                        # )
                        new_subscriber_count += len(new_subscriber_data)

            if stat["attributes"]["name"] == "Unsubscribed from List":
                unsubscribed_url = f"{klaviyo_url}/profiles/?additional-fields[profile]=subscriptions&fields[profile]=title&page[size]=100"
                unsubscribed_data = get_pagination_metrics(
                    unsubscribed_url, klaviyo_api_key
                )
                if isinstance(unsubscribed_data, list):
                    for unsubscribed in unsubscribed_data:
                        if (
                            isinstance(unsubscribed, dict)
                            and "attributes" in unsubscribed
                            and "subscriptions" in unsubscribed["attributes"]
                            and "email" in unsubscribed["attributes"]["subscriptions"]
                            and "marketing"
                            in unsubscribed["attributes"]["subscriptions"]["email"]
                        ):
                            if (
                                unsubscribed["attributes"]["subscriptions"]["email"]["marketing"][
                                    "consent"
                                ]
                                == "UNSUBSCRIBED"
                            ):
                                unsubscribed_count += 1

            if stat["attributes"]["name"] == "Bounced Email":
                bounced_emails = get_metrics(
                    ["Bounce Type"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )
                if isinstance(bounced_emails, list):
                    for bounced_email in bounced_emails:
                        if (
                            isinstance(bounced_email, dict)
                            and "measurements" in bounced_email
                            and "unique" in bounced_email["measurements"]
                        ):
                            bounced_email_count += sum(bounced_email["measurements"]["unique"])
                        else:
                            logging.error(
                                f"Unexpected bounced_email structure: {bounced_email}"
                            )
                            return f"Unexpected bounced_email structure: {bounced_email}"
                else:
                    logging.error(f"bounced_emails is not a list: {bounced_emails}")
                    return f"bounced_emails is not a list: {bounced_emails}"

            if stat["attributes"]["name"] == "Viewed Product":
                conversion_viewed_products = get_metrics(
                    ["$attributed_message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key,
                )
                if isinstance(conversion_viewed_products, list):
                    for conversion_viewed_product in conversion_viewed_products:
                        if isinstance(conversion_viewed_product, dict) and "measurements" in conversion_viewed_product and "unique" in conversion_viewed_product["measurements"]: 
                            if conversion_viewed_product["dimensions"] != [""]:
                                conversion_viewed_product_count += sum(
                                    conversion_viewed_product["measurements"]["unique"]
                                )

                        else:
                            logging.error(
                                f"Unexpected conversion_viewed_product structure: {conversion_viewed_product}"
                            )
                            return f"Unexpected conversion_viewed_product structure: {conversion_viewed_product}"
                else:
                    logging.error(
                        f"conversion_viewed_products is not a list: {conversion_viewed_products}"
                    )
                    return f"conversion_viewed_products is not a list: {conversion_viewed_products}"
            if stat["attributes"]["name"] == "Active on Site":
                conversion_active_on_sites = get_metrics(
                    ["$attributed_message"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )
                if isinstance(conversion_active_on_sites, list):
                    for conversion_active_on_site in conversion_active_on_sites:
                        if isinstance(conversion_active_on_site, dict) and "measurements" in conversion_active_on_site and "unique" in conversion_active_on_site["measurements"]: 
                            if conversion_active_on_site["dimensions"] != [""]:
                                conversion_active_on_site_count += sum(
                                    conversion_active_on_site["measurements"]["unique"]
                                )
                        else:
                            logging.error(
                                f"Unexpected conversion_active_on_site structure: {conversion_active_on_site}"
                            )
                            return f"Unexpected conversion_active_on_site structure: {conversion_active_on_site}"
                else:
                    logging.error(
                        f"conversion_active_on_sites is not a list: {conversion_active_on_sites}"
                    )
                    return f"conversion_active_on_sites is not a list: {conversion_active_on_sites}"

            if stat["attributes"]["name"] == "Placed Order":
                revenues = get_metrics(
                    ["$attributed_message", "$attributed_flow"],
                    metric_id,
                    report_url,
                    ["sum_value"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                        'not(equals($attributed_message,""))',
                    ],
                    klaviyo_api_key
                )
                if isinstance(revenues, list):
                    for revenue in revenues:
                        if (
                            isinstance(revenue, dict)
                            and "measurements" in revenue
                            and "sum_value" in revenue["measurements"]
                        ):

                            revenue_count += sum(revenue["measurements"]["sum_value"])
                        else:
                            logging.error(f"Unexpected revenue structure: {revenue}")
                            return f"Unexpected revenue structure: {revenue}"
                else:
                    logging.error(f"revenues is not a list: {revenues}")
                    return logging.error(f"revenues is not a list: {revenues}")

                revenue_uniques = get_metrics(
                    ["$attributed_message", "$attributed_flow"],
                    metric_id,
                    report_url,
                    ["unique"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                        'not(equals($attributed_message,""))',
                    ],
                    klaviyo_api_key
                )
                for revenue_unique in revenue_uniques:
                    revenue_unique_count += sum(revenue_unique["measurements"]["unique"])

                total_orders = get_metrics(
                    ["$flow"],
                    metric_id,
                    report_url,
                    ["count"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key,
                )
                if isinstance(total_orders, list):
                    for total_order in total_orders:
                        if isinstance(total_order, dict) and "measurements" in total_order and "count" in total_order["measurements"]:
                            total_order_count += sum(total_order["measurements"]["count"])
                        else:
                            logging.error(
                                f"Unexpected total_order structure: {total_order}"
                            )
                            return f"Unexpected total_order structure: {total_order}"
                            # print("Unexpected total_order structure:", total_order)
                else:
                    logging.error(f"total_orders is not a list: {total_orders}")
                    return logging.error(f"total_orders is not a list: {total_orders}")

                total_revenues = get_metrics(
                    ["$flow"],
                    metric_id,
                    report_url,
                    ["sum_value"],
                    [
                        "greater-or-equal(datetime,2023-12-01)",
                        "less-than(datetime,2024-04-30)",
                    ],
                    klaviyo_api_key
                )
                if isinstance(total_revenues, list):
                    for total_revenue in total_revenues:
                        if (
                            isinstance(total_revenue, dict)
                            and "measurements" in total_revenue
                            and "sum_value" in total_revenue["measurements"]
                        ):
                            total_revenue_count += sum(total_revenue["measurements"]["sum_value"])
                        else:
                            logging.error(
                                f"Unexpected total_revenue structure: {total_revenue}"
                            )
                            return (
                                f"Unexpected total_revenue structure: {total_revenue}"
                            )
                else:
                    logging.error(f"total_revenues is not a list: {total_revenues}")
                    return logging.error(
                        f"total_revenues is not a list: {total_revenues}"
                    )

        return [
            delivered_email_count if delivered_email_count else 0,
            bounced_email_count if bounced_email_count else 0,
            spam_email_count if spam_email_count else 0,
            dropped_email_count if dropped_email_count else 0,
            opened_email_count if opened_email_count else 0,
            clicked_email_count if clicked_email_count else 0,
            unsubscribed_count if unsubscribed_count else 0,
            conversion_active_on_site_count if conversion_active_on_site_count else 0,
            conversion_viewed_product_count if conversion_viewed_product_count else 0,
            revenue_unique_count if revenue_unique_count else 0,
            total_revenue_count if total_revenue_count else 0,
            total_order_count if total_order_count else 0,
            delivered_email_unique_count if delivered_email_unique_count else 0,
            revenue_count if revenue_count else 0,
            subscriber_count if subscriber_count else 0,
            new_subscriber_count if new_subscriber_count else 0,
        ]
    except Exception as e:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = []
        for trace in trace_back:
            stack_trace.append(
                f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
            )
        logging.error(
            f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
        )
        return stack_trace


def calculate_rate_metric(numerator, denominator):
    return (numerator / denominator) * 100 if denominator else 0


def data_handler():
    try:
        metrics = get_data()
        delivered_email_count, bounced_email_count, spam_email_count, dropped_email_count, opened_email_count, clicked_email_count, unsubscribed_count, conversion_active_on_site_count, conversion_viewed_product_count, revenue_unique_count, total_revenue_count, total_order_count, delivered_email_unique_count, revenue_count, subscriber_count, new_subscriber_count = metrics

        total_recipients = (
            delivered_email_count
            + bounced_email_count
            + spam_email_count
            + dropped_email_count
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
        revenue_per_email = (
            revenue_count / delivered_email_count if delivered_email_count != 0 else 0
        )
        product_purchase_rate = calculate_rate_metric(
            revenue_unique_count, delivered_email_unique_count
        )
        average_order_value = (
            total_revenue_count / total_order_count if total_order_count != 0 else 0
        )

        return [
                str(open_rate), 
                str(click_rate), 
                str(unsubscribed_rate), 
                str(bounce_rate), 
                str(delivery_rate), 
                str(conversion_active_on_site_rate), 
                str(conversion_viewed_product_rate), 
                str(revenue_per_email), 
                str(product_purchase_rate), 
                str(average_order_value), 
                str(subscriber_count), 
                str(new_subscriber_count)
            ]

    except Exception as e:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        stack_trace = []
        for trace in trace_back:
            stack_trace.append(
                f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
            )
        logging.error(
            f"File : {trace[0]} , Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}, Exception type: {ex_type}, Exception message: {ex_value}"
        )
        return stack_trace
