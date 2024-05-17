from utils import (
    get_subscribers,
    get_metrics,
    get_pagination_metrics,
    calculate_rate_metric,
    convert_to_local_timezone,
    get_consent_timestamp,
    processed_metric_ids,
    opened_email_count,
    delivered_email_count,
    delivered_email_unique_count,
    subscriber_count,
    clicked_email_count,
    unsubscribed_count,
    bounced_email_count,
    dropped_email_count,
    spam_email_count,
    conversion_viewed_product_count,
    conversion_active_on_site_count,
    revenue_count,
    revenue_unique_count,
    total_revenue_count,
    total_order_count,
    new_subscriber_count,
    local_timezone,
    cutoff_time,
    klaviyo_url,
)

import pandas as pd


statistic_url = (
    f"{klaviyo_url}/metrics/?fields[metric]=name,updated,created,integration"
)
statistics = get_pagination_metrics(statistic_url)

report_url = f"{klaviyo_url}/metric-aggregates"


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
        )

        for delivered_email in delivered_emails:
            if delivered_email["dimensions"] == ["UjjW7L"]:  # ID of report
                delivered_email_count += sum(delivered_email["measurements"]["count"])

        delivered_email_uniques = get_metrics(
            ["$message"],
            metric_id,
            report_url,
            ["unique"],
            [
                "greater-or-equal(datetime,2023-12-01)",
                "less-than(datetime,2024-04-30)",
            ],
            
        )
        for delivered_email_unique in delivered_email_uniques:
            delivered_email_unique_count += sum(
                delivered_email_unique["measurements"]["unique"]
            )

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
            
        )
        for dropped_email in dropped_emails:
            dropped_email_count += sum(dropped_email["measurements"]["count"])

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
            
        )
        for spam_email in spam_emails:
            spam_email_count += sum(spam_email["measurements"]["count"])

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
            
        )
        for opened_email in opened_emails:
            if opened_email["dimensions"] == ["UjjW7L"]: # ID of report
                opened_email_count += sum(opened_email["measurements"]["unique"])

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
            
        )
        for clicked_email in clicked_emails:
            clicked_email_count += sum(clicked_email["measurements"]["unique"])

    if stat["attributes"]["name"] == "Subscribed to List":
        segment_url = f"{klaviyo_url}/segments/?fields[segment]=name"
        segments = get_pagination_metrics(segment_url)
        for seg in segments:
            if seg["attributes"]["name"] == "All Subscribers Segment":
                subscriber_id = seg["id"]
                subscriber_url = f"{klaviyo_url}/segments/{subscriber_id}/?additional-fields[segment]=profile_count&fields[segment]=name,created,updated&fields[tag]=name&include=tags"
                subscribers = get_subscribers(subscriber_url, )
                subscriber_count += subscribers["data"]["attributes"]["profile_count"]

                new_subscriber_url = f"{klaviyo_url}/segments/{subscriber_id}/profiles/?additional-fields[profile]=subscriptions,predictive_analytics&fields[profile]=created,updated,location,email&page[size]=100"
                new_subscribers = get_pagination_metrics(
                    new_subscriber_url, 
                )
                new_subscriber_data = [
                    new_subscriber
                    for new_subscriber in new_subscribers
                    if (
                        new_subscriber["attributes"]["subscriptions"]["email"][
                            "marketing"
                        ]["consent"]
                        == "SUBSCRIBED"
                        and convert_to_local_timezone(
                            new_subscriber["attributes"]["subscriptions"]["email"][
                                "marketing"
                            ]["consent_timestamp"],
                            local_timezone,
                        )
                        is not None
                        and convert_to_local_timezone(
                            new_subscriber["attributes"]["subscriptions"]["email"][
                                "marketing"
                            ]["consent_timestamp"],
                            local_timezone,
                        )
                        >= cutoff_time
                    )
                ]
                sorted_new_subscriber_data = sorted(
                    new_subscriber_data, key=get_consent_timestamp
                )
                new_subscriber_count += len(new_subscriber_data)

    if stat["attributes"]["name"] == "Unsubscribed from List":
        unsubscribed_url = f"{klaviyo_url}/profiles/?additional-fields[profile]=subscriptions&fields[profile]=title&page[size]=100"
        unsubscribed_data = get_pagination_metrics(unsubscribed_url, )

        for unsubscribed in unsubscribed_data:
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
            
        )
        for bounced_email in bounced_emails:
            bounced_email_count += sum(bounced_email["measurements"]["unique"])

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
            
        )
        for conversion_viewed_product in conversion_viewed_products:
            if conversion_viewed_product["dimensions"] != [""]:
                conversion_viewed_product_count += sum(
                    conversion_viewed_product["measurements"]["unique"]
                )

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
            
        )
        for conversion_active_on_site in conversion_active_on_sites:
            if conversion_active_on_site["dimensions"] != [""]:
                conversion_active_on_site_count += sum(
                    conversion_active_on_site["measurements"]["unique"]
                )

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
            
        )
        for revenue in revenues:
            revenue_count += sum(revenue["measurements"]["sum_value"])

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
            
        )
        for total_order in total_orders:
            total_order_count += sum(total_order["measurements"]["count"])

        total_revenues = get_metrics(
            ["$flow"],
            metric_id,
            report_url,
            ["sum_value"],
            [
                "greater-or-equal(datetime,2023-12-01)",
                "less-than(datetime,2024-04-30)",
            ],
            
        )
        for total_revenue in total_revenues:
            total_revenue_count += sum(total_revenue["measurements"]["sum_value"])

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
revenue_per_email = revenue_count / delivered_email_count
product_purchase_rate = calculate_rate_metric(
    revenue_unique_count, delivered_email_unique_count
)
average_order_value = total_revenue_count / total_order_count


def get_df_data():
    date = (
        str(cutoff_time.month)
        + "-"
        + str(cutoff_time.day)
        + "-"
        + str(cutoff_time.year)
    )
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
        "Unsubscribed Rate": [
            str(unsubscribed_rate) + "%",
            str(unsubscribed_rate) + "%",
        ],
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
    df = pd.DataFrame(results)
    return df