import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import tzlocal
import os
from dotenv import load_dotenv

load_dotenv()

def get_subscribers(url: str):
    """Get subscribers for counting"""
    headers = {
        "accept": "application/json",
        "revision": "2024-02-15",
        "content-type": "application/json",
        "Authorization": "Klaviyo-API-Key " + klaviyo_api_key,
    }
    data = requests.get(url, headers=headers)
    return data.json()


def get_metrics(
    by: list, metric_id: str, url: str, measurement: list, filter: list):
    """Get Post Aggregate Metrics"""
    try:
        headers = {
            "accept": "application/json",
            "revision": "2024-02-15",
            "content-type": "application/json",
            "Authorization": "Klaviyo-API-Key " + klaviyo_api_key,
        }
        payload = {
            "data": {
                "type": "metric-aggregate",
                "attributes": {
                    "measurements": measurement,
                    "filter": filter,
                    "by": by,
                    "interval": "month",
                    "metric_id": metric_id,
                },
            }
        }

        data = requests.post(url, headers=headers, json=payload)
        reports = data.json()
        report_results = reports["data"]["attributes"]["data"]
        return report_results
    except Exception as e:
        return str(e)


def get_pagination_metrics(url: str):
    """Get Metrics With Pagination Data"""
    try:
        metrics = []
        headers = {
            "accept": "application/json",
            "revision": "2024-02-15",
            "content-type": "application/json",
            "Authorization": "Klaviyo-API-Key " + klaviyo_api_key,
        }
        while url:
            data = requests.get(url, headers=headers)
            data_pagination = data.json()
            metrics.extend(data_pagination["data"])
            url = data_pagination.get("links", {}).get("next")
        return metrics
    except Exception as e:
        return str(e)


def calculate_rate_metric(metric_a: float, metric_b: float):
    """Rate Calculation"""
    if metric_b == 0:
        result = "Nan"
    else:
        result = round((metric_a / metric_b) * 100, 1)
    return result


def convert_to_local_timezone(iso: str, local_timezone: ZoneInfo):
    """Convert any timezone to local timezone"""
    if iso is None:
        return None
    else:
        dt = datetime.fromisoformat(iso).astimezone(local_timezone)
        return dt


def get_consent_timestamp(subscriber: dict):
    """Get Consent timestamp"""
    return convert_to_local_timezone(
        subscriber["attributes"]["subscriptions"]["email"]["marketing"][
            "consent_timestamp"
        ],
        local_timezone,
    )

klaviyo_api_key = os.environ.get("KLAVIYO_API_KEY", "")
klaviyo_url = "https://a.klaviyo.com/api"

opened_email_count = 0
delivered_email_count = 0
delivered_email_unique_count = 0
subscriber_count = 0
clicked_email_count = 0
unsubscribed_count = 0
bounced_email_count = 0
dropped_email_count = 0
spam_email_count = 0
conversion_viewed_product_count = 0
conversion_active_on_site_count = 0
revenue_count = 0
revenue_unique_count = 0
total_revenue_count = 0
total_order_count = 0
new_subscriber_count = 0

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
