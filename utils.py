import requests

def get_subscribers(url: str, klaviyo_api_key: str):
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
    by: list, metric_id: str, url: str, measurement: list, filter: list, klaviyo_api_key
: str):
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


def get_pagination_metrics(url: str, klaviyo_api_key: str):
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
    try:
        result = (metric_a / metric_b) * 100
        return result
    except ZeroDivisionError:
        return 0.0
    # if metric_b == 0:
    #     result = 0.0
    # else:
    #     result = (metric_a / metric_b) * 100
    # return result