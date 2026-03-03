"""DLT pipeline to ingest NYC taxi data from a paginated REST API.

API base URL:
https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api

This source requests pages of JSON (up to 1000 records per page) and stops
when an empty page is returned.
"""

import typing
import requests
import dlt


@dlt.source
def taxi_rest_api_source(base_url: str = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"):
    """DLT source that fetches paginated JSON from the provided base URL.

    It requests pages with `page` (starting at 1) and `per_page=1000` and
    stops when a page returns no records.
    """

    session = requests.Session()

    @dlt.resource(write_disposition="append")
    def trips() -> typing.Iterator[typing.Dict]:
        page = 1
        per_page = 1000
        while True:
            resp = session.get(base_url, params={"page": page, "per_page": per_page})
            resp.raise_for_status()
            payload = resp.json()

            # support APIs that return a list directly or wrap records in common keys
            if isinstance(payload, list):
                records = payload
            elif isinstance(payload, dict):
                records = (
                    payload.get("data")
                    or payload.get("results")
                    or payload.get("items")
                    or payload.get("records")
                    or payload.get("rows")
                    or []
                )
            else:
                records = []

            if not records:
                break

            for r in records:
                yield r

            page += 1

    yield trips()


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(taxi_rest_api_source())
    print(load_info)  # noqa: T201
