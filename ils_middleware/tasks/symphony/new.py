"""New Record in Symphony"""

import json

from ils_middleware.tasks.symphony.request import SymphonyRequest


def NewMARCtoSymphony(**kwargs):
    """Creates a new record in Symphony and returns the new CatKey"""
    library_key = kwargs.get("library_key")
    item_type = kwargs.get("item_type")
    home_location = kwargs.get("home_location")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(
        key="new_resources", task_ids="process_symphony.new-or-overlay"
    )

    for resource_uri in resources:
        resource_uuid = resource_uri.split("/")[-1]
        marc_json = task_instance.xcom_pull(
            key=resource_uuid, task_ids="process_symphony.convert_to_symphony_json"
        )

        payload = {
            "@resource": "/catalog/bib",
            "catalogFormat": {"@resource": "/policy/catalogFormat", "@key": "MARC"},
            "shadowed": False,
            "bib": marc_json,
            "callList": [
                {
                    "@resource": "/catalog/call",
                    "callNumber": "AUTO",
                    "classification": {
                        "@resource": "/policy/classification",
                        "@key": "LC",
                    },
                    "library": {
                        "@resource": "/policy/library",
                        "@key": f"{library_key}",
                    },
                    "itemList": [
                        {
                            "@resource": "/catalog/item",
                            "barcode": "AUTO",
                            "itemType": {
                                "@resource": "/policy/itemType",
                                "@key": f"{item_type}",
                            },
                            "homeLocation": {
                                "@resource": "/policy/location",
                                "@key": f"{home_location}",
                            },
                        }
                    ],
                }
            ],
        }
        resource_uuid = resource_uri.split("/")[-1]
        task_instance.xcom_push(
            key=resource_uuid,
            value=SymphonyRequest(
                **kwargs,
                data=json.dumps(payload),
                endpoint="catalog/bib",
                filter=lambda response: response.json().get("@key"),
            ),
        )
