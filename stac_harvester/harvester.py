""" Module for publishing STAC messages """

__author__ = "Rhys Evans"
__date__ = "2023-01-24"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD"


import logging
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pystac
import pystac_client
import requests
import yaml
from pystac_client.stac_api_io import StacApiIO

log = logging.getLogger(__name__)


class Harvester:
    """
    Class to poll Elasticsearch for unaggregated assets or items and add
    the nessasary messages to the correct RabbitMQ queue
    """

    def __init__(self) -> None:
        config_file = os.environ.get("STAC_HARVESTER_CONFIGURATION_FILE")
        if not config_file:
            config_file = os.path.join(
                Path(__file__).parent,
                ".stac_harvester.yaml",
            )

        with open(config_file, encoding="utf-8") as reader:
            self.conf = yaml.safe_load(reader)

        self.input_conf = self.conf.get("INPUT")
        self.output_conf = self.conf.get("OUTPUT")
        self.log_conf = self.conf.get("LOGGING")

        logging.basicConfig(
            format="%(asctime)s @%(name)s [%(levelname)s]:    %(message)s",
            level=logging.getLevelName(self.log_conf.get("LEVEL")),
        )

        input_type = self.input_conf.get("TYPE")
        self.input_root = self.input_conf.get("ROOT")
        self.get = self.get_api if input_type == "API" else self.get_static

        output_type = self.output_conf.get("TYPE")
        self.post = self.post_api if output_type == "API" else self.post_static

        if output_type == "STATIC":
            self.output_catalog = pystac.Catalog.from_file(self.output_conf.get("ROOT"))
            self.parent = self.output_catalog
        else:
            self.output_catalog = pystac.Client.open(self.output_conf.get("ROOT"))

        self.collection_count = 0
        self.item_count = 0

    def update_links(self, stac_obj) -> dict:
        """
        The set of message that are older or younger than the cutoff time
        to be used to generate message list.

        :param cutoff: the time which older documents surtype can be generated
        :param operator: the comparison operator to use in the ES search
        :return: set of relevant messages
        """

        root_link = self.output_catalog.get_root_link().href.replace(
            "/catalog.json", ""
        )

        new_links = []
        for link in stac_obj.links:
            if link.href.startswith(self.input_root):
                if link.rel == "self":
                    if stac_obj.STAC_OBJECT_TYPE == "Collection":
                        link.target = (
                            link.href.replace(
                                self.input_root,
                                root_link,
                            )
                            + "/collection.json"
                        )
                    else:
                        link.target = (
                            link.href.replace(
                                self.input_root,
                                root_link,
                            )
                            + ".json"
                        )

                if link.rel in ["collection", "parent"]:
                    link.target = self.parent

                elif link.rel == "root":
                    link.target = self.output_catalog

                else:
                    link.target = link.href.replace(
                        self.input_root,
                        root_link,
                    )

                new_links.append(link)

        stac_obj.links = new_links
        return stac_obj

    def get_static(self) -> None:
        """
        GET RECORDS FROM STATIC CATALOG
        """
        pass

    def post_static(self, stac_obj: dict) -> None:
        """
        POST RECORDS TO STATIC CATALOG
        """
        if stac_obj.STAC_OBJECT_TYPE == "Collection":
            stac_obj = pystac.Collection.from_dict(stac_obj.to_dict())
            stac_obj = self.update_links(stac_obj)
            self.output_catalog.add_child(stac_obj, set_parent=False)
            self.parent = stac_obj
            self.collection_count += 1

        else:
            stac_obj = pystac.Item.from_dict(stac_obj.to_dict())
            stac_obj = self.update_links(stac_obj)
            self.item_count += 1

        pystac.write_file(stac_obj)

    def get_api(self) -> None:
        """
        GET RECORDS FROM STATIC CATALOG
        """
        client = pystac_client.Client.open(self.input_root)

        # collection = client.get_collection("ukcp")
        # yield collection
        for collection in client.get_collections():
            self.parent = self.output_catalog
            yield collection

            for item in collection.get_items():
                yield item

    def post_api_colletion(self, stac_obj: dict) -> None:
        response = requests.post(
            urljoin(self.output_catalog, "collections"), json=stac_obj, verify=True
        )

        if response.status_code == 409:
            response_json = response.json()

            if (
                response_json["description"]
                == f"Collection {stac_obj['id']} already exists"
            ):
                response = requests.put(
                    urljoin(self.output_catalog, "collections"),
                    json=stac_obj,
                    verify=True,
                )

                if response.status_code != 200:
                    print(
                        f"FastAPI Output Collection already exists and Update failed with status code: {response.status_code} and response text: {response.text}"
                    )

        elif response.status_code != 200:
            print(
                f"FastAPI Output failed with status code: {response.status_code} and response text: {response.text}"
            )

    def post_api_item(self, stac_obj: dict) -> None:
        response = requests.post(
            urljoin(self.output_root, f"collections/{stac_obj['collection']}/items"),
            json=stac_obj,
            verify=True,
        )

        if response.status_code == 409:
            response_json = response.json()

            if (
                response_json["description"]
                == f"Item {stac_obj['id']} in collection {stac_obj['collection']} already exists"
            ):
                response = requests.put(
                    urljoin(
                        self.output_root,
                        f"collections/{stac_obj['collection']}/items/{stac_obj['id']}",
                    ),
                    json=stac_obj,
                    verify=True,
                )

                if response.status_code != 200:
                    print(
                        f"FastAPI Output {stac_obj['type']} already exists and Update failed with status code: {response.status_code} and response text: {response.text}"
                    )

        elif response.status_code != 200:
            print(
                f"FastAPI Output failed with status code: {response.status_code} and response text: {response.text}"
            )

    def post_api(self, stac_obj: dict) -> None:
        """
        POST RECORDS TO STATIC CATALOG
        """
        if stac_obj["type"] == "collection":
            self.post_api_colletion(stac_obj)

        else:
            self.post_api_item(stac_obj)

    def run(self) -> None:
        """
        Generate and publish the surtype generation messages for the specified STAC type.

        :return: None
        """

        start = datetime.now()

        for stac_record in self.get():
            self.post(stac_record)

        self.output_catalog.save()

        end = datetime.now()
        print(
            f"Harvested {self.collection_count} Collections and {self.item_count} Items in {end-start}"
        )


if __name__ == "__main__":
    Harvester().run()
