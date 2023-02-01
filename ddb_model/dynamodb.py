"""This module contains the operation handlers for DynamoDB access using Boto3

"""
import time
import botocore.exceptions
from boto3.dynamodb.types import TypeDeserializer

from typing import Dict, List

from . import helpers
from . import logging
from . import constants

from .aws import default_client


logger = logging.getLogger(__name__)


class DynamoDBDeserializer(TypeDeserializer):
    def deserialize(self, value):
        if not isinstance(value, dict):
            return value
        try:
            return super().deserialize(value)
        except (AttributeError, TypeError):
            return {k: self.deserialize(v) for k, v in value.items()}


class DynamoDB:
    def select_table(self, table_name: str):
        """Selects a table to be used for further operations.

        Args:
            table_name: Name of the table.
        """
        self.table = default_client().get_resource_dynamodb().Table(table_name)

    def get_item(self, consistent_read=True, **primary_key) -> dict:
        """Returns a set of attributes for the item with the given primary key.

        Partition Key + Sort Key = Composite primary key
        Partition Key            = Simple primary key

        Args:
            consistent_read: Determines the read consistency model.
            primary_key: Primary key for the entry that is searched for.

        Returns:
            Dictionary with the item.
        """
        try:
            return self.table.get_item(Key=primary_key, ConsistentRead=consistent_read).get("Item", {})
        except botocore.exceptions.ClientError as err:
            logger.error(f"DynamoDB GetItem for {primary_key} failed: {err}.")
            return {}

    def put_item(self, extra_attributes=None, check_lock=False, **item_attributes) -> bool:
        """Creates a new item, or replaces an old item with a new item.

        Args:
            item_attributes: Attributes of the item.

        Returns:
            True if created, else False.
        """
        extra_attributes = extra_attributes or {}
        try:
            result = self.table.put_item(Item=item_attributes, **extra_attributes)
            return True
        except botocore.exceptions.ClientError as err:
            logger.error(
                f"DynamoDB PutItem with {item_attributes}, {extra_attributes} failed: {err}.",
                context_attributes=item_attributes,
            )
            # TODO should be changed to return acutal error to provide the error for reprocessing
            # e.g., retry if the item was locked
            return False

    def put_items(self, items: List[dict]) -> bool:
        """Creates new items, or replaces old items with new items in a batch operation.

        Args:
            items: A list of items.

        Returns:
            True if created, else False.
        """
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            return True
        except botocore.exceptions.ClientError as err:
            logger.error(f"Batch PutItem with {items} failed: {err}.")
            return False

    def scan(self, **request_attributes) -> Dict[str, list]:
        """Gets all items from DynamoDB table matching criteria.

        Args:
            request_attributes: Partition and sort keys, filters, etc.
        """
        has_more = True
        items = []
        while has_more:
            response = self.table.scan(**request_attributes)
            try:
                items += response.get("Items", [])
            except botocore.exceptions.ClientError as err:
                logger.error(f"DynamoDB Scan with {request_attributes} failed: {err}.")

            if response.get("LastEvaluatedKey"):
                request_attributes["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            else:
                has_more = False
        return {"Items": items}

    def delete_item(self, **primary_key) -> bool:
        """Deletes item from DynamoDB Table.

        Partition Key + Sort Key = Composite primary key
        Partition Key            = Simple primary key

        Args:
            primary_key: Primary and sort keys.

        Returns:
            True if item successfully deleted, else False.
        """
        try:
            self.table.delete_item(Key=primary_key)
            return True
        except botocore.exceptions.ClientError as err:
            logger.error(f"DynamoDB DeleteItem with {primary_key} failed: {err}.")
            return False

    def update_item(self, **request_attributes):
        """Update item in DynamoDB Table.

        Args:
            request_attributes: New properties of an item, should include primary and sort keys.

        Returns:
            True if item successfully updated, else False.
        """
        try:
            response = self.table.update_item(**request_attributes)
            return True
        except botocore.exceptions.ClientError as err:
            logger.error(f"DynamoDB UpdateItem request with {request_attributes} failed: {err}")
            return False

    def update_item_with_payload(self, payload: dict, **primary_key):
        """Update item in DynamoDB Table.

        This is our custom version with get - merge payload with result - put.

        Args:
            payload: Dict containing new properties of an item.
            primary_key: Partition and/or sort keys.
        """
        result = self.get_item(**primary_key)
        if not result:
            logger.error(
                f"Update on non-existing article {primary_key} is not allowed.",
                context_primary_key=primary_key,
            )
            return False

        updated_payload = helpers.dict_nested_update(result, payload)
        put_item_succeeded = self.put_item(**updated_payload)

        if not put_item_succeeded:
            logger.error(
                "update_item_with_payload failed due to put_item failure.",
                context_primary_key=primary_key,
            )
            return False

        return True

    def query_items(
        self,
        **request_attributes,
    ):
        """Finds items in the DynamoDB table based on certain criteria.

        Args:
            request_attributes:
                Dictionary with primary key, filters, ExclusiveStartKey etc. which will be passed into the DynamoDB
                request. Can contain MaxPages, which forces to return after collecting given number of pages.

        Returns:
            List with the requested items.
        """
        start_time = time.perf_counter()
        max_pages = request_attributes.pop("MaxPages", constants.DYNAMO_DB_MAX_QUERY_PAGES)
        start_key = request_attributes.get("ExclusiveStartKey", True)
        result = {"Items": [], "Counts": []}
        iterations = 0
        while start_key:
            iterations += 1
            if start_key is not True:
                request_attributes["ExclusiveStartKey"] = start_key
            try:
                query_response = self.table.query(**request_attributes)
            except botocore.exceptions.ClientError as err:
                logger.error(f"Query with {request_attributes} failed: {err}.")
                return result

            result["Items"].extend(query_response.pop("Items", []))
            result["Counts"].append(query_response["Count"])
            result.pop(
                "LastEvaluatedKey", None
            )  # in last page the key is not present in response, make sure to clean it up also from previous iteration
            result.update(query_response)
            if iterations >= max_pages:
                logger.debug(f"Item Count(s): {result.get('Counts')}, duration {time.perf_counter()-start_time:3.2f}s")
                return result
            start_key = query_response.get("LastEvaluatedKey")
        logger.debug(f"Item Count(s): {result.get('Counts')}, duration {time.perf_counter()-start_time:3.2f}s")
        return result
