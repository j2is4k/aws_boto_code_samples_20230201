import math
import zlib
import copy
import time
import random
import base64
import pickle
import threading
from typing import List

import boto3.dynamodb.conditions as conditions

from .. import logging
from .. import constants


BARRIER_DEFAULT_TIMEOUT = 900
SHARDS_RANGE_ARTICLE = range(0, 10)
SHARDS_RANGE_ARTICLE_RELEASE = range(0, 10)
SHARDS_RANGE_ARTICLE_SUPPLY_CHAIN = range(0, 10)
SHARDS_RANGE_ARTICLE_TYPE = range(0, 3)

logger = logging.getLogger(__name__)


class EmptyValue:
    pass


class SharedTableAccess:
    def __init__(self, table_class, force_reload=False):
        self._shared_table = table_class(force_reload)
        self._inherit_public_methods_of_shared_table()

    def _inherit_public_methods_of_shared_table(self):
        for name in dir(self._shared_table):
            obj = getattr(self._shared_table, name)
            if not name.startswith("_") and callable(obj) and name not in dir(self):
                setattr(self, name, obj)

    def select_table(self, name):
        if not getattr(self._shared_table, "table", None):
            self._shared_table.select_table(name)

    def __getattr__(self, name):
        if hasattr(self._shared_table, f"_protected_{name}"):
            return getattr(self._shared_table, f"_protected_{name}")
        raise AttributeError(f"Shared object at {self._shared_table} has no attribute: {name}.")

    def store_shared_object(self, name, obj):
        if obj is None:
            raise RuntimeError(f"Object with name {name} cannot be {obj}.")
        setattr(self._shared_table, f"_protected_{name}", obj)

    def retrieve_shared_object(self, name):
        # __getattr__ above can also be used, but this is more verbose.
        return getattr(self._shared_table, f"_protected_{name}", None)


class ShardIdTemplate:
    def __init__(self, shard_range: range):
        self._shard_range = shard_range

    def random_shard_id(self):
        return str(random.choice(self._shard_range))


class IndexMapping:
    """Different attributes of an entity are copied to root level to make use
    of Global Secondary Indexes, producing an "enriched entity". Depending on
    the GSI, they might serve as either Partition Keys or Sort Keys.

    These attributes are dropped when exposing the entity to the outside
    world, producing a "svelte entity".
    """

    def enrich_entity_with_index_keys(self, entity):
        enriched_entity = copy.deepcopy(entity)
        for index_key, template in self.index_keys_map.items():
            value = self._value_from_template(index_key, template, enriched_entity)
            if not isinstance(value, EmptyValue):
                enriched_entity[index_key] = value
        return enriched_entity

    def drop_index_keys_from_entity(self, entity):
        svelte_entity = copy.deepcopy(entity)
        for index_key in self.index_keys_map:
            svelte_entity.pop(index_key, None)
        return svelte_entity

    def drop_index_keys_from_entities(self, entities):
        return [self.drop_index_keys_from_entity(entity) for entity in entities]

    def _value_from_template(self, index_key, template, entity):
        if isinstance(template, ShardIdTemplate):
            return entity.get(index_key, template.random_shard_id())
        if isinstance(template, str):
            return self._format_template(template, entity)
        if callable(template):
            try:
                return template(entity)
            except BaseException:
                return EmptyValue()
        return None

    @staticmethod
    def _format_template(template: str, data: dict):
        """Variable to fill is expected to be in data's root level. No support
        for nested or dotted expressions.

        Args:
            template: Template with "{}" content.
            data: Data in form of a dictionary.

        Returns:
            Formatted string or EmptyValue.
        """
        try:
            return template.format(**data) or EmptyValue()
        except KeyError:
            return EmptyValue()


class Barrier:
    """Running parallel threads consumes provisioned throughput."""

    def __init__(self, callable_, kwargs_mapping: dict):
        """ """
        barrier_size = len(kwargs_mapping) + 1
        assert barrier_size < 100
        self.barrier = threading.Barrier(barrier_size, timeout=BARRIER_DEFAULT_TIMEOUT)
        self.callable = callable_
        self.kwargs_mapping = kwargs_mapping
        self.results = {}
        self.errors = {}

    def threaded_shell(self, id_, kwargs):
        # start_time = time.perf_counter()
        self.results[id_] = {}
        try:
            self.results[id_] = self.callable(**kwargs)
        except BaseException as exc:
            logger.error(f"Error {exc} detected in step {id_}", context_kwargs=kwargs)
            self.errors[id_] = exc
        # logger.debug(f'Function {id_} finished within {time.perf_counter()-start_time:3.2f}s.')
        try:
            self.barrier.wait()
        except threading.BrokenBarrierError as exc:
            logger.error(f"Barrier error {exc} detected in step {id_}", context_kwargs=kwargs)
            self.errors[id_] = exc

    def run(self):
        start_time = time.perf_counter()
        for id_, kwargs in self.kwargs_mapping.items():
            barrier_thread = threading.Thread(
                name=f"BThread-{id_:02d}",
                target=self.threaded_shell,
                args=(id_, kwargs),
            )
            barrier_thread.start()
        self.barrier.wait()
        logger.debug(f"Barrier penetrated after {time.perf_counter()-start_time:3.2f}s.")
        return self.results, self.errors


class QueryMixin:
    @staticmethod
    def _decompress_exclusive_start_key(key: str) -> List[dict]:
        exclusive_start_keys_coded = zlib.decompress(base64.b64decode(key))
        return pickle.loads(exclusive_start_keys_coded)

    def _compress_last_evaluated_keys(self, results: dict) -> str:
        last_evaluated_keys = {}
        for shard_id in self.shard_ids:
            # queries running only for non-empty shards, but here making sure all shards are collected regardless
            last_evaluated_keys[shard_id] = results.get(shard_id, {}).get("LastEvaluatedKey", {})
        if not any(last_evaluated_keys.values()):
            return ""
        zipped_binary_key = zlib.compress(pickle.dumps(last_evaluated_keys))
        return base64.b64encode(zipped_binary_key).decode()

    @staticmethod
    def _log_errors(errors: dict):
        steps_and_errors = ", ".join([f"[Step: {i}, Error: {err}]" for i, err in errors.items()])
        msg = f"{len(errors)} errors detected. Details: {steps_and_errors}"
        logger.error(msg)

    def _sort_by_and_add_priority(self, items, index_name):
        sort_key_name = constants.INDEX_NAME_SORT_KEY_MAPPING.get(index_name)
        if items and sort_key_name:
            for item in items:
                item["priority"] = item[sort_key_name].split("#")[-1]
            # e.g. channel#100#5_2021.08.10 -> sorting by string: 5_2021.08.10
            items.sort(key=lambda item: item["priority"])
        return items

    def query_shards(self, index_name, sk_condition=None, **additional_request_attributes):
        """Spawns a number of concurrent QueryItems requests, equal to the
        number of shards.

        Args:
            index_name: Global Secondary Index name.
            sk_condition: Optional Sort Key condition.
            additional_request_attributes: Additional attributes for each QueryItems request.

        """
        exclusive_start_keys_coded = additional_request_attributes.pop("ExclusiveStartKey", None)
        if exclusive_start_keys_coded:
            exclusive_start_keys = self._decompress_exclusive_start_key(exclusive_start_keys_coded)
        if "Limit" in additional_request_attributes:
            additional_request_attributes["Limit"] = math.ceil(
                int(additional_request_attributes["Limit"]) / len(self.shard_ids)
            )
        kwargs_mapping = {}
        for shard_id in self.shard_ids:
            pk_condition = conditions.Key("s_id").eq(str(shard_id))
            if exclusive_start_keys_coded:
                exclusive_start_key = exclusive_start_keys[shard_id]
                if not exclusive_start_key:
                    # in particular shard, there are no further items to collect -> skip, but keep others collecting
                    continue
                additional_request_attributes["ExclusiveStartKey"] = exclusive_start_key
            kwargs_mapping[shard_id] = dict(
                IndexName=index_name,
                KeyConditionExpression=sk_condition and pk_condition & sk_condition or pk_condition,
                **additional_request_attributes,
            )
        results, errors = Barrier(self.query_items, kwargs_mapping).run()
        if errors:
            self._log_errors(errors)
        flattened_result = []
        [flattened_result.extend(value.get("Items", [])) for value in results.values()]
        to_return = {"Items": self._sort_by_and_add_priority(flattened_result, index_name)}
        compressed_last_evaluated_keys = self._compress_last_evaluated_keys(results)
        if compressed_last_evaluated_keys:
            to_return["LastEvaluatedKey"] = compressed_last_evaluated_keys
        return to_return
