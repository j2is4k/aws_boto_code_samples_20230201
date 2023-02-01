from common.model import base
from common import response
from common import constants
from common import dynamodb
from common.helpers import borg_factory
from common.helpers import dict_nested_update


class SharedTable(borg_factory(), dynamodb.DynamoDB):
    pass


class BrandModel(base.SharedTableAccess):
    def __init__(self, force_reload=False):
        super().__init__(SharedTable, force_reload=force_reload)

    @response.generate
    def get_brand(self, brand):
        brand = self.get_item(brandKey=brand)
        if not brand or brand.get("status") == constants.BRAND_STATUS_RETIRED:
            return response.StatusCode().set_not_found("Brand was not found.")
        brand.pop("status", None)
        return brand

    @response.generate
    def put(self, brand):
        brand["status"] = constants.BRAND_STATUS_ACTIVE
        put_item_succeeded = self.put_item(**brand)
        if not put_item_succeeded:
            return response.StatusCode().set_bad_request("Put brand operation failed.")
        return response.StatusCode().set_created()

    @response.generate
    def update(self, brand: dict):
        stored_brand = self.get_item(brandKey=brand["brandKey"])

        if not stored_brand or stored_brand.get("status") == constants.BRAND_STATUS_RETIRED:
            return response.StatusCode().set_bad_request("Brand not found or retired.")

        updated_brand = dict_nested_update(stored_brand, brand)
        put_item_succeeded = self.put_item(**updated_brand)

        if not put_item_succeeded:
            return response.StatusCode().set_bad_request("Brand update operation failed.")
        return response.StatusCode().set_ok()

    def _delete(self, brand_key):
        """WARNING - this method DELETES a brand - only for INTERNAL USE."""
        delete_item_succeeded = self.delete_item(brandKey=brand_key)

        if delete_item_succeeded:
            return response.StatusCode().set_ok()
        return response.StatusCode().set_bad_request(f"Deleting a brand item with {brand_key} failed")

    @response.generate
    def retire(self, brand_key):
        return self.update(dict(brandKey=brand_key, status=constants.BRAND_STATUS_RETIRED))
