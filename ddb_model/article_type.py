from common.model import base
from common import constants
from common import response
from common import dynamodb
from common.helpers import borg_factory


class SharedTable(borg_factory(), dynamodb.DynamoDB):
    pass


class IndexMapping(base.IndexMapping):
    index_keys_map = {
        "s_id": base.ShardIdTemplate(base.SHARDS_RANGE_ARTICLE_TYPE),
    }


class ArticleTypeModel(base.SharedTableAccess, base.QueryMixin):
    def __init__(self, force_reload=False):
        super().__init__(SharedTable, force_reload=force_reload)
        self.index_mapping = IndexMapping()
        self.shard_ids = list(base.SHARDS_RANGE_ARTICLE_TYPE)

    @response.generate
    def get(self, article_type):
        """Get article type item.

        Args:
            article_type(str): Name of the article type in form of category|subcategory|ArticleType

        Returns:
            ArticleType definition with its rules
                {
                    'articleType': 'category|subcategory|articleType',
                    'displayName': 'Article Type',
                    'mandatoryAttributeKeys': [...],
                    'prohibitedAttributeKeys': [...],
                    'recommendedAttributeKeys': [...],
                    'variantDefiningAttributeKeys': [...]
                }
        """
        article_type_rules = self.get_item(articleType=article_type)
        if not article_type_rules:
            return response.StatusCode().set_not_found("ArticleType was not found.")
        article_type_rules = self.index_mapping.drop_index_keys_from_entity(article_type_rules)
        return article_type_rules

    @response.generate
    def put(self, article_type_data):
        """Puts new article type into the db

        Args:
            article_type_data(dict): Dictionary with the following structure:
                {
                    'articleType': 'category|subcategory|articleType',
                    'displayName': 'Article Type',
                    'mandatoryAttributeKeys': [...],
                    'prohibitedAttributeKeys': [...],
                    'recommendedAttributeKeys': [...],
                    'variantDefiningAttributeKeys': [...]
                }

            Returns:
                StatusCode
        """
        article_type_data = self.index_mapping.enrich_entity_with_index_keys(article_type_data)
        put_item_succeeded = self.put_item(**article_type_data)
        if not put_item_succeeded:
            return response.StatusCode().set_bad_request("Put article type operation failed.")
        return response.StatusCode().set_ok()

    @response.generate
    def update(self, article_type_data):
        """Updates item

        Args:
            article_type_data(dict): Dictionary with the following structure:
                {
                    'articleType': 'category|subcategory|articleType',
                    'displayName': 'Article Type',
                    'mandatoryAttributeKeys': [...],
                    'prohibitedAttributeKeys': [...],
                    'recommendedAttributeKeys': [...],
                    'variantDefiningAttributeKeys': [...]
                }

            Returns:
                StatusCode
        """
        key = {"articleType": article_type_data["articleType"]}
        update_item_succeeded = self.update_item_with_payload(article_type_data, **key)
        if not update_item_succeeded:
            return response.StatusCode().set_bad_request("Update article type operation failed.")
        return response.StatusCode().set_ok()

    @response.generate
    def delete(self, article_type):
        """Deletes an item in the database

        Args:
            article_type(str): Name of the article type in form of category|subcategory|ArticleType

        Returns:
            StatusCode
        """
        deletion_successful = self.delete_item(articleType=article_type)
        if not deletion_successful:
            return response.StatusCode().set_bad_request("Article type operation delete failed.")
        return response.StatusCode().set_ok()

    def query_article_types(self, **kwargs):
        return self.query_shards(constants.ARTICLE_TYPE_INDEX_CLASSIFICATION_LIST, **kwargs)
