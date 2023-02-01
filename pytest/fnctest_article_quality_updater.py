import boto3
import moto
import pytest
import json
from unittest import mock

import common
import articlelake as articlelake
from common.dynamodb import DynamoDBDeserializer


@mock.patch("common.constants.SQS_QUEUE_ARTICLE_QUALITY_UPDATER", "queue-article-quality-update")
class TestArticleQualityUpdater:
    @pytest.fixture(autouse=True)
    def setup(self, load_lambda_module):
        self.module_article_get = load_lambda_module(parent_dir="./../../endpoint/article/_get")
        self.module_article_post = load_lambda_module(parent_dir="./../../endpoint/article/_post")
        self.module_article_patch = load_lambda_module(parent_dir="./../../endpoint/article/_patch")

        classification = articlelake.Classification(force_reload=True)

        self.classification_test_items = ["testCategory|testSubcategory|test"]

        for article_type in self.classification_test_items:
            item = dict(
                articleType=article_type,
                displayName="testCategory",
                mandatoryAttributeKeys=[
                    "attributeGenericArticleDescription",
                    "attributeGenericArticleTitle",
                    "attributeGenericBrand",
                ],
                prohibitedAttributeKeys=[],
                recommendedAttributeKeys=[],
                variantDefiningAttributeKeys=[],
            )
            classification.put(item)

    def get_article(self, gtin, channel):
        event = {"headers": {"gtin": gtin, "channel": channel}}
        get_response = self.module_article_get.lambda_handler(event, None)
        return get_response

    def post_or_patch_article(self, data, context=None, method="POST", headers=None):
        event = {
            "body": json.dumps(data),
        }
        if headers:
            event.update(headers=headers["headers"])

        if method == "POST":
            return self.module_article_post.lambda_handler(event, context)
        return self.module_article_patch.lambda_handler(event, context)

    def test_article_quality_updater_sends_sqs_messages(
        self, load_lambda_module, data_article_quality_updater_dynamodb_record, data_articles_quality_updater_payload
    ):
        self.article_quality_updater = load_lambda_module()
        self.article_quality_updater.sqs = mock.MagicMock()
        self.post_or_patch_article(data_articles_quality_updater_payload)

        self.article_quality_updater.lambda_handler(data_article_quality_updater_dynamodb_record, None)
        assert self.article_quality_updater.sqs.send_messages.call_count == 1

    @pytest.mark.skip("moto.mock_sqs doesn't support direct Queue")
    def test_article_quality_updater_moto_mock_sqs(
        self,
        load_lambda_module,
        data_article_quality_updater_dynamodb_record,
        data_articles_quality_updater_payload,
    ):
        with moto.mock_sqs():
            sqs_resource = boto3.resource("sqs")
            queue = sqs_resource.create_queue(QueueName=common.constants.SQS_QUEUE_ARTICLE_QUALITY_UPDATER)
            self.article_quality_updater = load_lambda_module()
            self.article_quality_updater.sqs._queue = queue
            payload = {"body": json.dumps(data_articles_quality_updater_payload)}
            self.module_article_post.lambda_handler(payload, None)

            response = self.get_article(
                gtin=data_articles_quality_updater_payload["articles"][0]["gtin"],
                channel=data_articles_quality_updater_payload["articles"][0]["channel"],
            )

            article0 = json.loads(response["body"])["articles"][0]
            assert article0["quality"]["qualityTrafficLight"] == "green"

            self.article_quality_updater.lambda_handler(data_article_quality_updater_dynamodb_record, None)
            records = self.transform_message_to_records(self.article_quality_updater.sqs.queue.receive_messages())
            self.module_article_patch.lambda_handler({"Records": records}, context=None)

            response = self.get_article(
                gtin=data_articles_quality_updater_payload["articles"][0]["gtin"],
                channel=data_articles_quality_updater_payload["articles"][0]["channel"],
            )

            article0 = json.loads(response["body"])["articles"][0]
            assert article0["quality"]["qualityTrafficLight"] == "amber"

    @staticmethod
    def transform_message_to_records(messages):
        records = []
        for message in messages:
            records.append({"messageId": message.message_id, "body": message.body})
        return records

    def test_article_quality_updater_force_update_article_quality_when_classification_is_removed(
        self,
        load_lambda_module,
        data_article_quality_updater_dynamodb_record_classification_remove,
        data_articles_quality_updater_payload,
    ):
        self.article_quality_updater = load_lambda_module()
        self.article_quality_updater.sqs = mock.MagicMock()

        payload = {"body": json.dumps(data_articles_quality_updater_payload)}
        self.module_article_post.lambda_handler(payload, None)

        self.article_quality_updater.lambda_handler(
            data_article_quality_updater_dynamodb_record_classification_remove, None
        )
        assert self.article_quality_updater.sqs.send_messages.call_count == 1

    def test_article_patch_updates_article_quality_on_classification_change(
        self,
        data_article_quality_updater_dynamodb_record,
        data_articles_quality_updater_payload,
    ):
        classification_options = DynamoDBDeserializer().deserialize(
            data_article_quality_updater_dynamodb_record["Records"][0]["dynamodb"]["NewImage"]
        )

        payload = {
            "Records": [
                json.dumps(
                    {
                        "articles": data_articles_quality_updater_payload["articles"],
                        "classification_options": classification_options,
                    }
                )
            ],
        }

        self.module_article_patch.lambda_handler(payload, None)

        # patching updates quality
        for article in data_articles_quality_updater_payload["articles"]:
            response = self.get_article(gtin=article["gtin"], channel=article["channel"])

            body = json.loads(response["body"])

            assert "quality" in body["articles"][0]
            assert isinstance(body["articles"][0]["quality"], dict)

    def test_cleanup(self, data_articles_quality_updater_payload):
        table = articlelake.ArticleOperation()

        for article in data_articles_quality_updater_payload["articles"]:
            table._delete(article["gtin"], article["channel"])

        table = articlelake.Classification(force_reload=True)
        for article_type in self.classification_test_items:
            table.delete(article_type)

        assert 0 == len(table.scan()["Items"])

        articlelake.model.article.SharedTable().clear()
        articlelake.model.article_type.SharedTable().clear()
        articlelake.model.channel.SharedTable().clear()
