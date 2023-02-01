import json
import logging
import pytest
from http import HTTPStatus

from articlelake import articlelake

logger = logging.getLogger("test")


class TestAdapterBest:
    @pytest.fixture(autouse=True)
    def setup(self, load_lambda_module):
        self.module_get = load_lambda_module(parent_dir="_get")
        self.module_post = load_lambda_module(parent_dir="_post")
        self.module_patch = load_lambda_module(parent_dir="_patch")

    def get_article(self, gtin, channel):
        event = {"headers": {"gtin": gtin, "channel": channel}}
        get_response = self.module_get.lambda_handler(event, None)
        return get_response

    def post_or_patch_article(self, data, context=None, method="POST"):
        event = {"body": json.dumps(data)}
        if method == "POST":
            response = self.module_post.lambda_handler(event, context)
        else:
            response = self.module_patch.lambda_handler(event, context)
        return response

    def test_post_payload(self, data_article_post_best_payload):
        response = self.post_or_patch_article(data_article_post_best_payload)

        assert HTTPStatus.OK == response["statusCode"]
        body = json.loads(response["body"])
        first_returned_article = body["articles"][0]
        assert HTTPStatus.CREATED == first_returned_article["statusCode"]["code"]
        assert "articles" in body
        assert len(body["articles"]) == 4

    @pytest.mark.parametrize(
        "gtin, channel, should_have_hashed_references",
        [
            ("9990000000104", "CHANNEL_100", True),
            ("9990000000111", "CHANNEL_100", True),
            ("9990000000128", "CHANNEL_100", True),
            ("9990000000135", "CHANNEL_100", False),
        ],
    )
    def test_get(self, gtin, channel, should_have_hashed_references):
        response = self.get_article(gtin=gtin, channel=channel)
        body = json.loads(response["body"])
        assert HTTPStatus.OK == response["statusCode"]
        assert "articles" in body
        assert len(body["articles"]) == 1
        article = json.loads(response["body"])["articles"][0]
        hashed_anchor_gtin = None
        hashed_gtin = None
        try:
            hashed_anchor_gtin = article["references"]["supplyChain"][0]["aux"]["hashedAnchorGTIN"]
            hashed_gtin = article["references"]["supplyChain"][0]["aux"]["hashedGTIN"]
        except KeyError as e:
            pass
        assert should_have_hashed_references == bool(hashed_anchor_gtin and hashed_gtin)

    def test_get_pid(self, data_article_get_vid_pid):
        article_response = self.get_article(
            gtin=data_article_get_vid_pid["gtin"], channel=data_article_get_vid_pid["channel"]
        )
        body = json.loads(article_response["body"])
        article = articlelake.Article(**body["articles"][0])
        assert article.get_pid() == "GJGNMK46"
        assert article.get_vid() == "GJGNMK4603"

    def test_cleanup(self):
        articles = [
            {"gtin": "9990000000104", "channel": "CHANNEL_100"},
            {"gtin": "9990000000111", "channel": "CHANNEL_100"},
            {"gtin": "9990000000128", "channel": "CHANNEL_100"},
            {"gtin": "9990000000135", "channel": "CHANNEL_100"},
        ]
        table = articlelake.model.ArticleModel()
        table.select_table(self.module_post.common.constants.DYNAMO_DB_TABLE_NAME_ARTICLELAKE)

        for article in articles:
            table._delete(article["gtin"], article["channel"])

        articlelake.model.article.SharedTable().clear()
        articlelake.model.channel.SharedTable().clear()


if __name__ == "__main__":
    pytest.main([__file__])
