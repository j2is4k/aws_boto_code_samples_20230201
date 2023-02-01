import os
import sys
import pytest
import logging
import importlib
import importlib.util

os.environ["DISABLE_SENTRY"] = "True"

pytest_plugins = [
    "tests.sample.data.fnctest.adapter.best",
    "tests.sample.data.fnctest.article.article",
    "tests.sample.data.fnctest.article.article_anchor",
    "tests.sample.data.fnctest.article.article_classification",
    "tests.sample.data.fnctest.article.article_rendering",
    "tests.sample.data.fnctest.article.article_cleaning_dictionary",
    "tests.sample.data.fnctest.article.article_decoration",
    "tests.sample.data.fnctest.article.article_golden_record",
    "tests.sample.data.fnctest.article.article_gsi",
    "tests.sample.data.fnctest.article.article_history",
    "tests.sample.data.fnctest.article.article_key",
    "tests.sample.data.fnctest.article.article_retire",
    "tests.sample.data.fnctest.article.article_versioning",
    "tests.sample.data.fnctest.article.bulk.article_bulk",
    "tests.sample.data.fnctest.article.list.article_list",
    "tests.sample.data.fnctest.article.quality.article_quality",
    "tests.sample.data.fnctest.article.attribute.attribute_definition",
    "tests.sample.data.fnctest.brand.brand_updater",
    "tests.sample.data.fnctest.channel.channel",
    "tests.sample.data.fnctest.event.event_splunk_sqs",
    "tests.sample.data.fnctest.filter_dictionary.filter_dictionary",
    "tests.sample.data.fnctest.inventory.inventory",
    "tests.sample.data.fnctest.media.initial",
    "tests.sample.data.fnctest.media.naming_paths",
    "tests.sample.data.fnctest.media.pagination",
    "tests.sample.data.fnctest.media.resizing",
    "tests.sample.data.fnctest.media.retire",
    "tests.sample.data.fnctest.media.tagging",
    "tests.sample.data.fnctest.media.validations",
    "tests.sample.data.fnctest.model.article",
    "tests.sample.data.fnctest.model.article_release",
    "tests.sample.data.fnctest.model.article_type",
    "tests.sample.data.fnctest.user.user",
    "tests.sample.data.fnctest.plugin.cxl_dynamodb_update_product",
    "tests.sample.data.fnctest.plugin.image_editing_post_processor",
    "tests.sample.data.fnctest.plugin.medialake_image_editing_image_collector",
    "tests.sample.data.fnctest.plugin.medialake_image_labeler",
    "tests.sample.data.fnctest.plugin.pubsub_subscription_deleted",
    "tests.sample.data.fnctest.plugin.article_workflow_status_125",
    "tests.sample.data.fnctest.plugin.workflow_article_status_100",
    "tests.sample.data.fnctest.plugin.workflow_article_status_145",
    "tests.sample.data.fnctest.plugin.workflow_article_status_199",
    "tests.sample.data.fnctest.plugin.workflow_article_sap_inject",
    "tests.sample.data.fnctest.plugin.workflow_article_sfb_change",
    "tests.sample.data.fnctest.product.product",
    "tests.sample.data.fnctest.product.list.product_list",
    "tests.sample.data.fnctest.product.quality.product_quality",
    "tests.sample.data.fnctest.release.article.quality.release_article_quality",
    "tests.sample.data.fnctest.release.article.release_article",
    "tests.sample.data.fnctest.release.article.release_article_list",
    "tests.sample.data.fnctest.release.product.release_product",
    "tests.sample.data.fnctest.supply_chain.supply_chain",
    "tests.sample.data.fnctest.workbench.container.wb_ff__self",
    "tests.sample.data.fnctest.workbench.container.wb_ff_assignment",
    "tests.sample.data.fnctest.workbench.container.wb_ff_iterchannel",
    "tests.sample.data.fnctest.workbench.container.wb_ff_model",
    "tests.sample.data.fnctest.workbench.container.wb_ff_priority",
    "tests.sample.data.fnctest.workbench.container.wb_ff_stage",
    "tests.sample.data.fnctest.workbench.model.wb_ff_model",
    "tests.sample.data.fnctest.authentication.auth",
    "tests.sample.data.fnctest.authentication.cognito",
]

logger = logging.getLogger("test")

# in order to see common package and test package properly
sys.path.append(os.path.abspath(os.path.join(__file__, "../layer")))
sys.path.append(os.path.abspath(os.path.join(__file__, "../../")))

# Bitbucket pipeline automatically sets the environment Variable CI = True
os.environ["TEST_TYPE_PIPE"] = os.environ.get("CI", "false")


LOADED_LAMBDAS = {}


@pytest.fixture(scope="class")
def load_lambda_module(request):
    logger.info(request.module)

    def _load_lambda_module(parent_dir="", module_name="lambda_function", reload=False):
        abs_path = os.path.abspath(os.path.join(request.fspath.dirname, parent_dir, module_name + ".py"))
        if abs_path not in LOADED_LAMBDAS or reload is True:
            spec = importlib.util.spec_from_file_location(module_name, abs_path)
            module_ref = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module_ref)
            logger.info(f"imported module from {abs_path}")
            LOADED_LAMBDAS[abs_path] = module_ref
        return LOADED_LAMBDAS[abs_path]

    yield _load_lambda_module


@pytest.fixture(scope="class")
def abs_path_finder(request):
    dirname = os.path.dirname(request.module.__file__)
    return lambda subpath: os.path.join(dirname, subpath)
