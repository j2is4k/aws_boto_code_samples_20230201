"""Handler orchestrating workflow distribution to workflow entities.

********************
Description
********************

Modify DISTRIBUTION_RULES if you need to adapt rules upon which the a workflow instance should be used.

You can also define same rule for multiple targets or one target triggered by matching multiple roles.

The function gets triggered by reception of single SNS notification, therefore this resource experiences high execution
frequency. By the set of rules, the SNS notification is forwarded to a standard SQS which is in turn handled by
dedicated lambda. In order not to overload the receiving queue, rather the handling lambda, message in the queue can
be delayed in given range to force the queue consumer to receive the message in statistically calculated range, hence
limiting the lambda execution concurrency.

DelayedSeconds value is a per workflow entity random number:

    delay_offset + random(0, max_delay)

where the beta probability density function can be further parametrised.


AWS Resources / Permissions
  SNS

  * read

  SQS

  * read
  * write

"""

import json
import random

import common
import common.exception as exception
from common import aws
from common import helpers
from common import constants
from common.helpers import LazyCondition as Condition
from common.helpers import MessageAttributes


common.init()
logger = common.logging.init_logging("workflow")


# args = (queue, delay_offset, max_delay, pdf_betavar_alpha, pdf_betavar_beta)
workflow_article_sap_inject = (aws.SQSQueue(constants.SQS_QUEUE_WORKFLOW_ARTICLE_SAP_INJECT), 0, 0, 1, 1)
workflow_article_status_100 = (aws.SQSQueue(constants.SQS_QUEUE_WORKFLOW_ARTICLE_STATUS_100), 0, 0, 1, 1)
workflow_article_status_125 = (aws.SQSQueue(constants.SQS_QUEUE_WORKFLOW_ARTICLE_STATUS_125), 0, 0, 1, 1)
workflow_article_status_145 = (aws.SQSQueue(constants.SQS_QUEUE_WORKFLOW_ARTICLE_STATUS_145), 0, 0, 1, 1)
workflow_article_product_change = (aws.SQSQueue(constants.SQS_QUEUE_WORKFLOW_ARTICLE_PRODUCT_CHANGE), 0, 0, 1, 1)
workflow_article_ddm_change = (aws.SQSQueue(constants.SQS_QUEUE_WORKFLOW_ARTICLE_DDM_CHANGE), 0, 0, 1, 1)
workflow_article_sfb_change = (aws.SQSQueue(constants.SQS_QUEUE_WORKFLOW_ARTICLE_SFB_CHANGE), 0, 0, 1, 1)


DISTRIBUTION_RULES = {
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_ARTICLE)
    & Condition("status").contains("100")
    & Condition("channel").equals("gkkDigitalDataManagement"): workflow_article_status_100,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_MEDIA): workflow_article_status_100,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_ARTICLE)
    & Condition("status").contains("125")
    & Condition("channel").equals("gkkDigitalDataManagement")
    & Condition("article").contains("articleAttributes"): workflow_article_status_125,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_ARTICLE)
    & Condition("channel").equals("gkkDigitalDataManagement").invert()
    & Condition("channel").equals("gkkXtraMetaAttributes").invert()
    & Condition("channel").startswith("gkkCilGkkSap").invert()
    & Condition("channel").startswith("_").invert(): workflow_article_status_125,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_ARTICLE)
    & Condition("status").contains("125")
    & Condition("channel").equals("gkkDigitalDataManagement")
    & Condition("lifecycle").contains("contentCreationPriorityTier"): workflow_article_status_125,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_MEDIA)
    & Condition("media_type").equals("AuxiliaryImage"): workflow_article_status_125,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_ARTICLE)
    & Condition("status").contains("145")
    & Condition("channel").equals("gkkDigitalDataManagement"): workflow_article_status_145,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_MEDIA)
    & Condition("channel").equals("gkkDigitalDataManagement")
    & Condition("status").contains("ML070QaApproved"): workflow_article_status_145,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_MEDIA)
    & Condition("channel").equals("gkkDigitalDataManagement")
    & Condition("status").contains("Media.Active.490.Finished"): workflow_article_status_145,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_ARTICLE)
    & Condition("channel").startswith("_gkkSapLookup"): workflow_article_sap_inject,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_PRODUCT): workflow_article_product_change,
    #
    Condition("topic_name").equals(constants.SNS_TOPIC_NAME_NOTIFICATION_ARTICLE)
    & Condition("channel").equals("gkkCilGkkSapForwardBulk"): workflow_article_sfb_change,
}


class MessageSender:
    """Purpose of context manager is to skip sending duplicated messages to the same queue"""

    def __init__(self, message, message_attributes):
        self.message = message
        self.message_attributes = MessageAttributes.from_attributes(message_attributes)
        self._collected_targets = []
        self._buffering_messages = False

    def __enter__(self):
        self._buffering_messages = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._buffering_messages = False
        logger.info(f"Sending on context exit")
        for entity_args in self._collected_targets:
            self.send(*entity_args)

    def send(self, queue, delay_offset, max_delay, alpha, beta):
        if self._buffering_messages:
            entity_args = (queue, delay_offset, max_delay, alpha, beta)
            logger.info(f"Preparing to send: {entity_args}")
            # skip duplicated messages
            if entity_args not in self._collected_targets:
                self._collected_targets.append(entity_args)
        else:
            delay_seconds = delay_offset + round(max_delay * random.betavariate(alpha, beta))
            logger.info(f"Sending: {queue, delay_offset, max_delay, alpha, beta}")
            logger.info(f"Sending message: {self.message}")
            logger.info(f"Sending message_attributes: {self.message_attributes}")
            return queue.send_message(
                MessageBody=json.dumps(self.message, cls=helpers.JSONEncoder),
                MessageAttributes=self.message_attributes,
                DelaySeconds=delay_seconds,
            )


def distribute_notification_to_queue(notification):
    topic_name = notification["TopicArn"].split(":")[-1]
    message = json.loads(notification["Message"], cls=helpers.JSONDecoder)
    message_attributes = notification["MessageAttributes"]
    channel = message.get("channel")
    media_type = message.get("mediaType", "")
    status = constants.STATUS_LOOKUP[message.get("status")]
    article = message.get("article", {})
    lifecycle = article.get("lifecycle", {})
    local_variables = locals()

    with MessageSender(message, message_attributes) as message_sender:
        for rule, workflow_entity_args in DISTRIBUTION_RULES.items():
            if rule(**local_variables):
                message_sender.send(*workflow_entity_args)


@exception.lambda_exception_handler(logger, reraise_as=exception.RetryRuntimeError)
def lambda_handler(event, context):
    for record in event.get("Records", []):
        item = record.get("Sns", {})
        if item.get("Type") == "Notification":
            distribute_notification_to_queue(item)
