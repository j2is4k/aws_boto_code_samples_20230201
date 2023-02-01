"""Splunk event is sent in two steps:

#. Reporting lambda handler sends an event to a /event EP -> post_event
#. Receiving /event EP sends the event to splunk -> _send_to_splunk

"""
import os
import json
import time

from .aws import SQSQueue
from . import helpers
from . import logging
from . import constants
from . import apirequest


logger = logging.getLogger(__name__)
_SQS_QUEUE_SPLUNK_LOGS = None


def splunk_logs_queue():
    global _SQS_QUEUE_SPLUNK_LOGS
    if not _SQS_QUEUE_SPLUNK_LOGS:
        _SQS_QUEUE_SPLUNK_LOGS = SQSQueue(constants.SQS_QUEUE_SPLUNK_LOGS)
    return _SQS_QUEUE_SPLUNK_LOGS


class LambdaEventContext:
    """Base class that all Lambda Event Context implementations derive from"""

    def __init__(self, event, context):
        """Initialize MediaTagging class

        Args:
            event (dict):
            context (object or None):

        """
        self.event = event
        self.context = context

    def is_legible(self):
        """Checks if Lambda Event is eligible for Splunk logging

        Returns:
            bool: True if Lambda has context

        """
        if self.context and getattr(self.context, "aws_request_id", ""):
            return True
        return False


class EventCollector(LambdaEventContext):
    """Base class that all Event Collector implementations derive from.
    It inherits LambdaEventContext.

    """

    def __init__(self, event, context=None):
        """Initialize EventCollector class

        Args:
            event (dict):
            context (object or None):

        """
        super().__init__(event, context)
        self._buffering_messages = False
        self._buffered_messages = []

    def __enter__(self):
        self._buffering_messages = True
        return self

    def __exit__(self, type_, value, traceback):
        # return of not True forces any exception between enter and exit to be re-raised.
        self._send_buffered_messages()

    def send_prepared_messages(self, *prepared_data):
        if not self.is_legible():
            return False
        return self._send_prepared_data({"events": prepared_data})

    def send_message(self, *prepared_data, **business_data) -> bool:
        """Push message to SQS. If the method is called within context manager, the message to splunk is
        buffered and sent on context manager exit.

        Args:
            prepared_data (arg): supporting events parameters like URL, host and namespace.
            business_data (kwargs): the real data of the event.

        Returns:
            success_flag (bool)

        """
        if not self.is_legible():
            return False
        return self._send_prepared_data({"events": [self._prepare_data(**business_data)]})

    def _send_prepared_data(self, data):
        for event in data["events"]:
            if (
                len(json.dumps(event["businessData"], cls=helpers.JSONEncoder))
                > constants.SPLUNK_BUSINESS_DATA_MAX_SIZE
            ):
                long_business_data = json.dumps(event.pop("businessData"), cls=helpers.JSONEncoder)
                event["businessData"] = dict(
                    trimmed_business_data=long_business_data[: constants.SPLUNK_BUSINESS_DATA_MAX_SIZE],
                    error_message="Business data too long",
                )

        data_dump = json.dumps(data, cls=helpers.JSONEncoder)
        if self._buffering_messages:
            self._buffered_messages.append(data_dump)
            return True
        sqs_response = splunk_logs_queue().send_message(MessageBody=data_dump)
        if sqs_response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
            logger.warning(sqs_response)
            return False
        return True

    def _send_buffered_messages(self):
        batches = [
            self._buffered_messages[i : i + constants.SQS_QUEUE_SPLUNK_MESSAGE_MAX_BATCH_SIZE]
            for i in range(0, len(self._buffered_messages), constants.SQS_QUEUE_SPLUNK_MESSAGE_MAX_BATCH_SIZE)
        ]
        for batch in batches:
            entries = [
                {
                    "Id": f"id_{i}",
                    "MessageBody": body,
                }
                for i, body in enumerate(batch)
            ]
            response = splunk_logs_queue().send_messages(Entries=entries)
            if response.get("Failed"):
                logger.error(response)

    def _prepare_data(self, **business_data):
        """Returns a dictionary of data that will be sent to splunk

        Args:
            business_data (kwargs): the real data of the event

        Returns:
            dict: prepared data containing the business data
        """
        return dict(
            # user data
            businessData=dict(business_data),
            # meta data
            contentVersion=constants.SPLUNK_CONTENT_VERSION,
            # system related
            host=self.event.get("headers", {}).get("Host", ""),
            namespace=os.environ.get("BALDENEY_NAMESPACE", ""),
            # location related
            uriPath=self.event.get("path"),
            uriResource=self.event.get("resource"),
            uriPathParameters=self.event.get("pathParameters"),
            uriQueryStringParameters=self.event.get("queryStringParameters"),
            requestContext=self.event.get("requestContext"),
            # function related
            remainingTimeInMiliseconds=getattr(self.context, "get_remaining_time_in_millis", lambda: 0)(),
            localEpochtimeInMiliseconds=int(time.time() * 1000),
            functionName=getattr(self.context, "function_name", ""),
            invokedFunctionArn=getattr(self.context, "invoked_function_arn", ""),
            logGroupName=getattr(self.context, "log_group_name", ""),
            logStreamName=getattr(self.context, "log_stream_name", ""),
            memoryLimitInMB=getattr(self.context, "memory_limit_in_mb", ""),
        )


class SplunkClient:
    """Base class that all Splunk Collector implementations derive from"""

    def __init__(self):
        """Initialize SplunkClient class"""
        if not constants.URL_TOKEN_SPLUNK:
            logger.error(f"Splunk target {constants.URL_TARGET_SPLUNK} is missing authorisation header.")
        self.ep_splunk = apirequest.RequestBase(
            base_url=constants.URL_TARGET_SPLUNK,
            auth=None,
            headers={"Authorization": f"Splunk {constants.URL_TOKEN_SPLUNK}"},
        )

    def post(self, event: dict):
        """Posts event to Splunk endpoint

        Args:
            event (object): data to be sent

        Returns:
            dict: transaction result

        """
        data = {"event": event, "host": event.get("host", "unknown host")}
        return_value = self.ep_splunk.post(data=data, convert2bytes=True)
        logger.info(f"sending directly to splunk, return value: {return_value}")
        return return_value


class FotoFlowMixin:
    """Base class that all FotoFlowMixin implementations derive from
    These are implementation exclusive for FotoFlow to send events to splunk.

    """

    def assign_collector(self, collector):
        """Assigns a collector

        Args:
            collector (EventCollector): an instance of Splunk Collector

        Returns:
            dict: transaction result

        """
        self.splunk_collector = collector

    def report_new_container(self, response, **business_data):
        """Send a report when a new container is created

        Args:
            response (dict): response of the main lambda call
            business_data (kwargs): data to be sent
        """
        self.splunk_collector.send_message(
            comment="Container Level Log",
            statusCode=response.get_status_code(),
            **business_data,
        )

    def report_add_article(self, response, item, gtin, article_stage):
        """Send a report when a new article is added

        Args:
            response (dict): response of the main lambda call
            item (dict): article that was added
            gtin (str): gtin of the article
            article_stage (str): stage of the article
        """
        self.splunk_collector.send_message(
            gtin=gtin,
            articleStage=article_stage,
            containerStage=item["containerStage"],
            channel=item["channel"],
            containerKey=item["containerKey"],
            modelKey=item.get("modelKey", ""),
            statusCode=response.get_status_code(),
        )

    def report_delete_article(self, response, item, gtin, article_stage):
        """Send a report when an article is deleted

        Args:
            response (dict): response of the main lambda call
            item (dict): article that was deleted
            gtin (str): gtin of the article
            article_stage (str): stage of the article
        """
        self.splunk_collector.send_message(
            gtin=gtin,
            articleStage=article_stage,
            containerStage=item["containerStage"],
            channel=item["channel"],
            containerKey=item["containerKey"],
            modelKey=item.get("modelKey", ""),
            statusCode=response.get_status_code(),
        )

    def report_delete_all_articles(self, response, item, gtins, container_stage):
        """Send a report when an all article are deleted

        Args:
            response (dict): response of the main lambda call
            item (dict): article that was deleted
            gtins (str): list of gtins of the article deleted articles
            container_stage (str): stage of the article
        """
        # TODO: container_stage should be removed
        if not self.splunk_collector:
            return
        for gtin in gtins:
            self.splunk_collector.send_message(
                comment="sub-procedure deleting all articles triggered",
                gtin=gtin,
                articleStage=item["containerStage"],
                containerStage=item["containerStage"],
                channel=item["channel"],
                containerKey=item["containerKey"],
                modelKey=item.get("modelKey", ""),
                statusCode=response.get_status_code(),
            )
        self.splunk_collector.send_message(
            comment="Container Level Log",
            containerStage=item["containerStage"],
            channel=item["channel"],
            containerKey=item["containerKey"],
            modelKey=item.get("modelKey", ""),
            statusCode=response.get_status_code(),
        )

    def report_change_articles(self, response, item, back_from_waiting_stage):
        """Send a report when article has been updated

        Args:
            response (dict): response of the main lambda call
            item (dict): article that was deleted
            back_from_waiting_stage (bool): if the article is coming from a waiting stage
        """
        if not self.splunk_collector or back_from_waiting_stage:
            # don't report if moving back to previous container stage
            return
        wait_stage = "wait" in item["containerStage"].lower()
        for key, value in item["articles"].items():
            self.splunk_collector.send_message(
                gtin=key,
                articleStage=wait_stage and item["containerStage"] or value["articleStage"],
                containerStage=item["containerStage"],
                channel=item["channel"],
                containerKey=item["containerKey"],
                modelKey=item.get("modelKey", ""),
                statusCode=response.get_status_code(),
            )
        self.splunk_collector.send_message(
            comment="Container Level Log",
            backFromWaitingStage=back_from_waiting_stage,
            containerStage=item["containerStage"],
            channel=item["channel"],
            containerKey=item["containerKey"],
            modelKey=item.get("modelKey", ""),
            statusCode=response.get_status_code(),
        )
