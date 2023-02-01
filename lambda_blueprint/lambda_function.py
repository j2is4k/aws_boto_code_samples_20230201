# TODO: move to plugin/handler folder
# TODO: implement functionality?


import json
import common
import common.logging
import common.response


common.init()
logger = common.logging.init_logging(__name__)


def no_op_event(event):
    body = json.loads(event["body"])
    logger.error(f"Received dlq message {body}.")
    return body


@common.response.generate
def no_op(event):
    body = {}
    try:
        body = no_op_event(event)
        return common.response.StatusCode().set_ok()
    except Exception as exc:
        msg = f"Received dlq message for topic: {body}. See exception: {exc}"
        return common.response.StatusCode().set_internal_server_error(msg)


def lambda_handler(event, context):
    """Lambda handler to process event when messages land in dead letter queue for SNS topics.
    For now just a noop.
    """
    return common.response.SingleResponse(no_op(event)).to_dict()
