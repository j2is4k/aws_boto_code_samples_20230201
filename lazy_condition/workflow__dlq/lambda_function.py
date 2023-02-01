"""
AWS Resources / Permissions
  SQS

  * read

"""


import common
import common.exception


common.init()
logger = common.logging.init_logging(__name__)


@common.exception.lambda_exception_handler(logger)
def lambda_handler(event, context):
    """Lambda handler to process event."""
    logger.error(f"Received: {event}, context: {context}")
