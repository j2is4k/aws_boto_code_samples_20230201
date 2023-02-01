""" Contains a few common exceptions
Classes:
    InvalidProtocolError
Functions:
    lambda_exception_handler

"""

import json
import functools
import traceback
from http import HTTPStatus


NOT_PROD = 1


class InvalidProtocolError(Exception):
    """Exception raised for invalid protocol values.

    Attributes:
        message -- Only http or https are supported for protocol.
    """

    def __init__(self, message="Invalid protocol value. Only http or https are supported."):
        self.message = message
        super().__init__(self.message)


class ArticleReleaseNotFoundError(RuntimeError):
    MSG_TEMPLATE = "Could not find article release with gtin {} and version id {}"

    def __init__(self, gtin, version_id):
        super().__init__(self.MSG_TEMPLATE.format(gtin, version_id))


class OptionsClassificationClientError(RuntimeError):
    MSG_TEMPLATE = "Error retrieving classification options for article type {}"

    def __init__(self, article_type):
        super().__init__(self.MSG_TEMPLATE.format(article_type))


class RetryRuntimeError(RuntimeError):
    pass


class DropMessageException(Exception):
    pass


class AuthError(Exception):
    pass


class InfaPimError(Exception):
    pass


class InsufficientInformationError(Exception):
    pass


class ValidationError(Exception):
    pass


class LockFailedError(Exception):
    pass


class SQSBatchProcessingError(Exception):
    """When at least one message within a batch could not be processed"""

    def __init__(self, msg="", child_exceptions=()):
        super().__init__(msg)
        self.msg = msg
        self.child_exceptions = child_exceptions

    # Overriding this method so we can output all child exception tracebacks when we raise this exception to prevent
    # errors being lost. See https://github.com/awslabs/aws-lambda-powertools-python/issues/275
    def __str__(self):
        parent_exception_str = super(SQSBatchProcessingError, self).__str__()
        exception_list = [f"{parent_exception_str}\n"]
        for exception in self.child_exceptions:
            extype, ex, tb = exception
            formatted = "".join(traceback.format_exception(extype, ex, tb))
            exception_list.append(formatted)

        return "\n".join(exception_list)


def lambda_exception_handler(logger=None, reraise_as=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as err:
                message = f"{type(err).__name__} was raised: {err} when calling {func.__name__}"
                # cloud watch log streams posts only one-liners
                traceback_formatted = ("".join(traceback.format_tb(err.__traceback__))).replace("\n", "\\n")
                if reraise_as:
                    if logger:
                        logger.warning(
                            f"Reraised error: {message}, tb: {traceback_formatted}",
                            context_fnc_params={"args": args, "kwargs": kwargs},
                        )
                    raise reraise_as(message)
                status_code = {
                    "code": HTTPStatus.INTERNAL_SERVER_ERROR,
                    "message": message,
                }
                if NOT_PROD:
                    status_code["traceback"] = traceback_formatted
                message = f"{type(err).__name__} was raised: {err}, traceback: {traceback_formatted}"
                to_return = {
                    "statusCode": int(HTTPStatus.INTERNAL_SERVER_ERROR),
                    "body": json.dumps({"statusCode": status_code}),
                }

                if logger:
                    logger.exception(
                        message,
                        context_fnc_params={"args": args, "kwargs": kwargs},
                        tag_fnc_name=func.__name__,
                    )

                from . import splunk

                splunk.EventCollector(args[0], context=args[1]).send_message(
                    error=message,
                    statusCode=to_return["statusCode"],
                )
                return to_return

        return wrapper

    return decorator
