# -*- coding: utf-8 -*-

"""Logging module for

* local, development purposes
* AWS
* Splunk

"""

import os
import sys
import logging
import urllib.parse

from . import constants
from . import exception

SENTRY_SDK_AVAILABLE = False
if "DISABLE_SENTRY" in os.environ.keys() and os.environ["DISABLE_SENTRY"] == "True":
    pass
else:
    try:
        import sentry_sdk
        import sentry_sdk.utils
        from sentry_sdk import Hub
        from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        SENTRY_SDK_AVAILABLE = True
    except:
        pass


getLogger = logging.getLogger
SDK_DNS = "https://46ef576c32334494862c3be36ac0916e@o1076105.ingest.sentry.io/6077407"
DEFAULT_FORMATTER = logging.Formatter(
    fmt="%(asctime)s [%(threadName)-10s] %(levelname)-5s (%(module)s~%(funcName)s:%(lineno)d) %(message)s"
)


def init_logging(logging_for_name):
    """Creates a new logger instance

    Args:
        logging_for_name (str): name of the logger instance

    """
    logger = logging.getLogger(logging_for_name)
    if hasattr(logger, "_handler_added"):
        return logger
    logger.setLevel(getattr(logging, constants.LOGGING_LEVEL_NAME))
    logger.propagate = False  # Fix for duplicate logging in CloudWatch
    handler = logging.StreamHandler(sys.stdout)  # default to sys.stderr
    handler.setFormatter(DEFAULT_FORMATTER)
    logger.addHandler(handler)
    logger._handler_added = True
    logger.info(f'Logger instance for "{logging_for_name}" created, running on {sys.version}')
    return logger


def init_sentry():
    if SENTRY_SDK_AVAILABLE:
        sentry_logging = LoggingIntegration(
            level=logging.INFO,  # Capture info and above as breadcrumbs
            event_level=logging.ERROR,  # Send errors as events
        )
        sentry_sdk.init(
            dsn=SDK_DNS,
            integrations=[sentry_logging, AwsLambdaIntegration(timeout_warning=True)],
            environment=urllib.parse.urlsplit(constants.API_URL_CORE).netloc,
            before_send=before_sentry_send,
        )
        sentry_sdk.utils.MAX_STRING_LENGTH = 5000


def before_sentry_send(event, hint):
    # see https://docs.sentry.io/platforms/python/configuration/filtering/
    if "exc_info" in hint:
        exc_type, exc_value, tb = hint["exc_info"]
        if isinstance(exc_value, exception.RetryRuntimeError):
            return
        if isinstance(exc_value, exception.DropMessageException):
            return
        if isinstance(exc_value, exception.SQSBatchProcessingError):
            return
    return event


class SentryLogger(logging.Logger):
    """Allows sending additional content in form of "context" or "tag" to sentry. Example:

    logger.error("test error 1", context_attribute={1: 2, 3: 4}, tag_stop=True, tag_build_version="1.2.1")
    leading to sentry tag: stop=True and build_version="1.2.1" and context attribute={1: 2, 3: 4}
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not SENTRY_SDK_AVAILABLE:
            self.clear_sentry_scope = lambda *args, **kwargs: None
            self.set_tag = lambda *args, **kwargs: None
            self.set_context = lambda *args, **kwargs: None

    def _optional_entries(self, clear_scope=False, **kwargs):
        cleaned_kwargs = kwargs.copy()
        tags = []
        contexts = []
        for key, value in kwargs.items():
            if isinstance(key, str):
                if key.startswith("tag_"):
                    tags.append((key.split("tag_")[1], value))
                elif key.startswith("context_"):
                    contexts.append((key.split("context_")[1], value))
                else:
                    continue
                cleaned_kwargs.pop(key)
        if clear_scope:
            self.clear_sentry_scope()
        tags and [self.set_tag(entry[0], entry[1]) for entry in tags]
        contexts and [self.set_context(entry[0], entry[1]) for entry in contexts]
        return cleaned_kwargs

    @staticmethod
    def clear_sentry_scope():
        Hub.current.scope.clear()

    @staticmethod
    def set_tag(name, value):
        Hub.current.scope.set_tag(name, value)

    @staticmethod
    def set_context(name, value):
        if not isinstance(value, dict):
            value = {name: value}
        Hub.current.scope.set_context(name, value)

    def exception(self, msg, *args, **kwargs):
        super().exception(msg, *args, **self._optional_entries(**kwargs))

    def critical(self, msg, *args, **kwargs):
        super().critical(msg, *args, **self._optional_entries(**kwargs))

    def error(self, msg, *args, **kwargs):
        super().error(msg, *args, **self._optional_entries(**kwargs))

    def warning(self, msg, *args, **kwargs):
        super().warning(msg, *args, **self._optional_entries(**kwargs))

    def info(self, msg, *args, **kwargs):
        super().info(msg, *args, **self._optional_entries(**kwargs))

    def debug(self, msg, *args, **kwargs):
        super().debug(msg, *args, **self._optional_entries(**kwargs))


logging.setLoggerClass(SentryLogger)
