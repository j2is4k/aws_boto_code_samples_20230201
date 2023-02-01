from aws_lambda_powertools import Tracer
from typing import Any, Callable, Dict, Optional, Union
import functools
import json

from common import logging

logger = logging.getLogger("core")


class XRay(Tracer):
    request_id: str = None

    def trace_lambda_handler(
        self,
        lambda_handler: Union[Callable[[Dict, Any], Any], Optional[Callable[[Dict, Any, Optional[Dict]], Any]]] = None,
        truncate: Optional[bool] = None,
    ):
        if lambda_handler is None:
            logger.debug("Decorator called with parameters")
            return functools.partial(self.trace_lambda_handler, truncate=truncate)

        @functools.wraps(lambda_handler)
        def decorate(event, context, **kwargs):
            try:
                function_name = context.function_name
            except AttributeError:
                function_name = lambda_handler.__name__
            self.request_id = event.get("requestContext", {}).get("requestId")
            with self.provider.in_subsegment(name=f"## {function_name}") as subsegment:
                subsegment.put_annotation(key="request_id", value=self.request_id)
                subsegment.put_annotation(key="lambda_name", value=function_name)
                subsegment.put_metadata(key="identity", value=event.get("requestContext", {}).get("identity"))
                request_body = json.loads(event.get("body", "{}"))

                # because X-RAY accept only metadata less than 64K
                if truncate and "articles" in request_body.keys() and len(request_body["articles"]) > 10:
                    request_body["articles"] = request_body["articles"][:10]

                subsegment.put_metadata(key="request_body", value=request_body)
                try:
                    response = lambda_handler(event, context, **kwargs)
                except Exception as err:
                    logger.error(f"XRay error when handling {function_name}: {err}")
                    raise
                return response

        return decorate


_tracer = None


def get_tracer():
    global _tracer
    if not _tracer:
        _tracer = XRay(service="articlelake")
    return _tracer
