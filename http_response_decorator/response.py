"""This module contains the operation handlers for response generation

Currently there are two groups:

1. **ElementResponse**: used to feed data to response from single object (element); enables to generate response with
additional key: item pairs via constructor or/and by supplementing into generate method

2. **Single/ArrayResponse**: used to concatenate ElementResponse(s) towards a final dict/json object
returned by AWS's lambda function

"""


import json
import copy
from http import HTTPStatus

from functools import wraps
from . import logging
from .helpers import JSONEncoder


STATUS_CODE_KEY = "statusCode"
STATUS_TYPE_OK = "ok"
STATUS_TYPE_CREATED = "created"
STATUS_TYPE_ACCEPTED = "accepted"
STATUS_TYPE_NOT_FOUND = "not_found"
STATUS_TYPE_BAD_REQUEST = "bad_request"
STATUS_TYPE_UNAUTHORIZED = "unauthorized"
STATUS_TYPE_FORBIDDEN = "forbidden"
STATUS_TYPE_NOT_IMPLEMENTED = "not_implemented"
STATUS_TYPE_INTERNAL_SERVER_ERROR = "internal_server_error"
STATUS_TYPE_UNPROCESSABLE_ENTITY = "unprocessable_entity"

logger = logging.getLogger(__name__)


def generate_response(type_, message):
    """Base method generating a response

    Args:
        type_ (str): type of response
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return ElementResponse(StatusCode(type_, message))


def generate_ok_response(message=""):
    """Creates an element response with Status OK

    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_OK, message)


def generate_created_response(message=""):
    """Creates an element response with Status Created

    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_CREATED, message)


def generate_accepted_response(message=""):
    """Creates an element response with Status Created

    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_ACCEPTED, message)


def generate_not_found_response(message=""):
    """Creates an element response with Status Not found

    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_NOT_FOUND, message)


def generate_bad_request_response(message=""):
    """Creates an element response with Status Bas Request

    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_BAD_REQUEST, message)


def generate_not_implemented_response(message=""):
    """Creates an element response with Status Not Implemented

    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_NOT_IMPLEMENTED, message)


def generate_internal_server_error_response(message=""):
    """Creates an element response with Status Server Error

    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_INTERNAL_SERVER_ERROR, message)


def generate_unprocessable_entity_response(message=""):
    """Creates an Element Response with a status of Unprocessable Entity"""
    return generate_response(STATUS_TYPE_UNPROCESSABLE_ENTITY, message)


def generate_unauthorized_response(message=""):
    """Creates an element response with Status Unauthorized
    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_UNAUTHORIZED, message)


def generate_forbidden_response(message=""):
    """Creates an element response with Status Forbidden
    Args:
        message (str): additional response message

    Returns:
        ElementResponse: formatted response

    """
    return generate_response(STATUS_TYPE_FORBIDDEN, message)


def generate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, tuple):
            if len(result) == 2:
                response_dict, status_inst = result
            else:
                raise RuntimeError(f"Wrapped callable {func} returning tuple wih {len(result)} elements")
        elif isinstance(result, ElementResponse):
            # this is legitimate, improves readability
            logger.debug("Calling generate decorator multiple times")
            status_inst = result.status
            response_dict = result.get_content()
        elif isinstance(result, StatusCode):
            status_inst = result
            response_dict = {}
        elif isinstance(result, dict):
            status_inst = StatusCode().set_ok()
            response_dict = result
        else:
            raise RuntimeError(f"Wrapped callable {func} returns unknown type {type(result)}")
        return ElementResponse(status_inst, **response_dict)

    return wrapper


class StatusCode(dict):
    """Base class that all StatusCode operations derive from
    Inherits standard HTTPStatus codes and enables to provide additional message.

    """

    # upper layers can set status code directly over http module? Strict: not for now...
    def __init__(self, init_value="not_implemented", init_desc=""):
        super().__init__()
        self._code = None
        self._desc = ""
        getattr(self, f"set_{init_value}")(message=init_desc)

    def __int__(self):
        return self._code.value

    @property
    def code(self):
        return self

    @code.setter
    def code(self, http_status_message):
        """Updates the code of the response

        Args:
            http_status_message (str): additional response description

        """
        self._code = http_status_message[0]
        self._desc = str(http_status_message[1])
        self.clear()
        self["code"] = int(self)
        self["message"] = self._desc

    def is_ok(self):
        """Checks if status code is a success

        Return:
            bool: returns True if status code is in range from 200 to 299

        """
        return 200 <= self._code.value <= 299

    def is_not_found(self):
        """Checks if status code is a not found

        Return:
            bool: returns True if status code is NOT_FOUND

        """
        return self._code.value == HTTPStatus.NOT_FOUND

    def is_unprocessable_entity(self):
        """Checks if status code is unprocessable entity

        Return:
            bool: returns True if status code is UNPROCESSABLE_ENTITY
        """
        return self._code.value == HTTPStatus.UNPROCESSABLE_ENTITY

    def set_ok(self, message=""):
        """Sets the status code to ok: 200

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.OK, message)
        return self

    def set_created(self, message=""):
        """Sets the status code to created: 201

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.CREATED, message)
        return self

    def set_accepted(self, message=""):
        """Sets the status code to created: 202

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.ACCEPTED, message)
        return self

    def set_internal_server_error(self, message=""):

        """Sets the status code to internal server error: 500

        Args:
            message (str): additional response message

        Returns:
            self

        """
        logger.error(message)
        self.code = (HTTPStatus.INTERNAL_SERVER_ERROR, message)
        return self

    def set_not_found(self, message=""):
        """Sets the status code to not found: 404

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.NOT_FOUND, message)
        return self

    def set_not_implemented(self, message=""):
        """Sets the status code to not implemented: 501

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.NOT_IMPLEMENTED, message)
        return self

    def set_bad_request(self, message=""):
        """Sets the status code to bad request: 400

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.BAD_REQUEST, message)
        return self

    def set_unauthorized(self, message=""):
        """Sets the status code to unauthorized: 401

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.UNAUTHORIZED, message)
        return self

    def set_forbidden(self, message=""):
        """Sets the status code to forbidden: 403

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.FORBIDDEN, message)
        return self

    def set_conflicted(self, message=""):
        """Sets the status code to conflicted: 409

        Args:
            message (str): additional response message

        Returns:
            self

        """
        self.code = (HTTPStatus.CONFLICT, message)
        return self

    def set_unprocessable_entity(self, message=""):
        """Sets the status code to 422 Unprocessable Entity.

        Args:
            message: Optional reason for this status code.

        Returns:
            Self.
        """
        self.code = (HTTPStatus.UNPROCESSABLE_ENTITY, message)
        return self


class ElementResponse(dict):
    """Base class that all ElementResponse operations derive from"""

    def __init__(self, __status=None, **kwargs):
        """Initialise ElementResponse base class

        Args:
            __status (dict): status of the response
            kwargs (kwargs): response content

        """
        super().__init__(**kwargs)
        if __status is None:
            __status = StatusCode().set_ok()
        self.status = __status
        self[STATUS_CODE_KEY] = self.status

    def __bool__(self):
        """boolean check against the content, if empty, returns False.

        :return:

        """
        return bool(self.get_content())

    def copy(self, only_include_keys=None) -> "ElementResponse":
        new_copy = copy.deepcopy(self)
        if only_include_keys is None:
            return new_copy
        for key in self:
            if key == STATUS_CODE_KEY:
                continue  # always included
            if key not in only_include_keys:
                new_copy.pop(key)
        return new_copy

    def is_ok(self):
        """Checks if status code is a success

        Return:
            bool: returns True if status code is in range from 200 to 299

        """
        return self.status.is_ok()

    def is_not_found(self):
        """Checks if status code is a not found

        Return:
            bool: returns True if status code is NOT_FOUND

        """
        return self.status.is_not_found()

    def is_unprocessable_entity(self):
        """Checks if status code is a unprocessable entity

        Return:
            bool: returns True if status code is UNPROCESSABLE_ENTITY

        """
        return self.status.is_unprocessable_entity()

    def get_content(self):
        """Returns the content of the response

        Return:
            dict: content of the response

        """
        content = copy.deepcopy(self)
        content.pop(STATUS_CODE_KEY, None)
        return dict(**content)

    def get_status(self) -> StatusCode:
        """Returns the status of the response

        Return: StatusCode

        """
        return self.status

    def get_status_code(self) -> int:
        """Returns the status code (HTTPStatus) of the response

        Return: int value like 200, 201, etc.

        """
        return int(self.status)

    def update(self, **kwargs):
        """Updates the content of the response

        Return:
            self

        """
        super().update(kwargs)
        self[STATUS_CODE_KEY] = self.status
        return self


class SingleResponse:
    """Base class that all SingleResponse operations derive from"""

    def __init__(self, element_response=None):
        """Initialise SingleResponse base class

        Args:
            element_response (None or ElementResponse):

        """
        self._master_key = "body"
        self._do_json_dump = True
        if element_response is None:
            element_response = ElementResponse()
        self.element_response = element_response

    def dump_content(self, response):
        """Dumps content into the master key

        Args:
            response (dict):

        Returns:
            dict: formatted dict

        """
        if self._do_json_dump:
            response[self._master_key] = json.dumps(response[self._master_key], cls=JSONEncoder)
        return response

    def to_dict(self):
        """Generate a json formatted object

        Returns:
            dict: formatted json

        """
        response = {self._master_key: self.element_response.get_content()}
        response[self._master_key][STATUS_CODE_KEY] = self.element_response.get_status()
        to_return = self.dump_content(response)
        to_return[STATUS_CODE_KEY] = int(self.element_response.get_status())
        return to_return


class ArrayResponse(SingleResponse):
    """Base class that all ArrayResponse operations derive from"""

    def __init__(self, array_key, *element_responses: ElementResponse):
        """Dumps content into the master key

        Args:
            array_key (str): name of the root attribute
            element_responses (list): list of element response

        """
        super().__init__()
        self._array_key = array_key
        self.element_responses = element_responses

    def to_dict(
        self,
        initial_status=StatusCode("ok"),
        response_success_regardless=False,
        **flat_kwargs,
    ):
        """Generate a json formatted object

        Args:
            initial_status (StatusCode): in case of empty response, provide initial value
            response_success_regardless (bool): in case of failed responses, state OK anyway
            flat_kwargs (kwargs):

        Returns:
            object: formatted json

        """
        response = {
            self._master_key: {self._array_key: []},
        }
        if not self.element_responses:
            status = initial_status
        else:
            # report containing status with highest HTTP code
            status = max(self.element_responses, key=lambda e_resp: e_resp.get_status_code()).get_status()

        if response_success_regardless:
            status = StatusCode("ok")
        response[self._master_key][STATUS_CODE_KEY] = status
        response[STATUS_CODE_KEY] = int(status)

        for element_response in self.element_responses:
            response[self._master_key][self._array_key].append(element_response)

        response[self._master_key].update(**flat_kwargs)
        return self.dump_content(response)
