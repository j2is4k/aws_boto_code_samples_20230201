import logging
from enum import Enum, auto


logger = logging.getLogger(__name__)


def get_possible_index(str_):
    if str_.startswith("[") and str_.endswith("]"):
        try:
            return int(str_[1:-1])
        except ValueError:
            logger.exception(f"Cannot convert list index to integer: {str_}")
            return None
    else:
        return str_


def extract_data(lookup_reference, from_dotted_key):
    index_or_key = get_possible_index(from_dotted_key)
    if index_or_key is None:
        return FailedExtract()
    elif isinstance(index_or_key, int):
        type_ = list
    else:
        type_ = dict
    try:
        ref = lookup_reference.__getitem__(index_or_key)
    except (IndexError, KeyError, TypeError, AttributeError) as exc:
        # IndexError if list ref invalid
        # KeyError if dict key invalid
        # TypeError if ref is already a value
        # AttributeError if it is value and int
        return FailedExtract()
    return ref


class DottedState(Enum):
    LAST_REFERENCE = auto()
    NEXT_REFERENCE = auto()


class FailedExtract(object):
    pass


class DottedNotationMixin:
    def __getitem__(self, key, default=None):
        if "." in key:
            for ref_status, ref_or_value, last_ref, remaining_key in self.iter_getter(self, key):
                if ref_status == DottedState.LAST_REFERENCE:
                    return ref_or_value
        try:
            return super().__getitem__(key)
        except KeyError:
            return default

    def __setitem__(self, key, value):
        if "." in key:
            for ref_status, _, last_ref, remaining_key in self.iter_getter(self, key):
                if ref_status == DottedState.LAST_REFERENCE:
                    return self.iter_setter(_, last_ref, remaining_key, value)
        return super().__setitem__(key, value)

    def get(self, key, default=None):
        return self.__getitem__(key, default)

    def iter_setter(self, ref_or_value, last_ref, remaining_key, value):
        keys = remaining_key.split(".")
        key_types = [(key, get_possible_index(key)) for key in keys]
        ref = last_ref
        for traversal_id in range(len(keys)):
            key, maybe_index = key_types[traversal_id]
            if maybe_index is None:
                return
            is_last_key = traversal_id + 1 == len(keys)
            if isinstance(maybe_index, str) and isinstance(ref, dict):
                if is_last_key:
                    ref[key] = value
                elif isinstance(key_types[traversal_id + 1][1], str):
                    ref = ref.setdefault(key, {})
                else:
                    ref = ref.setdefault(key, [])
            elif isinstance(maybe_index, str) and isinstance(ref, list):
                if is_last_key:
                    ref.insert(0, value)
                elif isinstance(key_types[traversal_id + 1][1], str):
                    ref.insert(0, {})
                    ref = ref[0]
                else:
                    ref.insert(0, [])
                    ref = ref[0]
            elif isinstance(maybe_index, int) and isinstance(ref, list):
                if is_last_key:
                    ref.insert(maybe_index, value)
                else:
                    if isinstance(key_types[traversal_id + 1][1], str):
                        insert_value = {}
                    else:
                        insert_value = []
                    while len(ref) <= maybe_index:
                        ref.append(None)
                    ref[maybe_index] = insert_value
                    ref = ref[maybe_index]
            else:
                logger.error(f"unable to set with key: {key}, value: {value} to reference: {ref}", context_dict=self)

    def iter_getter(self, lookup_reference, dotted_keys: str):
        last_reference = lookup_reference
        remaining_keys = dotted_keys
        while 1:
            keys = dotted_keys.split(".", 1)
            extracted_reference = extract_data(lookup_reference, keys[0])
            if len(keys) == 1:
                if isinstance(extracted_reference, FailedExtract):
                    yield DottedState.LAST_REFERENCE, None, last_reference, remaining_keys
                    return
                else:
                    yield DottedState.LAST_REFERENCE, extracted_reference, last_reference, remaining_keys
                    return
            else:
                if isinstance(extracted_reference, FailedExtract):
                    yield DottedState.LAST_REFERENCE, None, last_reference, remaining_keys
                    return
                else:
                    yield DottedState.NEXT_REFERENCE, extracted_reference, lookup_reference, remaining_keys
                    if type(extracted_reference) in (dict, list):
                        last_reference = extracted_reference
                        remaining_keys = keys[1]
                    lookup_reference = extracted_reference
                    dotted_keys = keys[1]


class DottedNotationDict(DottedNotationMixin, dict):
    pass
