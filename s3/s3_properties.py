"""This module contains the operation handlers for Amazon S3 access through Boto3 specific to tags

"""
from typing import Optional
import botocore.exceptions

from .aws import default_client
from . import logging
from . import constants
from . import helpers


logger = logging.getLogger(__name__)


class MediaTagging:
    """Base class that all S3 access point implementations derive from"""

    def __init__(self, bucket_name, key):
        """Initialize MediaTagging class and setting up buckets

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object

        """
        self.client = default_client().get_client_s3()
        self.bucket_name = bucket_name
        self.key = key
        self.key_tags_using_base64 = ["description", "brand", "category"]

    def _iter_aggregated_tag(self, tag_key, tag_value):
        """Iterate tags that has been combined

        Args:
            tag_key (str): key of the tag
            tag_value (str): value of the tag

        Returns:
            tuple: key and tag

        """
        for key_value in zip(
            tag_key.split(constants.MEDIA_TAG_KEY_SEPARATOR),
            tag_value.split(constants.MEDIA_TAG_VALUE_SEPARATOR),
        ):
            yield key_value[0], key_value[1]

    def _update_aggregated(
        self,
        sub_tags_old_aggregated_values: Optional[str],
        sub_tags_new_values: tuple,
        are_old_values_b64_encoded: bool,
    ) -> tuple:
        """Update values of aggregated sub-tags.

        >>> self._update_aggregated('uncoolBrand-----coolCategory', ('coolBrand', None))
        ('coolBrand', 'coolCategory')

        >>> self._update_aggregated(None, ('coolBrand', 'coolCategory'))
        ('coolBrand', 'coolCategory')

        Args:
            sub_tags_old_aggregated_values: Old aggregated values of sub-tags.
            sub_tags_new_values: New values of sub-tags.
            are_old_values_b64_encoded: Whether or not old values of sub-tags
                are b64 encoded.

        Returns:
            A tuple of values corresponding to sub-tags.
        """
        sub_tags_values = []
        sub_tags_old_values = sub_tags_old_aggregated_values and sub_tags_old_aggregated_values.split(
            constants.MEDIA_TAG_VALUE_SEPARATOR
        )
        for index, new_sub_tag_value in enumerate(sub_tags_new_values):
            if new_sub_tag_value is None:
                old_sub_tag_value = sub_tags_old_values and sub_tags_old_values[index] or ""

                if are_old_values_b64_encoded and old_sub_tag_value != "":
                    old_sub_tag_value = helpers.base64_decode(old_sub_tag_value)

                sub_tags_values.append(old_sub_tag_value)
            else:
                sub_tags_values.append(new_sub_tag_value)

        return tuple(sub_tags_values)

    def _are_sub_tags_b64_encoded(self, key):
        """Only certain sub-tags were chosen to be b64 encoded.

        >>> self._are_sub_tags_b64_encoded('brand_category')
        True

        >>> self._are_sub_tags_b64_encoded('resolutionInPx_resolutionKey')
        False
        """
        return any(tag in key for tag in constants.KEY_TAGS_USING_BASE64)

    def update(self, **tags):
        """Update tags of an object.

        Args:
            tags (kwargs): New tags of an object.
        """
        existing_tags = self.get(separate_aggregated=False)
        logger.debug(f"Updating existing tags: {existing_tags} with following tags: {tags}.")
        for new_key, new_value in tags.items():
            if constants.MEDIA_TAG_KEY_SEPARATOR in new_key:
                new_value = self._update_aggregated(
                    existing_tags.get(new_key),
                    new_value,
                    self._are_sub_tags_b64_encoded(new_key),
                )
            existing_tags[new_key] = new_value
        self.put(**existing_tags)

    def delete(self):
        """Delete tags of an object"""
        response = self.client.delete_object_tagging(Bucket=self.bucket_name, Key=self.key)
        logger.debug(f'client.delete_object_tagging->HTTPStatusCode: {response["ResponseMetadata"]["HTTPStatusCode"]}')

    def get(self, separate_aggregated=True) -> dict:
        """Retrieves all tags of an object.

        Args:
            separate_aggregated (bool): if the aggregated tags needs to be separated to a separate key pair

        Returns:
            A dictionary containing the tags.
        """
        response = self.client.get_object_tagging(Bucket=self.bucket_name, Key=self.key)
        tags = {}
        for tag_dict in response["TagSet"]:
            if constants.MEDIA_TAG_KEY_SEPARATOR in tag_dict["Key"] and separate_aggregated:
                for key, value in self._iter_aggregated_tag(tag_dict["Key"], tag_dict["Value"]):
                    tags[key] = value
            else:
                tags[tag_dict["Key"]] = tag_dict["Value"]
        for key in tags:
            if key in self.key_tags_using_base64:
                tags[key] = helpers.base64_decode(tags[key])
        return tags

    def put(self, **tags):
        """Update tags of an object

        Args:
            tags (kwargs): new tags of an object

        """
        logger.debug(f"Creating s3 tags: {tags}")
        tag_list = self.create_s3_tags(**tags)

        logger.debug(f"Tag list: {tag_list}")
        response = self.client.put_object_tagging(Bucket=self.bucket_name, Key=self.key, Tagging={"TagSet": tag_list})
        logger.debug(f'client.put_object_tagging->HTTPStatusCode: {response["ResponseMetadata"]["HTTPStatusCode"]}')

    def create_s3_tags(self, **tags):
        """Generate final form of s3 Tags

        Args:
            tags (kwargs): arguments

        Returns:
            string: extension
        """
        tag_list = []
        for key, value in tags.items():
            if isinstance(value, tuple) and key.count(constants.MEDIA_TAG_KEY_SEPARATOR) == len(value) - 1:
                keys = key.split(constants.MEDIA_TAG_KEY_SEPARATOR)
                joined_value = []
                for sub_key, sub_value in zip(keys, value):
                    if not sub_value:
                        sub_value = ""
                    if sub_key in constants.KEY_TAGS_USING_BASE64:
                        sub_value = helpers.base64_encode(sub_value)
                    joined_value.append(sub_value)
                value = constants.MEDIA_TAG_VALUE_SEPARATOR.join(joined_value)
            elif key in constants.KEY_TAGS_USING_BASE64:
                value = helpers.base64_encode(value)
            else:
                value = str(value)
            tag_list.append(dict(Key=key, Value=value))

        return tag_list


class ObjectMetadata:
    """Base class that all S3 access point implementations derive from"""

    def __init__(self, bucket_name, key):
        """Initialize ObjectMetadata class and setting up buckets

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object

        """
        self.client = default_client().get_client_s3()
        self.bucket_name = bucket_name
        self.key = key

    def get_metadata(self):
        """Return metadata of an object

        Returns:
            dict: metadata of an object
        """
        try:
            response = self.client.head_object(Bucket=self.bucket_name, Key=self.key)

            logger.debug(f'client.head_object->HTTPStatusCode: {response["ResponseMetadata"]["HTTPStatusCode"]}')
        except botocore.exceptions.ClientError as exc:
            logger.exception(f"S3 object not found. {exc}")
            response = dict()

        return response

    def get_content_type(self):
        """Return content type of an object

        Returns:
            string: content type of the object

        """
        return self.get_metadata()["ContentType"]
