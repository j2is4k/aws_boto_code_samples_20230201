"""This module contains the operation handlers for Amazon S3 access through Boto3

"""
import copy
import datetime

import botocore.exceptions

from .aws import default_client, AWS
from . import constants
from . import logging
from . import response
from . import s3_properties
from . import helpers


logger = logging.getLogger(__name__)


class AccessPoint:
    """Wrapper for S3 client."""

    def __init__(self):
        """Initialize AccessPoint base class"""
        aws_client = default_client()
        self.client = aws_client.get_client_s3()
        self.resource = aws_client.get_resource_s3()

        pre_signing_aws_client = AWS(
            access_key_id=constants.IAM_S3_VALIDITY_AWS_ACCESS_KEY,
            access_key_secret=constants.IAM_S3_VALIDITY_AWS_SECRET_KEY,
            role_arn=None,
            profile=None,
        )
        self.signing_client = pre_signing_aws_client.get_client_s3()

    @response.generate
    def delete_object(self, bucket_name, key):
        """Delete an object from S3 bucket

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object to be deleted

        Returns:
            dict: Transaction result
        """
        logger.info(f"Deleting object: {bucket_name} with key: {key}")
        try:
            obj = self.resource.Object(bucket_name, key)
            result = dict(obj.delete())
            return result
        except botocore.exceptions.ClientError as exc:
            msg = f"Can get object {key}: {exc}"
            logger.exception(msg)
            return response.StatusCode().set_bad_request(msg)

    @response.generate
    def get_object(self, bucket_name, key):
        """Retrieve an object from S3 bucket

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object to be retrieved

        Returns:
            dict: s3 object
        """
        try:
            obj = self.resource.Object(bucket_name, key)
            obj.load()
            return dict(object=obj)
        except botocore.exceptions.ClientError as exc:
            msg = f"Can get object {key}: {exc}"
            logger.exception(msg)
            return response.StatusCode().set_bad_request(msg)

    @response.generate
    def download_fo(self, bucket_name, key, binary_fo, **kwargs):
        """Retrieve an object from S3 bucket

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object to be downloaded
            binary_fo (object): file object
            kwargs (kwargs): additional parameters

        Returns:
            response.ElementResponse: Transaction result

        """
        try:
            self.client.download_fileobj(bucket_name, key, binary_fo, **kwargs)
            return response.StatusCode().set_ok()
        except botocore.exceptions.ClientError as exc:
            msg = f"Can not download {key}: {exc}"
            logger.exception(msg)
            return response.StatusCode().set_bad_request(msg)

    @response.generate
    def get_url_accessor(self, bucket_name, key):
        """Retrieve an objects URL from S3 bucket

        .. note:
            We use an extra user with long-term credentials to pre-sign URLs with longer expiration.
            https://aws.amazon.com/de/premiumsupport/knowledge-center/presigned-url-s3-bucket-expiration/

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object to be accessed

        Returns:
            dict: Transaction result and pre-signed URL
        """
        try:
            self.resource.Object(bucket_name, key).load()
        except botocore.exceptions.ClientError as exc:
            logger.exception(f"S3 object `{bucket_name}#{key}` not found. {exc}")
            return response.StatusCode().set_ok(f"S3 object not found. {exc}")

        url = self.signing_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": key},
            ExpiresIn=int(constants.URL_TTL_IN_DAYS) * 24 * 3600,
        )
        valid_from = helpers.get_localized_datetime_now()
        valid_until = valid_from + datetime.timedelta(int(constants.URL_TTL_IN_DAYS))
        result = dict(
            sourceUrl=url,
            sourceUrlValidUntil=valid_until.isoformat(timespec="milliseconds"),
            sourceUrlValidFrom=valid_from.isoformat(timespec="milliseconds"),
        )
        return result

    @response.generate
    def list_objects(self, bucket_name, prefix="", **kwargs):
        """Retrieve list of objects based on prefix

        Args:
            bucket_name (str): name of the bucket where the object resides
            prefix (str): prefix of the objects to be accessed

        Returns:
            dict: List of objects
        """
        logger.info(f"Listing under bucket_name: {bucket_name} with prefix: {prefix}")

        return_dict = self.client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=int(constants.PAGINATION_MAX_KEYS),
            **kwargs,
        )
        return dict(listed_objects=return_dict)

    @response.generate
    def store_object(self, bucket_name, key, payload, content_type="", tags=None):
        """Retrieve list of objects based on prefix

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object to be stored
            content_type (str): content type of the object
            tags (dict): tags of an object

        Returns:
            dict: transaction result
        """
        tags_response = {}
        if tags is None:
            tags = {}
        logger.info(f"Storing to bucket: {bucket_name} with key: {key}; payload len={len(payload)}")
        try:
            self.resource.Object(bucket_name, key).put(Body=payload, ContentType=content_type)
            if tags:
                s3_properties.MediaTagging(bucket_name, key).put(**tags)
                tags_response = dict(**s3_properties.MediaTagging(bucket_name, key).get())
        except botocore.exceptions.ClientError as exc:
            logger.exception(exc)
            return response.StatusCode().set_bad_request(message=exc)
        return (
            tags_response,
            response.StatusCode().set_created(),
        )

    @response.generate
    def update_tagging(self, bucket_name, key, tags):
        """Update tags of an object

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object to be stored
            tags (dict): tags of an object

        Returns:
            dict: transaction result
        """
        logger.info(f"Updating tags to bucket: {bucket_name} with key: {key}; tags={tags}")
        try:
            s3_properties.MediaTagging(bucket_name, key).update(**tags)
        except botocore.exceptions.ClientError as exc:
            logger.exception(exc)
            return response.StatusCode().set_bad_request(message=exc)
        return dict(**s3_properties.MediaTagging(bucket_name, key).get())

    @response.generate
    def get_object_tags(self, bucket_name: str, key_name: str):
        """Retrieves tags of an S3 object.

        Args:
            bucket_name: Name of the S3 bucket.
            key_name: S3 object key name.
        """
        logger.info(f"Retrieving tags for S3 object {bucket_name}:{key_name}.")
        try:
            return dict(**s3_properties.MediaTagging(bucket_name, key_name).get(separate_aggregated=True))
        except botocore.exceptions.ClientError as exc:
            logger.exception(exc)
            return response.StatusCode().set_bad_request(message=exc)

    @response.generate
    def create_pre_signed_url(self, bucket_name, key, content_type="", tags=None, expiration=900):
        """Generate a pre-signed URL S3 POST request to upload a file

        Args:
            bucket_name (str): name of the bucket where the object resides
            key (str): key of the object to be stored
            content_type (str): content type of the object
            tags (dict): tags to be added to the object
            expiration (int): Time in seconds for the pre-signed URL to remain valid

        Returns:
            dict: transaction result
        """

        try:
            xml_tags = [f'<Tag><Key>{item["Key"]}</Key><Value>{item["Value"]}</Value></Tag>' for item in tags]
            fields = {
                "tagging": f"<Tagging><TagSet>{ ''.join(xml_tags) }</TagSet></Tagging>",
                "Content-Type": content_type,
            }
            conditions = [dict([field]) for field in fields.items()]
            sign_url_response = self.client.generate_presigned_post(
                bucket_name,
                key,
                Fields=copy.deepcopy(fields),
                Conditions=copy.deepcopy(conditions),
                ExpiresIn=expiration,
            )
        except botocore.exceptions.ClientError as exc:
            logger.exception(exc)
            return response.StatusCode().set_bad_request(message=exc)
        return sign_url_response
