from datetime import datetime
from io import BytesIO

import base64
import boto3
import botocore
import hashlib
import json
import logging
import os
import tempfile
import uuid


class s3DB(object):
    """
    A s3db connection object.
    """

    ##
    # Config
    ##

    bucket = None
    backend = "s3"
    region = "eu-west-1"
    serializer = "json"
    index = "id"
    signature_version = "s3v4"
    cache = False
    encoding = 'utf8'
    profile_name = None

    s3 = boto3.resource('s3', config=botocore.client.Config(signature_version=signature_version))

    def __init__(self, prefix="s3db", profile_name=None, session=None):
        self.prefix = "." + prefix + "/"
        if profile_name:
            self.profile_name = profile_name
        if self.profile_name:
            session = boto3.session.Session(profile_name=self.profile_name)
        if session:
            self.s3 = session.resource('s3', config=botocore.client.Config(signature_version=self.signature_version))

    ##
    # Advanced config
    ##

    #cache_dir = tempfile.gettempdir()
    human_readable_indexes = False
    hash_function = hashlib.sha256

    ##
    # Public Interfaces
    ##

    def save(self, obj, index=None):
        """
        Save an object to the backend datastore.

        Will use this s3db's index by default if an explicit index isn't supplied.
        """

        # First, serialize.
        serialized = self._serialize(obj)

        # Next, compute the index
        if not index:
            real_index = self._get_object_index(obj, self.index)
        else:
            real_index = self._format_index_value(index)

        # Then, store.
        bytesIO = BytesIO()
        bytesIO.write(serialized.encode(self.encoding))
        bytesIO.seek(0)

        s3_object = self.s3.Object(self.bucket, self.prefix + real_index)
        result = s3_object.put('rb', Body=bytesIO)
        logging.debug("Put remote bytes: " + self.prefix + real_index)

        if result['ResponseMetadata']['HTTPStatusCode'] == 200:
            resp = True
        else:
            resp = False


        return resp

    def load(self, index, metainfo=False, default=None):
        """
        Load an object from the backend datastore.

        Returns None if not found.
        """

        # First, calculate the real index
        real_index = self._format_index_value(index)

        # If cache enabled, check local filestore for bytes

        # Next, get the bytes (if any)
        try:
            serialized_s3 = self.s3.Object(self.bucket, self.prefix + real_index)
            serialized = serialized_s3.get()["Body"].read()
        except botocore.exceptions.ClientError as e:
            # No Key? Return default.
            logging.debug("No remote object, returning default.")
            return default

        # Then read the data format
        deserialized = self._deserialize(serialized)

        # And return the data
        if metainfo:
            return deserialized['obj'], (
                                            deserialized['dt'],
                                            deserialized['uuid']
                                        )
        else:
            return deserialized['obj']

    def delete(self, index):
        """
        Given an index, delete this object.
        """

        # First, calculate the real index
        real_index = self._format_index_value(index)

        # Next, get the bytes (if any)
        serialized_s3 = self.s3.Object(self.bucket, self.prefix + real_index)
        result = serialized_s3.delete()

        if result['ResponseMetadata']['HTTPStatusCode'] in [200, 204]:
            return True
        else:
            return False


    ###
    # Private interfaces
    ###

    def _serialize(self, obj):
        """
        Create a s3db storage item. They exist in the format:

        /my_bucket/_s3db/[[index]]
        {
            "serializer:" [[serializer_format]],
            "dt": [[datetime created]],
            "uuid": [[uuid4]],
            "obj": [[object being saved]]
        }

        """

        packed = {}
        packed['serializer'] = self.serializer
        packed['dt'] = str(datetime.utcnow())
        packed['uuid'] = str(uuid.uuid4())

        if self.serializer == 'pickle':
            packed['obj'] = base64.b64encode(pickle.dumps(obj)).decode(self.encoding)
        elif self.serializer == 'json':
            packed['obj'] = obj
        else:
            raise Exception("Unsupported serialize format: " + str(self.serializer))

        return json.dumps(packed)

    def _deserialize(self, serialized):
        """
        Unpack and load data from a serialized s3db entry.
        """

        obj = None
        deserialized = json.loads(serialized)
        return_me = {}

        if deserialized['serializer'] == 'pickle':

            if self.serializer != 'pickle':
                raise Exception("Security exception: Won't unpickle if not set to pickle.")

            return_me['obj'] = pickle.loads(base64.b64decode(deserialized['obj'].encode(self.encoding)))

        elif deserialized['serializer'] == 'json':
            return_me['obj'] = deserialized['obj']

        else:
            raise Exception("Unsupported serialize format: " + deserialized['serializer'])

        return_me['dt'] = deserialized['dt']
        return_me['uuid'] = deserialized['uuid']

        return return_me

    def _get_object_index(self, obj, index):
        """
        Get the "Index" value for this object. This may be a hashed index.

        If it's a dictionary, get the key.
        If it has that as an attribute, get that attribute as a string.
        If it doesn't have an attribute, or has an illegal attribute, fail.
        """

        index_value = None
        if type(obj) is dict:
            if index in obj:
                index_value = obj[index]
            else:
                raise Exception("Dict object has no key: " + str(index))
        else:
            if hasattr(obj, index):
                index_value = getattr(obj, index)
            else:
                raise Exception("Dict object has no attribute: " + str(index))

        return self._format_index_value(index_value)

    def _format_index_value(self, index_value):
        """
        Hash these bytes, or don't.
        """

        logging.debug("Formatting index value: " + str(index_value))

        if self.human_readable_indexes:
            # You are on your own here! This may not work!
            return index_value
        else:
            return self.hash_function(index_value.encode(self.encoding)).hexdigest()
