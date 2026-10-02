import datetime as dt
from typing import Optional

import bson
import pymongo.asynchronous.client_session as pmses
import pymongo.asynchronous.collection as pmcol

from .schema import DocumentType


def get_collection(
    session: pmses.AsyncClientSession, dbname: Optional[str], collection: str
) -> pmcol.AsyncCollection[DocumentType]:
    codec_options = bson.CodecOptions[DocumentType](tz_aware=True, tzinfo=dt.timezone.utc)
    if dbname:
        database = session.client.get_database(dbname, codec_options=codec_options)
    else:
        database = session.client.get_default_database(codec_options=codec_options)

    return database.get_collection(collection)
