from typing import MutableMapping, Union

import msgspec

MetadataValue = Union[int, float, str, bool, None]
Metadata = MutableMapping[str, MetadataValue]

metadata_encoder = msgspec.json.Encoder()
metadata_decoder = msgspec.json.Decoder(type=Metadata)
