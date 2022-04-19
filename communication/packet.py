from dataclasses import dataclass, field
from dataclasses_json import dataclass_json

import datetime
import os
from typing import Dict, List, Union

@dataclass_json
@dataclass
class DataPacket:

    service_type: str = os.environ['SERVICE_TYPE']
    service_name: str = os.environ['SERVICE_NAME']
    topic: str = ''
    timestamp: Union[datetime.datetime, List[datetime.datetime]] = field(default_factory=datetime.datetime.now())
    body: Dict = field(default_factory={})
