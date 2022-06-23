from dataclasses import dataclass, field, asdict

import datetime
import os
from typing import Dict, List, Union
import json

@dataclass
class DataPacket:

    service_type: str = os.environ['SERVICE_TYPE']
    service_name: str = os.environ['SERVICE_NAME']
    topic: str = ''
    timestamp: Union[datetime.datetime, List[datetime.datetime]] = field(default_factory=datetime.datetime.now)
    body: Dict = field(default_factory=dict)

    @staticmethod
    def from_json(digest):
        ddig = json.loads(digest)
        return DataPacket(
            service_type=ddig['service_type'],
            service_name=ddig['service_name'],
            topic=ddig['topic'],
            timestamp=ddig['timestamp'],
            body=ddig['body']
        )
    
    @staticmethod
    def from_file(path):
        with open(path, 'r') as f:
            return json.load(f)
        
    def dumps(self):
        return json.dumps(asdict(self), default=str)
    
    def to_file(self, path):
        with open(path, 'w') as f:
            json.dump(asdict(self), f, default=str)
