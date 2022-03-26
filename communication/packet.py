from ctypes import FormatError
import json
import time

class DataPacket:

    def __init__(self, name=None, value=None, data_type=None, timestamp=None):
        self.name = name
        self.value = value
        self.timestamp = time.time() if timestamp is None else timestamp
        self.data_type = data_type

    @staticmethod
    def from_string(self, bytes):
        data = json.loads(bytes)
        if self.__dict__.keys() != data.keys():
            raise FormatError('Wrong data format on packet')
        self.name = data['name']
        self.value = data['value']
        self.timestamp = data['timestamp']
        self.data_type = data['data_type']

    def __str__(self):
        return str(self.__dict__)

    def dumps(self):        
        return json.dumps(self.__dict__)
