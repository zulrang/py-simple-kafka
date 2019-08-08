import copy
import uuid
import datetime
import atexit
import json
import socket

from kafka import KafkaProducer, KafkaConsumer

_PRODUCER = False
_CONSUMER = False


def get_consumer(topic):
    global _CONSUMER
    if not _CONSUMER:
        _CONSUMER = KafkaConsumer(topic, value_deserializer=Message.from_bin)
    return _CONSUMER


def get_producer():
    global _PRODUCER
    if not _PRODUCER:
        _PRODUCER = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return _PRODUCER


def flush():
    global _PRODUCER
    if _PRODUCER:
        _PRODUCER.flush()


class Consumer():

    def __init__(self, topic):
        self._topic = topic
        self._consumer = get_consumer(topic)

    def listen(self):
        for msg in self._consumer:
            yield msg.value


class Message():

    def __init__(self, topic, name='producer', version='0.1', data=None):
        self._producer = get_producer()
        self._topic = topic
        self._version = "1.0"
        self._headers = {
            "producer": {
                "node": socket.gethostname(),
                "name": name,
                "version": version
            },
            "id": uuid.uuid4().hex,
            "time": datetime.datetime.utcnow().isoformat(),
            "topic": topic
        }
        self._events = {}
        self._data = data if data else {}

    def to_json(self):
        return {
            "version": self._version,
            "headers": copy.deepcopy(self._headers),
            "events": copy.deepcopy(self._events),
            "data": copy.deepcopy(self._data)
        }

    @classmethod
    def from_json(cls, dct):
        obj = cls(dct['headers']['topic'])
        for k, v in dct.get('headers').items():
            obj.add_header(k, copy.deepcopy(v))
        for k, v in dct.get('events').items():
            obj.add_event(k, copy.deepcopy(v))
        for k, v in dct.get('data').items():
            obj.add_data(k, copy.deepcopy(v))
        return obj

    @classmethod
    def from_bin(cls, byts):
        return cls.from_json(json.loads(byts))

    def add_header(self, key, value):
        self._headers[key] = value

    def add_data(self, key, value):
        self._data[key] = value

    def add_event(self, key, value):
        self._events[key] = value

    def send(self):
        self._producer.send(self._topic, self.to_json())

    @classmethod
    def flush(cls):
        get_producer().flush()

    def __repr__(self):
        return "Message <{}>".format(self.to_json())

    def __str__(self):
        return repr(self)
