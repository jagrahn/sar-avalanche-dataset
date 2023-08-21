import dataclasses
import shapely.geometry
import shapely.wkt
from dateutil import parser
import datetime as dt
from pymongo import MongoClient
from bson.objectid import ObjectId
import json
import hashlib


_DUMMY_STORE = {}


LABEL_DESCRIPTION = {
    0: 'Avalanche(s) absent',
    1: 'Avalanche(s) present',
    2: 'Unsure',
    3: 'Defected' 
}


def _as_datetime(time):
    return time if isinstance(time, dt.datetime) else parser.parse(time)


@dataclasses.dataclass(frozen=True)
class Item:
    uuid: str
    geometry: str
    t_0: dt.datetime
    t_1: dt.datetime
    label: int = None
    comment: str = None
    type: str = None
    certainty: int = None
    source: str = None
    json: str = None
    _id: ObjectId = None

    def __post_init__(self):
        # Type casting:
        if isinstance(self.geometry, shapely.geometry.base.BaseGeometry):
            super().__setattr__('geometry', shapely.wkt.dumps(self.geometry))

        # Normalize time stamps unix time:
        super().__setattr__('t_0', _as_datetime(self.t_0))
        super().__setattr__('t_1', _as_datetime(self.t_1))

        # Dump json to string:
        if self.json is not None and not isinstance(self.json, str):
            super().__setattr__('json', json.dumps(self.json))
    
    def asdict(self): 
        return dataclasses.asdict(self)
    
    def __hash__(self):
        return hash((self.uuid, self.geometry, self.t_0, self.t_1, self.label, self.comment,
                     self.type, self.certainty, self.source, self.json, self._id))


@dataclasses.dataclass(frozen=False)
class Database:
    host: str = 'localhost'
    port: int = 27017
    database: str = 'skreddata'
    collection: str = 'avl-v20230607'

    def __post_init__(self):
        client = MongoClient(self.host, self.port)
        self.db = client[self.database]
        self.collection = self.db[self.collection]
        
    def find(self, *args, **kwargs): 
        items = []
        cursor = self.collection.find(*args, **kwargs)
        for item in cursor:
            items.append(Item(**item))
        return items
    
    def find_one(self, *args, **kwargs): 
        item = self.collection.find_one(*args, **kwargs)
        if item is not None: 
            item = Item(**item)
        return item

    def insert(self, item):
        assert isinstance(item, (dict, Item))
        item = Item(**item).asdict() if isinstance(item, dict) else item.asdict()
        item.pop('_id')
        self.collection.insert_one(item)

    def replace(self, item):
        assert isinstance(item, (dict, Item))
        item = Item(**item).asdict() if isinstance(item, dict) else item.asdict()
        item.pop('_id')
        self.collection.replace_one({'uuid': item['uuid']}, item)

    def remove_by_uuid(self, uuid):
        self.collection.delete_one({'uuid': uuid})

    def get_by_uuid(self, uuid):
        item = self.collection.find_one({'uuid': uuid})
        return None if item is None else Item(**item)

    def get_by_label(self, label):
        return self.find({'label': label})

    def get_by_time(self, time):
        time = _as_datetime(time)
        return self.find({'$and': [{'t_0': {'$lte': time}}, {'t_1': {'$gte': time}}]})

    def get_by_uuid_contains(self, part):
        return self.find({'uuid': {'$regex': part}})

    def get_by_comment_contains(self, part):
        return self.find({'comment': {'$regex': part}})

    def get_all(self):
        return self.find()

    def get_all_unlabeled(self):
        return self.find({'$or': [{'label': {'$exists': False}}, {'label': None}]})

    def get_all_labeled(self):
        return self.find({'label': {'$exists': True, '$ne': None}})

    def get_length(self):
        return self.collection.count_documents({})

    def get_length_unlabeled(self):
        return self.collection.count_documents({'label': {'$exists': False}})

    def get_length_labeled(self):
        return self.collection.count_documents({'label': {'$exists': True}})

    def get_length_with_label(self, label):
        return self.collection.count_documents({'label': label})


@dataclasses.dataclass(frozen=False)
class DummyDatabase:
    
    collection = _DUMMY_STORE
    
    def _hash_from_item(itm):
        return hashlib.sha256(json.dumps(itm).encode("utf-8")).hexdigest()

    def insert(self, item):
        assert isinstance(item, (dict, Item))
        item = Item(**item).asdict() if isinstance(item, dict) else item.asdict()
        self.collection[item['uuid']] = item

    def get_by_uuid(self, uuid):
        if uuid in self.collection: 
            return self.collection[uuid]

    def get_all(self):
        return list(self.collection.values())

    def get_length(self):
        return len(self.collection)
    
    