
#
#  Scrapy's mognodb 's piple
#
#  Here is the plan,
#  1, each item type  is mapped to a collection. collection name come from item's class.
#  2, each item is a record of the collection above. with new generated id. and a 'key' property, whose value is unique for every scrapped item. the item class should provide that.(or the record will be conflict)
#
#  Mongodb layout. here are two layout.
#  1, only one fixed database (user specified),	 each collection is named as  'project_name +  item type'
#  2, database name is mapped to project name directly. and the collection name is mapped to item type.
#
#

import datetime
from collections import Mapping
import logging


from scrapy.item import Item
from pymongo import MongoClient
from . import Config


logger = logging.getLogger(__name__)
        
class MongoPipeline(object):
    def __init__(self, settings):
        self.settings = settings
        self.config = Config(self.settings)
        self.client = MongoClient(self.config.host)
        self.db = self.client[self.config.database_name()]
        logger.debug("%s:%s %s" % (type(self).__name__,  '__init__',  [self.config, self.client, self.db]))

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    def process_item(self, item, spider):
        self.calculate_fk(item)
        self.process_obj(item)
        return item

    def process_obj(self, item):
        col = self.db[self.collection_name(item)]
        pending_keys = self.config.keys or  ['key', 'url'] 
        key = ''
        for name in pending_keys:
            if name in list(item.keys()):
                key = name
                break

        logger.debug("%s:%s %s" % (type(self).__name__,  'process_obj',  [pending_keys, key, item]))
        value = item[key]
        hit = col.find({ key : value }).count() > 0
        if not hit:
            col.insert(dict(item))
        else:
            col.update({key:value}, {'$set': dict(item)})

    def calculate_fk(self, item):
        '''
        check fk field,  match the fk value to foregin key and set   xx_id  in item 
        '''
        for name in item.fields:
            field = item.fields[name]
            if 'fk' in field and name in item: # if field is fk and item has value set
                typename, keyname = name.split('_', 1)
                idf = typename+'_id'
                if isinstance(field['fk'], Mapping):
                    typename = field['fk'].get('type') or typename
                    keyname = field['fk'].get('key') or keyname
                    idf = field['fk'].get('idf') or typename+'_id'
                col = self.db[self.collection_name(typename)] 
                hit = col.find_one({ keyname: item[name]})
                logger.debug("%s:%s %s" % (type(self).__name__,  'process_fk',  [name, field, typename, keyname, hit, idf]))

                if hit:
                    item[idf] = hit['_id']

        

    def collection_name(self, item):
        if not isinstance(item, str):
            item = item.__class__.__name__
        typename =  item.lower()
        return self.config.collection_name(typename)
