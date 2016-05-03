
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


MONGO_PIPELINE_HOST  = 	'MONGO_PIPELINE_HOST'
MONGO_PIPELINE_DBNAME = 'MONGO_PIPELINE_DBNAME'
MONGO_PIPELINE_COLNAME_BOTPREFIX = 'MONGO_PIPELINE_COLNAME_BOTPREFIX'
MONGO_PIPELINE_DBNAME_BOTSUFFIX = 'MONGO_PIPELINE_DBNAME_BOTSUFFIX'
MONGO_PIPELINE_KEYS  = 'MONGO_PIPELINE_KEYS'

default_settings = {
    MONGO_PIPELINE_HOST:'localhost',
    MONGO_PIPELINE_DBNAME_BOTSUFFIX: False,
    MONGO_PIPELINE_COLNAME_BOTPREFIX : False,
    MONGO_PIPELINE_KEYS : ['key', 'url']
}



logger = logging.getLogger(__name__)

class MongoPipeline(object):
    def __init__(self, settings):
        self.settings = settings

        # settings from scrapy
        self.bot_name = self.settings.get("BOT_NAME")
        self.host = self.settings.get(MONGO_PIPELINE_HOST, default_settings[MONGO_PIPELINE_HOST])
        self.keys = self.settings.get(MONGO_PIPELINE_KEYS, default_settings[MONGO_PIPELINE_KEYS])

        self.col_prefix = self.settings.getbool(MONGO_PIPELINE_COLNAME_BOTPREFIX, default_settings[MONGO_PIPELINE_COLNAME_BOTPREFIX])
        self.db_suffix = self.settings.getbool(MONGO_PIPELINE_DBNAME_BOTSUFFIX, default_settings[MONGO_PIPELINE_DBNAME_BOTSUFFIX])
        self.db_name = self.database_name()
        self.client = MongoClient(self.host)
        self.db = self.client[self.db_name]


    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    def process_item(self, item, spider):
        self.calculate_fk(item)
        self.process_obj(item)
        return item

    def process_obj(self, item):
        col = self.db[self.collection_name(item)]
        pending_keys = self.keys or  ['key', 'url'] 
        key = ''
        for name in pending_keys:
            if name in item.keys():
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

        
    def database_name(self):
        dbname  =  self.settings.get(MONGO_PIPELINE_DBNAME)
        if dbname is None:  # not set fixed db name use bot name.
            dbname = self.bot_name
        elif self.db_suffix:
            dbname = dbname +'.' + self.bot_name
        return dbname

    def collection_name(self, item):
        if not isinstance(item, str):
            item = item.__class__.__name__
        colname =  item.lower()
        if self.col_prefix:
            colname = self.bot_name + "." + colname
        return colname
