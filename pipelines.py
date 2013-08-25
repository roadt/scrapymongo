

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

from scrapy.item import Item
from pymongo import MongoClient


class MongoItemMixin(object):
	pass


class MongoItem(Item, MongoItemMixin):
	pass

MONGO_PIPELINE_HOST  = 	'MONGO_PIPELINE_HOST'
MONGO_PIPELINE_DBNAME = 'MONGO_PIPELINE_DBNAME'
MONGO_PIPELINE_COLNAME_BOTPREFIX = 'MONGO_PIPELINE_COLNAME_BOTPREFIX'
MONGO_PIPELINE_DBNAME_BOTSUFFIX = 'MONGO_PIPELINE_DBNAME_BOTSUFFIX'

default_settings = {
	MONGO_PIPELINE_HOST:'localhost',
	MONGO_PIPELINE_DBNAME_BOTSUFFIX: False,
	MONGO_PIPELINE_COLNAME_BOTPREFIX : False
}

class MongoPipeline(object):
	def __init__(self, settings):
		self.settings = settings
		self.settings.defaults.update(default_settings)

		# settings from scrapy
		self.host = self.settings.get(MONGO_PIPELINE_HOST)
		self.bot_name = self.settings.get("BOT_NAME")

		self.col_prefix = self.settings.getbool(MONGO_PIPELINE_COLNAME_BOTPREFIX)
		self.db_suffix = self.settings.getbool(MONGO_PIPELINE_DBNAME_BOTSUFFIX)

		self.db_name = self.database_name()
		print self.db_name
		self.client = MongoClient(self.host)
		self.db = self.client[self.db_name]


	@classmethod
	def from_settings(cls, settings):
		return cls(settings)

	def process_item(self, item, spider):
		col = self.db[self.collection_name(item)]
		key = item['key']
		hit = col.find({ 'key' : key }).count() > 0
		if not hit:
			col.insert(dict(item))
		else:
			col.update({'key':key}, dict(item))
		return item

	def database_name(self):
		dbname  =  self.settings.get(MONGO_PIPELINE_DBNAME)
		if dbname is None:  # not set fixed db name use bot name.
			dbname = self.bot_name
		elif self.db_suffix:
			dbname = dbname +'.' + self.bot_name
		return dbname

	def collection_name(self, item):
		colname =  item.__class__.__name__.lower()
		if self.col_prefix:
			colname = self.bot_name + "." + colname
		return colname