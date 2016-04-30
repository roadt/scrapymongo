scrapymongo
===========

A simple mongo pipeline for scrapy framework.


# Design

  * each item type  is mapped to a collection. collection name come from class of Item.
  * each item is a record of the collection above. with new generated id. and a 'key' property, whose value is unique for every scrapped item. the item class have to ensure the value of key exists.   
  * foregin key support. in Item defind fk field, pipeline will calculate the fk_id ('type'+'_id')
	                        Field({ 'fk' : True }) 
							Field({'fk': {'type':'class_name','key':'key_in_type_in_the_left'}})

# Settings options

	MONGO_PIPELINE_HOST  - mongo host, defualt is 'localhost'
    MONGO_PIPELINE_KEYS  -  'key' property of item. checked in order. default is  ['key', 'url']. i.e. if 'key' doesn't exist , check 'url'
	
	MONGO_PIPELINE_DBNAME	-   mongo db name, default is None. if None, pipeline will use scrapy project name as db name
	MONGO_PIPELINE_DBNAME_BOTSUFFIX - if set True, use DBNAME.BOTNAME as mongo database name. only take effect when  if DBNAME is exlicitly set. 
    MONGO_PIPELINE_COLNAME_BOTPREFIX  -  if set True, use BOTNAME.ITEM-CLASS-NAME as mongo collection name.x



License
=========
MIT
