import contextlib
from pymongo import MongoClient

class Config(object):
    ''' config logic here '''
    def __init__(self, settings):
        self.settings = settings
        # settings from scrapy
        self.bot_name = self.settings.get("BOT_NAME")
        self.host = self.settings.get('MONGO_PIPELINE_HOST', 'localhost')
        self.keys = self.settings.get('MONGO_PIPELINE_KEYS', ['key', 'url'])
        self.col_prefix = self.settings.getbool('MONGO_PIPELINE_COLNAME_BOTPREFIX', False)
        self.db_suffix = self.settings.getbool('MONGO_PIPELINE_DBNAME_BOTSUFFIX', False)

    def database_name(self):
        dbname  =  self.settings.get('MONGO_PIPELINE_DBNAME')
        if dbname is None:  # not set fixed db name use bot name.
            dbname = self.bot_name
        elif self.db_suffix:
            dbname = dbname +'.' + self.bot_name
        return dbname

    def collection_name(self, typename):
        if self.col_prefix:
            typename = self.bot_name + "." + typename
        return typename

    def __repr__(self):
        return "\n".join(map(lambda n :"%s:%s"%(n, getattr(self,n)),  ["bot_name", 'host', 'keys', 'col_prefix', 'db_suffix']))


@contextlib.contextmanager
def mongodb(settings):
    config = Config(settings)
    clt  = MongoClient(config.host)
    db = clt[config.database_name()]
    try:
        yield db
    finally:
        clt.close()


