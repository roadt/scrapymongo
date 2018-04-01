import bson
import logging
from time import time
from pymongo import MongoClient

from scrapy.responsetypes import responsetypes
from scrapy.http import Headers
from scrapy.utils.request import request_fingerprint
from . import Config
from .util import convert

class MongoCacheStorage(object):
    def __init__(self, settings):
        self.settings = settings
        self.config = Config(settings)
        self.expiration_secs = settings.getint('HTTPCACHE_EXPIRATION_SECS')
        self.logger = logging.getLogger(__name__)

    def open_spider(self, spider):
        self.clt = MongoClient(self.config.host)
        self.db = self.clt[self.config.database_name()]
        self.col = self.db['cache']

    def close_spider(self, spider):
        self.clt.close()

    def retrieve_response(self, spider, request):
        """Return response if present in cache, or None otherwise."""
        key = self._request_key(request)

        data = self.col.find_one({'key': key})
        if not data: # not cache
            return 

        # expiration?
        mtime = data['meta']['timestamp']
        if 0 < self.expiration_secs < time() - float(mtime):
            return  # expired
            
        # retrieve
        body = data['response_body']
        url = str(data.get('url'))
        status = data['meta']['status']
        headers = Headers(data['response_headers'])
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        """Store the given response in the cache."""
        key = self._request_key(request)
        data = {
            'url': request.url,
            'key': key,
            'meta' :  {
                'url': request.url,
                'method': request.method,
                'status': response.status,
                'response_url': response.url,
                'timestamp': time(),
            },
            'response_headers' : convert(response.headers),
            'response_body': bson.binary.Binary(response.body),
            'request_headers' : convert(request.headers),
            'request_body': bson.binary.Binary(request.body)
        }
        #self.logger.info(request.url)
        #self.logger.info(data)
        self.col.update({'key': key}, data, upsert=True)

    def _request_key(self, request):
        return str(request_fingerprint(request))





from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint
from . import Config

class CacheDupeFilter(BaseDupeFilter):
    """DupeFilter which can be used wth MongoCacheStorage"""

    def __init__(self, settings, debug=False):
        self.config = Config(settings)
        self.debug = debug
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(settings, debug)

    def request_seen(self, request):
        fp = request_fingerprint(request)
        return self.col.count({'key': fp }) > 0
        
    def open(self):
        self.clt = MongoClient(self.config.host)
        self.db = self.clt[self.config.database_name()]
        self.col = self.db['cache']
        self.logger.debug("%s:%s %s" % (type(self).__name__,  '__init___',  [self.config, self.clt, self.db]))

    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clt.close()

    def clear(self):
        """Clears fingerprints data"""
        self.server.delete(self.key)
