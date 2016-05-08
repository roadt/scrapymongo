import bson
from time import time
from pymongo import MongoClient

from scrapy.responsetypes import responsetypes
from scrapy.http import Headers
from scrapy.utils.request import request_fingerprint
from . import Config

class MongoCacheStorage(object):
    def __init__(self, settings):
        self.settings = settings
        self.config = Config(settings)
        self.expiration_secs = settings.getint('HTTPCACHE_EXPIRATION_SECS')

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
            'response_headers' : response.headers,
            'response_body': bson.binary.Binary(response.body),
            'request_headers' : request.headers,
            'request_body': bson.binary.Binary(request.body)
        }
        self.col.update({'key': key}, data, upsert=True)

    def _request_key(self, request):
        return request_fingerprint(request)


