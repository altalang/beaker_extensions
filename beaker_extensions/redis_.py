import logging
from beaker.exceptions import InvalidCacheBackendError

from beaker_extensions.nosql import Container
from beaker_extensions.nosql import NoSqlManager
from beaker_extensions.nosql import pickle

from datetime import datetime

try:
    from redis import StrictRedis
except ImportError:
    raise InvalidCacheBackendError("Redis cache backend requires the 'redis' library")

log = logging.getLogger(__name__)

class RedisManager(NoSqlManager):
    def __init__(self, namespace, url=None, data_dir=None, lock_dir=None, **params):
        self.expiretime = params.pop('expiretime', None)
        NoSqlManager.__init__(self, namespace, url=url, data_dir=data_dir, lock_dir=lock_dir, **params)

    def open_connection(self, host, port, **params):
        self.db_conn = StrictRedis(host=host, port=int(port), **params)

    def __getitem__(self, key):
        return pickle.loads(self.db_conn.hget(self._format_key(key), 'data'))

    def __contains__(self, key):
        return self.db_conn.exists(self._format_key(key))

    def set_value(self, key, value, expiretime=None):
        key = self._format_key(key)

        #
        # beaker.container.Value.set_value calls NamespaceManager.set_value
        # however it (until version 1.6.4) never sets expiretime param.
        #
        # Checking "type(value) is tuple" is a compromise
        # because Manager class can be instantiated outside container.py (See: session.py)
        #
        if (expiretime is None) and (type(value) is tuple):
            expiretime = value[1]

        self.db_conn.hset(key, 'data', pickle.dumps(value))
        self.db_conn.hset(key, 'accessed', datetime.now())
        self.db_conn.hsetnx(key, 'created', datetime.now())

        if expiretime or self.expiretime:
            self.db_conn.expire(key, expiretime or self.expiretime)

    def __delitem__(self, key):
        self.db_conn.delete(self._format_key(key))

    def _format_key(self, key):
        return 'beaker:%s:%s' % (self.namespace, key.replace(' ', '\302\267'))

    def do_remove(self):
        self.db_conn.flushdb()

    def keys(self):
        return self.db_conn.keys('beaker:%s:*' % self.namespace)


class RedisContainer(Container):
    namespace_class = RedisManager
