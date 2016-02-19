#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Postgres Clients`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
try:
    import ujson as json
except:
    import json

from pydoc import locate

import psycopg2
import psycopg2.pool
import psycopg2.extras

from multiprocessing import cpu_count

from vital.cache import DictProperty, local_property
from vital.tools.dicts import merge_dict
from bloom.cursors import CNamedTupleCursor
from vital.debug import prepr


__all__ = (
    "local_client",
    "Postgres",
    "PostgresPool",
    "db",
    "create_client",
    "create_pool",
    # TODO: remove these
    "create_kola_client",
    "create_kola_pool",
    "create_kola_db"
)


class BaseClient(object):

    @DictProperty('_cache', 'cursor_factory', read_only=True)
    def cursor_factory(self):
        if isinstance(self._cursor_factory, str):
            return locate(self._cursor_factory)
        return self._cursor_factory

    @property
    def schema(self):
        opt = self._connection_options
        return opt.get('schema', opt.get('search_path', self._schema))

    @staticmethod
    def to_dsn(opt):
        """ Converts @opt to a string if it isn't one already.
            @opt: (#dict) dsn options
        """
        if isinstance(opt, str):
            return opt
        return " ".join(
            '{}={}'.format(k.replace('database', 'dbname'), v)
            for k, v in opt.items()
            if k in {'dbname', 'database', 'user', 'password', 'host', 'port'})


class Postgres(BaseClient):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``

        Creates a new :class:bloom.Model with a customized
        Postgres instance.
        ..
            from psycopg2.extras import NamedTupleCursor
            from bloom import Model, Postgres

            class MyModel(Model):

                def __init__(self, **model):
                    client = Postgres(cursor_factor=NamedTupleCursor)
                    super().__init__(client=client, **model)
        ..
    """
    __slots__ = ('_dsn', 'autocommit', '_connection', '_connection_options',
                 '_schema', 'encoding', '_cursor_factory', '_cache')

    def __init__(self, dsn=None, cursor_factory=CNamedTupleCursor,
                 connection=None, autocommit=False, encoding=None, schema=None,
                 **connection_options):
        """ `Postgres Client`

            This is a thin wrapper for the :mod:psycopg2 connection object
            returned by :func:psycopg2.connect, it's purpose being to
            standardize a client object in the event that other SQL backends
            aside from :mod:psycopg2 or Postgres are added in the future.

            @connection: (:mod:psycopg2) connection object returned by
                :func:psycopg2.connect
            @cursor_factory: (:mod:psycopg2 cursor factory) passed to
                :prop:cursor
            @autocommit: (#bool) if True all the commands executed will be
                immediately committed and no rollback is possible
            @encoding: (#str) Sets the client encoding for the current session.
                If None, the default is the encoding defined by the database.
            @schema: (#str) schema to set the postgres search path to
            @**connection_options: |key=value| arguments to pass to
                :func:psycopg2.connect
        """
        self._cache = {}

        # Connection options
        self._dsn = dsn
        self.autocommit = autocommit
        self._connection = connection
        self._connection_options = connection_options or {}
        self._schema = schema
        self.encoding = encoding

        # Cursor options
        self._cursor_factory = cursor_factory

    @prepr('_connection', 'autocommit', _break=False)
    def __repr__(self): return

    def __enter__(self):
        return self.connection

    def __exit__(self, *exc_info):
        self.close()

    @property
    def closed(self):
        """ -> #bool True if the connection is closed """
        try:
            return self._connection.closed
        except AttributeError:
            return True

    @property
    def connection(self):
        """ Creates a new connection if there isn't one or it is closed.
            -> :mod:psycopg2 connection object
        """
        if not self._connection or self._connection.closed:
            self.connect()
        return self._connection

    def cursor(self, name=None, cursor_factory=None, scrollable=None,
               withhold=False):
        """ Creates a new cursor object with given options, defaulting to
            configured options.

            @name: (#str) name of the cursor
            @cursor_factory: :mod:psycopg2 cursor factory passed to
                :prop:cursor
            @scrollable: (#bool) specifies if a named cursor is declared
                SCROLL, hence is capable to scroll backwards (using scroll()).
                If True, the cursor can be scrolled backwards, if False it is
                never scrollable. If None (default) the cursor scroll option is
                not specified, usually but not always meaning no backward
                scroll.
            @withhold: (#bool) specifies if a named cursor lifetime should
                extend outside of the current transaction, i.e., it is possible
                to fetch from the cursor even after a connection.commit()
                (but not after a connection.rollback()).

            -> :mod:psycopg2 cursor object
        """
        cursor_factory = cursor_factory if cursor_factory is not None else \
            self.cursor_factory
        cursor = self.connection.cursor(
            name=name, cursor_factory=cursor_factory, scrollable=scrollable,
            withhold=withhold)
        if self.schema:
            cursor.execute('SET search_path TO {}'.format(self.schema))
        return cursor

    def connect(self, dsn=None, **options):
        """ Opens a :mod:psycopg2 connection with @options combined with
            :prop:connection_options

            -> :mod:psycopg2 connection object
        """
        if not self._connection or self._connection.closed or dsn or options:
            dsn = dsn or self._dsn
            if not dsn:
                dsn = self.to_dsn(self._connection_options)
            self._connection = psycopg2.connect(dsn)
            self._set_conn_options()
        return self._connection

    def _set_conn_options(self, seen=False):
        if not seen:
            if self.autocommit:
                self._connection.set_session(autocommit=self.autocommit)
            if self.encoding:
                self._connection.set_client_encoding(encoding=self.encoding)
            psycopg2.extras.register_inet(None, self._connection)
            try:
                psycopg2.extras.register_default_json(
                    self._connection, True, json.loads)
                psycopg2.extras.register_default_jsonb(
                    self._connection, True, json.loads)
            except:
                pass

    def commit(self):
        """ Commits a transaction """
        self._connection.commit()

    def rollback(self):
        """ Rolls back a transaction """
        self._connection.rollback()

    def close(self):
        """ Closes the psycopg2 cursor and connection """
        self._connection.close()

    def get(self, *args, **kwargs):
        """ Dummy method in order to work seamlessly in the ORM with
            :class:PostgresPool
        """
        return self

    def put(self, *args, **kwargs):
        """ Dummy method in order to work seamlessly in the ORM with
            :class:PostgresPool
        """


class PostgresPoolConnection(Postgres):
    __slots__ = ('pool', '_connection')

    def __init__(self, pool, connection):
        self.pool = pool
        self._connection = connection
        self._set_conn_options()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.pool.put(self)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.pool.__getattribute__(name)

    def put(self, *args, **kwargs):
        """ Returns the connection to the pool """
        self.pool.putconn(self._connection, *args, **kwargs)


class PostgresPool(BaseClient):
    __slots__ = ('_dsn', 'autocommit',  '_connection_options', '_schema',
                 'encoding', '_cursor_factory', 'minconn', 'maxconn', '_pool',
                 '_seen', '_cache')

    def __init__(self, minconn=1, maxconn=1, dsn=None,
                 cursor_factory=CNamedTupleCursor,
                 pool=None, autocommit=False, encoding=None, schema=None,
                 **connection_options):
        """ :see::class:Postgres
            @minconn: (#int) minimum number of connections to establish
                within the pool
            @maxconn: (#int) maximum number of connections to establish
                within the pool
            @pool: (:class:psycopg2.pool.ThreadedConnectionPool) initialized
                pyscopg2 connection pool object
        """
        self._cache = {}
        # Connection options
        self._dsn = dsn
        self.autocommit = autocommit
        self._connection_options = connection_options or {}
        self._schema = schema
        self.encoding = encoding
        self._pool = pool
        self.minconn = minconn
        self.maxconn = maxconn

        # Cursor options
        self._cursor_factory = cursor_factory
        self._seen = set()

    @prepr('_pool', 'autocommit', _break=False)
    def __repr__(self): return

    def __enter__(self):
        """ Gets a connection from the pool """
        self.connect()
        return self

    def __exit__(self, *exc_info):
        """ Puts away the active connection in the pool """
        self.close()

    @property
    def closed(self):
        """ -> #bool True if the connection is closed """
        try:
            return self._pool.closed
        except (ValueError, AttributeError):
            return True

    def connect(self, dsn=None, **options):
        """ Opens a :mod:psycopg2 connection with @options combined with
            :prop:connection_options

            -> :mod:psycopg2 connection object
        """
        if not self._pool or self._pool.closed or dsn or options:
            dsn = dsn or self._dsn
            opt = merge_dict(self._connection_options, options)
            if not dsn:
                dsn = self.to_dsn(opt)
            minconn = opt.get('minconn', self.minconn)
            maxconn = opt.get('maxconn', self.maxconn)
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn, maxconn, dsn)
        return self._pool

    @property
    def pool(self):
        return self.connect()

    def get(self, *args, **kwargs):
        return PostgresPoolConnection(pool=self,
                                      connection=self.pool.getconn())

    def put(self, poolconn, *args, **kwargs):
        """ Returns the connection to the pool
            @poolcon: (:class:PostgresPoolConnection) object
        """
        if hasattr(poolconn, '_connection'):
            poolconn = poolconn._connection
        self.pool.putconn(poolconn, *args, **kwargs)

    def close(self):
        """ Closes all the psycopg2 cursors and connections """
        self.pool.closeall()


#: Thread-local storage for connection clients
local_client = local_property()
local_client = {}


class _db(object):
    """ Thread-local ORM session """
    engine = None

    def __init__(self):
        self.engine = local_property()

    def __getattr__(self, name):
        if name != 'engine' and hasattr(self.engine, name):
            return self.engine.__getattribute__(name)
        return self.__getattribute__(name)

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __delattr__(self, name):
        return self.engine.__detattr__(name)

    def __repr__(self):
        try:
            return self.engine.__repr__()
        except RuntimeError:
            return "<_db id={}>".format(id(self))

    def __get__(self):
        if self.engine:
            return self.engine
        return self


db = _db()


def create_db(*opt, client=None, **opts):
    """ Creates a global :class:ORM object which

        @*opt and **opts are passed to :class:Postgres
    """
    from bloom.orm import ORM
    if not client:
        create_client(*opt, **opts)
    db.engine = ORM(*opt, client=client, **opts)


def create_client(*opt, name='db', **opts):
    """ Creates a connection client in the :attr:local_client thread which
        will be used as the default client in the ORM.

        @name: (#str) name in the :attr:local_client thread dictionary to cache
            the client within

        See also: :class:Postgres
    """
    local_client[name] = Postgres(*opt, **opts)
    return local_client[name]


def create_pool(minconn=None, maxconn=None, name='db', *args, **kwargs):
    """ Creates a connection pool in the :attr:local_client thread which
        will be used as the default client in the ORM.

        @name: (#str) name in the :attr:local_client thread dictionary to cache
            the pool within

        See also: :class:PostgresPool
    """
    minconn = minconn or cpu_count()
    maxconn = maxconn or (cpu_count() * 2)
    local_client[name] = PostgresPool(minconn, maxconn, *args, **kwargs)
    return local_client[name]


# TODO: Move to separate vital package
def create_kola_client(*opt, name='db', **opts):
    """ :see::func:create_client

        The client created will be configured with :class:vital.config
        within the keyname @name

        @name: (#str) name in the :attr:local_client thread dictionary to cache
            the client within, and the name in the local :class:vital.config
            where the configuration arguments are stored
    """
    from kola import config
    cfg = config.get(name, {})
    return create_client(*opt, name=name, **merge_dict(cfg, opts))


def create_kola_pool(*opt, name='db', **opts):
    """ :see::func:create_pool

        The pool created will be configured with :class:vital.config
        within the keyname @name

        @name: (#str) name in the :attr:local_client thread dictionary to cache
            the pool within, and the name in the local :class:vital.config
            where the configuration arguments are stored
    """
    from kola import config
    cfg = config.get(name, {})
    return create_pool(*opt, name=name, **merge_dict(cfg, opts))


def create_kola_db(*opt, client=None, **opts):
    """ Creates a global :class:ORM object which

        @*opt and **opts are passed to :class:Postgres
    """
    from bloom.orm import ORM
    if not client:
        create_kola_client(*opt, **opts)
    db.engine = ORM(*opt, client=client, **opts)
