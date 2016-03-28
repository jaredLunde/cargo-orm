"""

  `Async Postgres Clients`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import aiopg
import asyncio
from collections import defaultdict
from multiprocessing import cpu_count

from vital.tools.dicts import merge_dict
from vital.debug import prepr

from cargo.cursors import CNamedTupleCursor
from cargo.clients import BasePostgresClient, PostgresPool, Postgres,\
                          PostgresPoolConnection, local_client, _db


__all__ = (
    "AioPostgresPool",
    "aiodb",
    "create_aio_pool"
)


class AioPostgresPoolConnection(Postgres):
    __slots__ = ('pool', '_connection')

    def __init__(self, pool, connection):
        self.pool = pool
        self._connection = connection
        self._set_conn_options()

    async def get_type_OID(self, typname):
        """ -> (#tuple) |(OID, ARRAY_OID)| """
        q = """SELECT t.oid AS OID, t.typname AS name
               FROM pg_catalog.pg_type t
               WHERE t.typname IN(%s, %s)
               ORDER BY name DESC;"""
        conn = self.get()
        cur = await conn.cursor(cursor_factory=CNamedTupleCursor)
        await cur.execute(q, ('_%s' % typname, typname))
        res = await cur.fetchall()
        self.put(conn)
        return tuple(r.oid for r in res)

    @staticmethod
    async def apply_schema(cursor, *schemas):
        """ Sets @schemas to the cursor search path.
            @schemas: (#str) one or several schema search paths
        """
        return await cursor.execute('SET search_path TO %s' %
                                    ", ".join(schemas))

    def __aenter__(self):
        return self

    def __aexit__(self, *exc_info):
        self.pool.put(self)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.pool.__getattribute__(name)

    async def put(self):
        """ Returns the connection to the pool """
        self.pool.put(self._connection)


class AioPostgresPool(BasePostgresClient):
    __slots__ = ('_dsn', '_connection_options', '_schema', 'encoding',
                 '_cursor_factory', 'minconn', 'maxconn', '_pool', '_cache',
                 '_search_paths', '_events', 'autocommit', 'loop')

    def __init__(self, minconn=1, maxconn=1, dsn=None,
                 cursor_factory=CNamedTupleCursor, pool=None,
                 encoding=None, schema=None, search_paths=None, events=None,
                 loop=None, **connection_options):
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
        self._events = events or defaultdict(dict)
        self._connection_options = connection_options or {}
        self._schema = schema
        self._search_paths = search_paths or []
        self.encoding = encoding
        self._pool = pool
        self.minconn = minconn
        self.maxconn = maxconn
        self.autocommit = True
        self.loop = loop or asyncio.get_event_loop()

        # Cursor options
        self._cursor_factory = cursor_factory

    @prepr('_pool')
    def __repr__(self): return

    async def __aenter__(self):
        """ Gets a connection from the pool """
        await self.connect()
        return self

    async def __aexit__(self, *exc_info):
        """ Puts away the active connection in the pool """
        self.close()

    async def connect(self, dsn=None, **options):
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
            self._pool = await aiopg.create_pool(dsn,
                                                 minsize=self.minconn,
                                                 maxsize=self.maxconn,
                                                 loop=self.loop)
        return self._pool

    @property
    def pool(self):
        return self._pool

    async def get(self, *args, **kwargs):
        return PostgresPoolConnection(
            pool=self, connection=(await self.pool.acquire()))

    def put(self, poolconn):
        """ Returns the connection to the pool
            @poolcon: (:class:PostgresPoolConnection) object
        """
        try:
            poolconn = poolconn._connection
        except AttributeError:
            pass
        self.pool.release(poolconn)

    def close(self):
        """ Closes all the psycopg2 cursors and connections """
        try:
            self.pool.close()
        except AttributeError:
            pass

    def wait_closed(self):
        """ Closes all the psycopg2 cursors and connections """
        try:
            return self.pool.wait_closed()
        except AttributeError:
            pass


class _aiodb(_db):

    async def bind(self, *opt, client=None, **opts):
        """ Creates a thread-local, global :class:ORM object with the
            given options.

            @*opt and **opts are passed to :class:Postgres
        """
        from cargo.aio.orm import AioORM
        if not client:
            client = await create_aio_pool(*opt, **opts)
        self.engine = AioORM(client=client)
        return self

    open = bind

    def close(self):
        try:
            self.engine.db.close()
        except AttributeError:
            pass

    def wait_closed(self):
        try:
            return self.engine.db.wait_closed()
        except AttributeError:
            pass


aiodb = _aiodb()


async def create_aio_pool(minconn=None, maxconn=None, name='aiodb', *args,
                          **kwargs):
    """ Creates a connection pool in the :attr:local_client thread which
        will be used as the default client in the ORM.

        @name: (#str) name in the :attr:local_client thread dictionary to cache
            the pool within

        See also: :class:AioPostgresPool
    """
    minconn = minconn or cpu_count()
    maxconn = maxconn or (cpu_count() * 2)
    local_client[name] = AioPostgresPool(minconn, maxconn, *args, **kwargs)
    await local_client[name].connect()
    return local_client[name]
