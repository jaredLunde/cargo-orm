"""

  `Cargo SQL ORM`
  ``SQL table object modelling classes``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import asyncio
import psycopg2

from vital.cache import cached_property
from vital.tools.lists import grouped
from vital.debug import prepr

from cargo.aio.clients import *
from cargo.etc.types import *
from cargo.exceptions import *
from cargo.expressions import *
from cargo.statements import *
from cargo.fields import Field
from cargo.orm import ORM, Model


__all__ = (
    "AioORM",
    "AioModel",
    "AioRestModel")


class AioORM(ORM):
    table = None

    # ``Connection handing``

    @cached_property
    def db(self):
        return self._client or local_client.get('aiodb') or AioPostgresPool()

    client = db

    async def __aenter__(self):
        """ Context manager, connects to :prop:cargo.ORM.db
            ..
            with ORM() as sql:
                sql.where(Expression("table.field", "=", "some_val"))
                result = sql.select()
            print(result)
            ..

            -> @self
        """
        await self.connect()
        return self

    def __aexit__(self, type=None, value=None, tb=None):
        """ Context manager, closes the :prop:cargo.ORM.db connection """
        self.close()

    async def connect(self, **options):
        """ Connects the :prop:cargo.ORM.db.

            Passes @*args and @**kwargs to the local instance of
            :class:Postgres
        """
        return await self.db.connect(**options)

    # ``Query execution``

    async def run_iter(self, *queries):
        """ Runs all of the queries in @*qs or in :prop:queries atomically
            and fetches all of the results if there are any

            @*queries: (:class:cargo.Query) one or several objects

            -> (#list) results of the queries
        """
        #: Gets the client connection from a pool or client object
        conn = await self.db.get()
        results = []
        for q in queries or self.queries:
            #: Executes the query with its parameters
            try:
                result = await self.execute(q.query,
                                            q.params,
                                            conn=conn)
            except QueryError:
                if not queries:
                    self.queries.remove(q)
                    self.reset_state()
                raise
            try:
                #: Fetches the result
                result = await result.__getattribute__('fetchone'
                                                       if q.one else
                                                       'fetchall')()
            except psycopg2.ProgrammingError:
                #: No results to fetch
                pass
            results.append(result)
        self._reset_accordingly(queries, conn)
        return results

    def _reset_accordingly(self, queries, conn):
        if queries:
            #: Explicit 'run', the user is in control of everything except
            #  for the query state and client connection
            self.reset_state()
        else:
            #: Implicit 'run', the ORM is in control
            self.reset(multi=True)
        self.db.put(conn)

    async def run(self, *queries):
        """ Runs all of the queries in @*qs or in :prop:queries,
            fetches all of the results if there are any

            @*queries: (:class:cargo.Query) one or several

            -> #list of results if more than one query is executed, otherwise
                the cursor factory
        """
        results = await self.run_iter(*queries)
        if len(results) == 1:
            return results[0]
        else:
            return results

    async def get_cursor(self, conn, *args, **kwargs):
        """ Gets a database cursor from @conn
            -> (:class:psycopg2.cursor)
        """
        return await conn.cursor(*args, cursor_factory=self._cursor_factory,
                                 **kwargs)

    async def execute(self, query, params=None, conn=None):
        """ Executes @query with @params in the cursor and autocommits.

            @query: (#str) query string
            @params: (#tuple|#dict|#list) of params referenced in @query
                with |%s| or |%(name)s|
            @conn: (:class:Postgres|:class:PostgresPoolConnection) if
                a connection object is provided, it is your responsibility
                to put the connection if it is a part of a pool.

            -> :mod:psycopg2 cursor or None
        """
        #: Gets a client connection if one wasn't passed as an argument
        _conn = conn or await self.db.get()
        cursor = await self.get_cursor(_conn)
        #: Sets the search path to the locally defined schema
        query = self._prepend_search_path_to(query)
        #: For debug mode
        self.debug(cursor, query, params)
        try:
            #: Executes the cursor
            await cursor.execute(query, params or tuple())
        except (psycopg2.ProgrammingError,
                psycopg2.IntegrityError,
                psycopg2.DataError,
                psycopg2.InternalError) as e:
            raise QueryError(e.args[0].strip())
        #: Puts a client connection away if it is a pool and no connection
        #  was passed in arguments. If a connection object is passed,
        #  it's the user's responsibility to put it away.
        if conn is None:
            self.db.put(_conn)
        return cursor


#
#  ``Models``
#


class AioModel(AioORM, Model):

    def __iter__(self):
        """ -> iterates through records in the model """
        return self.iter()

    async def approx_size(self, alias=None):
        """ Selects the approximate size of the model.
            ..
            SELECT reltuples::bigint FROM pg_class WHERE oid = 'foo'::regclass;
            ..
        """
        table = self.table if not self.db.schema else\
            "{}.{}".format(self.db.schema, self.table)
        expr = Expression(safe('oid'),
                          '=',
                          parameterize(table, safe('::regclass')))
        self.from_('pg_class')
        self.where(expr)
        self.naked()
        self.state.add_fields(safe('reltuples::bigint %s'
                                   % (alias or "count")))
        q = Select(self)
        q.one = True
        result = await self.run(q)
        self.reset_naked()
        if alias is None:
            try:
                result = result['count']
            except (TypeError, KeyError):
                try:
                    result = result.count
                except KeyError:
                    result = result[0]
        return result

    async def get_cursor(self, conn, *args, **kwargs):
        """ Gets a database cursor from @conn
            -> (:class:psycopg2.cursor)
        """
        if not self._is_naked():
            cursor = await conn.cursor(*args, cursor_factory=ModelCursor,
                                       **kwargs)
            cursor._cargo_model = self
            return cursor
        else:
            return await conn.cursor(*args,
                                     cursor_factory=self._cursor_factory,
                                     **kwargs)

    async def run_iter(self, *queries, **kwargs):
        """ :see::meth:ORM.run_iter
            -> yields :prop:_cursor_factory if this isn't a :meth:many query
                or :prop:_naked is |True|, otherwise returns :class:Model
        """
        if self._multi:
            self.new()
        return (x for x in await super().run_iter(*queries, **kwargs))

    async def pull_all(self, *args, dry=False, **kwargs):
        """ Pulls all the relationships in the model.
            :see::meth:Relationship.pull

            -> (#dict) |{relationship_name: results}|
        """
        d = {}
        for relationship in self.relationships:
            d[relationship._owner_attr] = await relationship.pull(*args,
                                                                  dry=dry,
                                                                  **kwargs)
        return d

    '''async def iternaked(self, offset=0, limit=0, buffer=100, order_field=None,
                        reverse=False):
        """ Yields cursor factory until there are no more results to fetch.

            @offset: (#int) cursor start position
            @limit: (#int) total number of results to fetch
            @buffer: (#int) cursor max results to fetch in one page
            @reverse: (#bool) True if returning in descending order
            @order_field: (:class:cargo.Field) object to order the
                query by
        """
        return (item for item in await self.naked().iter(
            offset=offset,
            limit=limit,
            buffer=buffer,
            order_field=order_field,
            reverse=reverse))

    async def iter(self, offset=0, limit=0, buffer=100, order_field=None,
                   reverse=False):
        """ Yields populated models until there are no more
            results to fetch.

            @offset: (#int) cursor start position
            @limit: (#int) total number of results to fetch
            @buffer: (#int) cursor max results to fetch in one page
            @reverse: (#bool) True if returning in descending order
            @order_field: (:class:cargo.Field) object to order the
                query by
        """
        if not self.state.has('WHERE'):
            self.where(self.best_available_index or True)
        field = getattr(self, order_field) if order_field else self.best_index
        if field is not None:
            order = field.asc() if not reverse else field.desc()
            self.order_by(order)
        self.offset(offset)
        if limit:
            self.limit(limit)
        q = await super().dry().select().execute()
        while True:
            results = q.fetchmany(buffer)
            if not results:
                break
            for result in results:
                yield result
        self.reset()'''


class AioRestModel(AioModel):

    async def patch(self, field, **kwargs):
        """ Updates one @field in the model based on the
            :prop:best_available_index

            @field: an instance of :class:cargo.Field

            -> result of query, differs depending on cursor settings
        """
        return await self.update(field, **kwargs)
