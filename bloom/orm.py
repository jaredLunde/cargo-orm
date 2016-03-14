"""

  `Bloom SQL ORM`
  ``SQL table object modelling classes``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import re
import copy
from collections import OrderedDict

try:
    from ujson import dumps, loads
except ImportError:
    from json import dumps, loads

try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple

import psycopg2
import psycopg2.extras
from psycopg2.extensions import cursor as _cursor

from vital.cache import cached_property
from vital.tools.strings import camel_to_underscore
from vital.debug import prepr, line, logg

from bloom.clients import *
from bloom.cursors import CNamedTupleCursor
from bloom.etc.types import *
from bloom.exceptions import *
from bloom.expressions import *
from bloom.statements import *
from bloom.relationships import *
from bloom.fields import *


__all__ = (
    "ORM",
    "QueryState",
    "Joins",
    "Model",
    "RestModel"
)


class ORM(object):

    def __init__(self, client=None, cursor_factory=None, schema=None,
                 table=None, debug=False):
        """ `SQL ORM`
            The base model which interacts with :class:Postgres
            and provides the basic ORM interface.

            @client: (:class:Postgres or :class:PostgresPool)
            @cursor_factory: :mod:psycopg2.extras cursor factories
            @schema: (#str) the name of the schema search path
            @table: (#str) default table
            @debug: (#bool) prints mogrified queries when queries are executed,
                severely hampers performance and should only be used for,
                well, debugging purposes.
        """
        self._client = client
        self.queries = []
        self.schema = schema
        self._state = QueryState(self)
        self._multi = False
        self._many = False
        self._with = False
        self._dry = False
        self._naked = None
        self._cursor_factory = cursor_factory
        if not hasattr(self, 'table'):
            self.table = table
        self._debug = debug

    @prepr('db')
    def __repr__(self): return

    # ``Connection handing``

    @cached_property
    def db(self):
        return self._client or local_client.get('db') or Postgres()

    client = db

    def __enter__(self):
        """ Context manager, connects to :prop:bloom.ORM.db
            ..
            with ORM() as sql:
                sql.where(Expression("table.field", "=", "some_val"))
                result = sql.select()
            print(result)
            ..

            -> @self
        """
        self.connect()
        return self

    def __exit__(self, type=None, value=None, tb=None):
        """ Context manager, closes the :prop:bloom.ORM.db connection """
        self.close()

    def connect(self, **options):
        """ Connects the :prop:bloom.ORM.db.

            Passes @*args and @**kwargs to the local instance of
            :class:Postgres
        """
        return self.db.connect(**options)

    def close(self):
        """ Closes the :prop:bloom.ORM.db, resets the :prop:state """
        self.reset()
        return self.db.close()

    def set_table(self, table):
        """ Sets the ORM table to @table (#str) """
        self.table = table

    def set_cursor_factory(self, factory):
        """ Sets the ORM default cursor factory to @factory """
        self._cursor_factory = factory

    # Pickling and copying

    def __getstate__(self):
        """ For pickling """
        d = self.__dict__.copy()
        d['_client'] = None
        if 'db' in d:
            del d['db']
        nt = self.__class__.__name__ + 'Record'
        if nt in d:
            del d[nt]
        fkeys = []
        for k, v in d.copy().items():
            if isinstance(v, Field) and hasattr(v, 'ref'):
                d[k] = ForeignKey(v._state.ref, *v._state.args,
                                  relation=v._state.relation,
                                  **v._state.kwargs)
                fkeys.append(v.name)
        if '_fields' in d:
            d['_fields'] = list(filter(lambda f: f.name not in fkeys,
                                       d['_fields']))
            d['_foreign_keys'] = list(filter(lambda f: f.name not in fkeys,
                                             d['_foreign_keys']))
        return d

    def __setstate__(self, dict):
        """ For pickling """
        fkeys = []
        for k, v in dict.items():
            if isinstance(v, ForeignKey):
                fkeys.append((k, v))
            self.__dict__[k] = v

        for name, fkey in fkeys:
            fkey.forge(self, name)

    # ``Insert/Select/Delete/Update Clauses``

    def dry(self):
        """ Prevents queries from executing when they are declared and
            makes the ORM return a :class:Query object rather than
            a list of query results.
        """
        self._dry = True
        return self

    def _is_naked(self):
        return True

    def _cast_query(self, q):
        """ Decides how to return the result of a query call.
            @q: (:class:Query)
        """
        if q.is_subquery:
            self.reset()
            return Subquery(q)
        elif self._dry:
            self.reset_state()
            self.reset_dry()
            return q
        elif self._many:
            return self
        elif self._multi:
            self.add_query(q)
            self.reset()
            return self
        elif q._with:
            self.add_query(q)
            return self
        else:
            self.add_query(q)
            return self.run()

    def _insert(self, *fields, **kwargs):
        """ Parses the :prop:state as an :class:INSERT statement

            @*fields: :class:Field(s) to set VALUES for, if None are given,
                all fields in a given model will be evaluated
            @**kwargs: keyword arguments to pass to :class:INSERT

            -> result of :class:INSERT query
        """
        return self._cast_query(Insert(self, *fields, **kwargs))

    insert = _insert

    def _select(self, *fields, **kwargs):
        """ Parses the :prop:state as a :class:SELECT statement. If a table
            is specified within the model and a |FROM| clause hasn't been
            added to the state, a |FROM| clause will automatically be added
            using |self.table|

            @*fields: :class:Field(s) or :mod:bloom.expressions
                to retrieve values for
            @**kwargs: keyword arguments to pass to :class:SELECT

            -> result of :class:SELECT query, :class:Subquery object if this
                :prop:state is a subquery, or :class:Query if :prop:_dry
                is True
        """
        if not self.state.has('FROM'):
            self.from_()
        return self._cast_query(Select(self, *fields, **kwargs))

    select = _select

    def get(self, *fields, **kwargs):
        """ Gets a single record from the database
            :see::meth:_select
        """
        return self.one().select(*fields, **kwargs)

    def _update(self, *exps, **kwargs):
        """ Parses the :prop:state as an :class:UPDATE statement. An |UPDATE|
            will be performed on the fields given in @*exps

            @*fields: :class:bloom.Field or :mod:bloom.expressions
                objects
            @**kwargs: keyword arguments to pass to :class:SELECT

            -> result of :class:UPDATE query, :class:Subquery object if this
                :prop:state is a subquery, or the :class:Query if :prop:_dry
                is |True|
        """
        return self._cast_query(Update(self, *exps, **kwargs))

    update = _update

    def _delete(self, **kwargs):
        """ Parses the :prop:state as a :class:DELETE statement. If a table
            is specified within the model and a |FROM| clause hasn't been
            added to the state, a |FROM| clause will automatically be added
            using |self.table|

            @**kwargs: keyword arguments to pass to :class:SELECT

            -> result of :class:DELETE query, :class:Subquery object if this
                :prop:state is a subquery, or the :class:Query if :prop:_dry
                is |True|
        """
        self.from_()
        return self._cast_query(Delete(self, **kwargs))

    delete = _delete
    remove = _delete

    def _upsert(self, *fields, conflict_field=None, conflict_action=None,
                **kwargs):
        """ !! Postgres 9.5 only !!
            Updates @fields if they don't already exist, otherwise the
            fields are inserted.

            @*fields: (:class:Field) to insert or update
            @conflict_field: (:class:Field) the unique field which will
                determine if a conflict exists. If none is given, Postgres
                will decide based on unique fields and primary keys.
            @conflict_action: (:class:Query or :class:Clause) action
                to perform when the conflict arises, defaults to |DO UPDATE|
            @**kwargs: additional arguments to pass to :meth:insert
        """
        conflict_action = conflict_action or Clause('DO UPDATE')
        if conflict_field:
            self.on(
                Clause('CONFLICT', conflict_field, wrap=True), conflict_action)
        else:
            self.on(Clause('CONFLICT'), conflict_action)
        return self._insert(*fields, **kwargs)

    upsert = _upsert

    def _raw(self, *args, **kwargs):
        """ Parses the :prop:state as a :class:Raw statement. All clauses
            added to the ORM :prop:state will be parsed in the order of
            which they were added.

            @**kwargs: keyword arguments to pass to :class:SELECT

            -> result of :class:Raw query, :class:Subquery object if this
                :prop:state is a subquery, or the :class:Raw if :prop:_dry
                is |True|
        """
        return self._cast_query(Raw(self, *args, **kwargs))

    raw = _raw

    # ``Clauses``

    def from_(self, table=None, alias=None):
        """ Sets a |FROM| clause in the :prop:state
            |FROM @table @alias|

            @table: #str table name
            @alias: #str alias for table name

            -> @self
        """
        table = table if table else self.table
        if table:
            self.state.add(CommaClause("FROM", safe(table), alias=alias))
        return self

    use = from_

    def window(self, alias, *expressions, partition_by=None, order_by=None):
        """ Creates a |WINDOW| clause and adds it to the query :prop:state.
            This is only for :class:Select queries.

            e.g. |WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);|

            @alias: (#str) name of the alias
            @partition_by: (:class:Field|:class:BaseExpression) Creates
                a |PARTITION BY| clause in the |AS| portion of the |WINDOW|
                clause
            @order_by: (:class:Field|:class:BaseExpression) Creates
                a |PARTITION BY| clause in the |AS| portion of the |WINDOW|
                clause

            ..
                model = Model()
                model.window(alias='w',
                             partition_by=model.partition_field,
                             model.order_field.desc())
                model.select(model.order_field.avg().over('w'))
            ..
            |SELECT avg(foo.order_field) OVER w FROM foo  |
            |WINDOW w AS (PARTITION BY foo.partition_field|
            |             ORDER BY foo.order_field DESC)  |
        """
        if not expressions:
            expressions = []
            if partition_by is not None:
                expressions.append(Clause('PARTITION BY', partition_by))
            if order_by is not None:
                expressions.append(Clause('ORDER BY', order_by))
        as_ = WrappedClause('AS', *expressions)
        return self.state.add(Clause('WINDOW', safe(alias), as_))

    def join(self, a=None, b=None, on=None, using=None, type="", alias=None):
        """ Sets a |JOIN| clause in the :prop:state of type @type

            @a: (:class:Model, :class:Expression or :class:Field)
            @b: (:class:Field) to |JOIN| @a to
            @on: (:class:Expression or #tuple) of
                :class:bloom.Expression(s) |JOIN| @a |ON|
            @using: (:class:Field or #tuple) of :class:Field objects, sets a
                |USING| clause for the |JOIN| using the given fields.

            -> @self
            ===================================================================
            ``Usage Example``
            ..
                m1 = Model()
                m2 = SomeOtherModel()
                m1.join(m2, on=(m1.user_id == m2.id,), alias="_m2")
            ..
            |JOIN m2 _m2 ON m1.user_id = _m2.id|
        """
        self.state.join(a, b, on, using, type, alias)
        return self

    inner_join = join

    def natural_join(self, a, alias=None):
        """ Naturally joins the table in the |FROM| clause to the model @a

            @a: :class:Model object
            @alias: #str alias name for the @b model table if @b model table
                is given, otherwise @a model table in the |JOIN|

            -> @self
            ===================================================================

            ``Usage Example``
            ..
                m1 = Model()
                m2 = SomeOtherModel()
                m1.join(m2, alias="_m2")
            ..
            |FROM m1 NATURAL JOIN m2 _m2|
        """
        return self.join(a, type="NATURAL", alias=alias)

    def left_join(self, a=None, b=None, on=None, using=None, natural=False,
                  alias=None):
        """ |LEFT JOIN|
            @natural: #bool, True if you want to execute a
                |NATURAL LEFT JOIN| on @a

            :see::meth:join
        """
        jt = "LEFT INNER"
        if natural:
            jt = "NATURAL " + jt
        return self.join(a, b, on, using, jt, alias=alias)

    left_outer_join = left_join

    def right_join(self, a=None, b=None, on=None, using=None, natural=False,
                   alias=None):
        """ |RIGHT JOIN|
            :see::meth:join

            @natural: #bool, True if you want to execute a
                |NATURAL RIGHT JOIN| on @a
        """
        jt = "RIGHT OUTER"
        if natural:
            jt = "NATURAL " + jt
        return self.join(a, b, on, using, jt, alias=alias)

    right_outer_join = right_join

    def full_join(self, a=None, b=None, on=None, using=None, natural=False,
                  alias=None):
        """ |FULL JOIN|
            @natural: #bool, True if you want to execute a
                |NATURAL FULL JOIN|

            :see::meth:join
        """
        jt = "FULL"
        if natural:
            jt = "NATURAL " + jt
        return self.join(a, b, on, using, jt, alias=alias)

    full_outer_join = full_join

    def cross_join(self, a=None, b=None, on=None, using=None, natural=False,
                   alias=None):
        """ |CROSS JOIN|
            :see::meth:join
        """
        jt = "CROSS"
        if natural:
            jt = "NATURAL " + jt
        return self.join(a, b, on, using, jt, alias=alias)

    def where(self, *exps, **kwargs):
        """ Sets a |WHERE| :class:Clause in the query :prop:state for
            :class:SELECT, :class:UPDATE, and :class:DELETE queries

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self

            ===================================================================
            ``Usage Example``

            Groups results by the field 'user_id'
            ..
                m = Model()
                m.where(
                    (m.name >> ['Ringo Starr', 'Paul McCartney']) &
                    ((m.band_name == 'Beatles') |
                     (m.band_name == 'Ringo Starr & His All-Starr Band'))
                )
                m.select()
            ..
            |WHERE model_table.name IN ('Ringo Starr', 'Paul McCartney')    |
            |AND (model_table.band_name = 'Beatles'                         |
            |OR model._table.band_name = 'Ringo Starr & His All-Starr Band')|

            ..
                m.where(1, 1)
            ..
            |WHERE 1 AND 1|
        """
        if exps or kwargs:
            self.state.add(Clause("WHERE", *exps, join_with=' AND ', **kwargs))
        return self

    filter = where

    def into(self, table, alias=None):
        """ Sets an |INTO| :class:Clause in the query :prop:state for
            :class:INSERT queries.

            @table: (#str) name of the table to insert into

            -> @self

            ===================================================================
            ``Usage Example``
            ..
                m = ORM()
                m.into('foo_table')
                m.insert()
            ..
            |INSERT INTO foo_table|
        """
        if table is not None:
            self.state.add(CommaClause("INTO", safe(table), alias=alias))
        else:
            raise ValueError("INTO clause cannot be 'None'")
        return self

    def values(self, *vals):
        """ Sets a |VALUES| :class:Clause in the query :prop:state for
            :class:INSERT queries.

            @*vals: one or several values to set

            -> @self

            ===================================================================
            ``Usage Example``
            ..
                m = Model()
                m.values(1, 2, 3, 4)
                m.insert(m.f1, m.f2, m.f3, m.f4)
            ..
            |(f1, f2, f3, f4) VALUES (1, 2, 3, 4)|

            ..
                m = Model()
                m.values(1, 2, 3, 4)
                m.values(5, 6, 7, 8)
                m.insert(m.f1, m.f2, m.f3, m.f4)
            ..
            |(f1, f2, f3, f4) VALUES (1, 2, 3, 4), (5, 6, 7, 8)|
        """
        def _do_insert(field):
            try:
                if field._should_insert():
                    return field.value
                else:
                    return safe('DEFAULT')
            except AttributeError:
                return field

        self.state.add(ValuesClause("VALUES ", *map(_do_insert, vals)))
        return self

    def set(self, *exps, **kwargs):
        """ Sets a |SET| :class:Clause in the query :prop:state for
            :class:UPDATE queries.

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self

            ===================================================================
            ``Usage Example``

            Increments the field 'views' in the model by one
            ..
                m = Model()
                m.where(m.uid == 700)
                m.set(m.views == (m.views + 1))
                m.update()
            ..
            |SET model_table.views = model_table.views + 1|

            Sets the field `views` in the model to its real value
            ..
                m['views'] = 14
                m.set(m.views)
            ..
            |SET model_table.views = 14|
        """
        def _do_update(field):
            try:
                if field._should_update():
                    return field.eq(field.value)
            except AttributeError:
                return field

        exps = filter(lambda x: x is not None,  map(_do_update, exps))
        self.state.add(CommaClause("SET", *exps, use_field_name=True,
                                   **kwargs))
        return self

    def group_by(self, *exps, **kwargs):
        """ Sets a |GROUP BY| :class:Clause in the query :prop:state for
            :class:SELECT queries.

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self

            ===================================================================
            ``Usage Example``

            Groups results by the field 'user_id'
            ..
                m = Model()
                m.group_by(m.user_id)
                m.select()
            ..
        """
        self.state.add(CommaClause("GROUP BY", *exps, **kwargs))
        return self

    def order_by(self, *exps, **kwargs):
        """ Sets an |ORDER BY| :class:Clause in the query :prop:state for
            :class:SELECT queries.

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self

            ===================================================================
            ``Usage Example``

            Orders by the model field 'username' ASC
            ..
                m = Model()
                m.order_by(m.username.asc())
                m.select()
            ..
        """
        self.state.add(CommaClause("ORDER BY", *exps, **kwargs))
        return self

    def asc(self, exp):
        """ Sets an :meth:order_by @val |ASC| :class:Clause in the query
            :prop:state

            -> @self
        """
        self.order_by(Expression(exp, "ASC"))
        return self

    def desc(self, exp):
        """ Sets an :meth:order_by @val |DESC| :class:Clause in the query
            :prop:state

            -> @self
        """
        self.order_by(Expression(exp, "DESC"))
        return self

    def limit(self, limit_offset, limit=None):
        """ Sets a |LIMIT| :class:Clause in the query :prop:state for
            :class:SELECT queries, and optionally an |OFFSET| if @limit2 is
            defined.

            @limit_offset: #int number of records to limit the query to if
                @limit is not defined, otherwise an #int number of records to
                offset the query by
            @limit: #int number of records to limit the query by if
                @limit_offset is defined.

            -> @self

            ===================================================================
            ``Usage Example``

            Sets the LIMIT to 10
            ..
                m = Model()
                m.limit(10)
                m.select()
            ..

            =========================================
            Sets the LIMIT to 10 and the OFFSET to 50
            ..
                m = Model()
                m.limit(50, 10)
                m.select()
            ..
        """
        clause = CommaClause("LIMIT", limit_offset if limit is None else limit)
        self.state.add(clause)
        if limit:
            self.offset(limit_offset)
        return self

    def fetch(self, type="NEXT", count=None, direction=None):
        """ Sets a |FETCH| :class:Clause in the query :prop:state for
            :class:SELECT queries. The @type and @direction arguments
            are not parameterized and should not depend on user input.

            @type: (#str) see [Postgres Docs](
                http://www.postgresql.org/docs/9.1/static/sql-fetch.html)
                for all types
            @count: (#int) determining the location or number of rows to fetch.
                For |FORWARD| and |BACKWARD| cases, specifying a negative count
                is equivalent to changing the sense of |FORWARD| and |BACKWARD|
            @direction: (#str) see [Postgres Docs](
                http://www.postgresql.org/docs/9.1/static/sql-fetch.html)
                for all types

            -> @self
        """
        args = []
        if count:
            args.append(count)
        if direction:
            args.append(safe(direction))
        c = Clause(type, *args)
        self.state.add(Clause("FETCH", c))
        return self

    def offset(self, offset):
        """ Sets an |OFFSET| :class:Clause in the query :prop:state for
            :class:SELECT queries

            @offset: #int number of records to offset the query by

            -> @self
        """
        self.state.add(CommaClause("OFFSET", offset))
        return self

    def one(self):
        """ Tells the ORM to |fetchone| result from the query as opposed to
            |fetchall|.

            -> @self
        """
        self.state.one = True
        return self

    def page(self, page=1, limit=25):
        """ Tells the ORM to return a @page of results from the table. The
            |OFFSET| is calculated by multiplying @page by @limit and
            subtracting @limit, and returns @limit results.

            |OFFSET| and |LIMIT| :class:Clause(s) are added to the
            query :prop:state.

            @page: #int page number
            @limit: #int max results to return

            -> @self

            ===================================================================
            ``Usage Example``

            Sets an OFFSET of 450 |(10*50)-50| and a LIMIT of 50
            ..
                m = Model()
                m.page(10, 50)
                m.select()
            ..
        """
        limit, page = int(limit), int(page)
        self.limit((page * limit) - limit, limit)
        return self

    def having(self, *exps, **kwargs):
        """ Sets a |HAVING| :class:Clause in the query :prop:state

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(CommaClause("HAVING", *exps, **kwargs))
        return self

    def for_update(self, *exps, **kwargs):
        """ Sets a |FOR UPDATE| :class:Clause in the query :prop:state

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(Clause("FOR", Clause("UPDATE"), *exps, **kwargs))
        return self

    def for_share(self, *exps, **kwargs):
        """ Sets a |FOR SHARE| :class:Clause in the query :prop:state

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(Clause("FOR", Clause("SHARE"), *exps, **kwargs))
        return self

    def returning(self, *exps, **kwargs):
        """ Sets a |RETURNING| :class:Clause in the query :prop:state

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        exps = list(filter(lambda x: x is not None, exps))
        try:
            if "".join(exps) == "*":
                exps[0] = safe("*")
        except TypeError:
            pass
        exps = exps or (safe("*"),)
        self.state.add(CommaClause("RETURNING", *exps, **kwargs))
        return self

    def on(self, *exps, join_with='AND', **kwargs):
        """ Sets a |ON| :class:Clause in the query :prop:state

            @*exps: :mod:bloom.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(Clause("ON", *exps, join_with=join_with, **kwargs))
        return self

    # ``Query execution``

    def run_iter(self, *queries, fetch=False):
        """ Runs all of the queries in @*qs or in :prop:queries,
            fetches all of the results if there are any and :prop:_dry
            is |False|

            @*queries: (:class:bloom.Query) one or several objects
            @fetch: (#bool) True if all query results should be automatically
                fetched with :meth:psycopg2.extensions.cursor.fetchall

            -> yields results of the query if @fetch is True otherwise
                yields the cursor from each query
        """
        #: Do not commit if it is a multi query
        multi = self._multi
        commit = False if multi else True
        #: Prepare the state if the this is an implicit run
        if not queries:
            #: Implicitly running compiles and closes multi/many queries
            if self._many:
                self.queries[-1].compile()
            self._multi = False
        #: Gets the client connection from a pool or client object
        conn = self.db.get()
        for q in queries or self.queries:
            #: Executes the query with its parameters
            try:
                result = self.execute(q.query,
                                      q.params,
                                      commit=commit,
                                      conn=conn)
            except QueryError:
                if not queries:
                    self.queries.remove(q)
                    self.reset_state()
                raise
            if fetch:
                #: Fetches 'all' by default
                fetch_type = 'fetchall'
                if q.one:
                    #: Fetches one result if 'one' specified in the :clas:Query
                    fetch_type = 'fetchone'
                try:
                    #: Fetches the result
                    result = result.__getattribute__(fetch_type)()
                except psycopg2.ProgrammingError:
                    #: No results to fetch
                    pass
            yield result

        self._get_reset_method(multi, queries, conn)

    def _get_reset_method(self, multi, queries, conn):
        if queries:
            #: Explicit 'run', the user is in control of everything except
            #  for the query state and client connection
            self.reset_state()
        else:
            #: Implicit 'run', the ORM is in control
            self.reset(multi=True)
            if multi and not self.db.autocommit:
                try:
                    conn.commit()
                except psycopg2.ProgrammingError as e:
                    conn.rollback()
                    raise QueryError(e.args[0].strip())
        self.db.put(conn)

    def run(self, *queries):
        """ Runs all of the queries in @*qs or in :prop:queries,
            fetches all of the results if there are any

            @*queries: (:class:bloom.Query) one or several

            -> #list of results if more than one query is executed, otherwise
                the cursor factory
        """
        results = [result for result in self.run_iter(*queries, fetch=True)]
        if len(results) == 1:
            return results[0]
        else:
            return results

    def execute(self, query, params=None, commit=True, conn=None):
        """ Executes @query with @params in the cursor.
            If the client isn't configured to autocommit and @query
            isn't part of a :meth:multi query, it will be commited
            here.  If an exception is raised, the query will automatically
            be rolled back in that case.

            @query: (#str) query string
            @params: (#tuple|#dict|#list) of params referenced in @query
                with |%s| or |%(name)s|
            @commit: (#bool) True to commit transaction
            @conn: (:class:Postgres|:class:PostgresPoolConnection) if
                a connection object is provided, it is your responsibility
                to put the connection if it is a part of a pool.

            -> :mod:psycopg2 cursor or None
        """
        params = params or tuple()
        _conn = conn
        #: Gets a client connection if one wasn't passed as an argument
        if _conn is None:
            conn = self.db.get()
        cursor = conn.cursor(schema=self.schema,
                             model=self,
                             cursor_factory=self._cursor_factory)
        self.debug(cursor, query, params)
        #: Sets the cursor search path to the current schema
        try:
            #: Executes the cursor
            cursor.execute(query, params)
            #: Commits if the client connection is not set to autocommit
            #  and the 'commit' argument is true
            if commit and not conn.autocommit:
                conn.commit()
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError,
                psycopg2.DataError, psycopg2.InternalError) as e:
            #: Rolls back the transaction in the event of a failure
            conn.rollback()
            raise QueryError(e.args[0].strip())
        #: Puts a client connection away if it is a pool and no connection
        #  was passed in arguments. If a connection object is passed,
        #  it's the user's responsibility to put it away.
        if _conn is None:
            self.db.put(conn)
        return cursor

    def subquery(self, alias=None):
        """ Parses the query :prop:state as a subquery. This query will not be
            executed and can be passed around like other
            :class:bloom.expressions.

            Also see: :class:Subquery

            -> @self
            =================================================================
            ``Usage Example``
            ..
                m = Model()
                m.subquery()
                subquery = m.select(m.user_id.max())

                m.where(m.user_id == subquery)
                m.select()
            ..
        """
        self.state.is_subquery = True
        self.state.alias = alias
        return self

    #  `` Query chaining ``

    def multi(self, *queries):
        """ Starts chaining multiple queries - queries which will be executed
            atomically in the future in one transaction. This cannot be used
            if your (:class:Postgres) is set to |autocommit|

            @queries: (:class:Query) querys to add to the multi queue

            -> @self
            =================================================================
            ``Usage Example``
            ..
                my_model.multi()  # Initializes the multi query
                my_model['username'] = 'foo'
                my_model['password'] = 'bar'
                my_model.insert()  # Does not execute query,
                                   # appends it to the multi query
                my_model['username'] = 'foo'
                my_model['password'] = 'bar'
                my_model.select()  # Does not execute query,
                                   # appends it to the multi query
                my_model.run()  # Runs the multi query
            ..
        """
        self._multi = True
        if queries:
            self.add_query(*queries)
        return self

    def many(self):
        """ Starts a 'many' INSERT query statement
            i.e. |INSERT INTO foo (bar) VALUES (foobar,), (foobar2,),
                (foobar3,)|

            -> @self
            =================================================================
            ``Usage Example``
            ..
                my_model.many()  # Initializes the many query
                my_model['username'] = 'foo'
                my_model['password'] = 'bar'
                my_model.insert()  # Does not execute query,
                                   # appends it to the many query
                my_model['username'] = 'foo2'
                my_model['password'] = 'bar2'
                my_model.insert()  # Does not execute query,
                                   # appends it to the many query
                my_model.run()     # Runs the many query
            ..
        """
        self._many = 'BEGIN'
        return self

    def end_many(self):
        """ Turns off |many()| without having to run the query.
            ..
                # Adds many query to the query stack
                self.orm.many()
                self.orm.insert(f1, f2)
                self.orm.insert(f1, f2)
                self.orm.end_many()
                # Adds another many query to the query stack
                self.orm.many()
                self.orm.insert(f1, f2)
                self.orm.insert(f1, f2)
                self.orm.end_many()
            ..
            -> :class:Query
        """
        self.queries[-1].compile()
        self.reset_many()
        return self.queries[-1]

    # ``Query State``

    def add_query(self, *queries):
        """ Adds a @query to the :meth:run queue """
        self.queries.extend(queries)
        return self

    def remove_query(self, query):
        """ Removes a @query from the :meth:run queue """
        self.queries.remove(query)
        return self

    @property
    def state(self):
        """ -> :class:QueryState object """
        return self._state

    def reset_dry(self):
        """ Resets the dry option """
        self._dry = False
        return self

    def reset_naked(self):
        """ Resets the naked option """
        self._naked = None
        return self

    def reset_state(self):
        """ Resets the :prop:state object """
        self.state.reset()
        return self

    def reset_many(self):
        """ Removes many mode from the ORM state """
        self._many = False
        return self

    def reset_multi(self):
        """ Removes multi mode from the ORM state """
        self._multi = False
        self.reset_dry()
        self.reset_naked()
        self.queries = []
        self.reset_state()
        return self

    def reset(self, multi=False):
        """ Resets the query :prop:state, many mode and optionally
            multi-mode if @multi is set to |True|
        """
        self._with = False
        self.reset_many()
        if not self._multi:
            self.queries = []
            self.reset_dry()
            self.reset_naked()
        if multi:
            self.reset_multi()
        self.reset_state()
        return self

    def debug(self, cursor, query, params):
        """ Prints query information including :prop:state,
            :prop:params, and :prop:queries
        """
        if self._debug:
            line('—')
            logg().warning("Bloom Query")
            line('—')
            l = logg(query, params)
            l.log("Parameterized", force=True)
            mog = cursor.mogrify(query, params).decode()
            l = logg(mog)
            l.log("Mogrified", force=True)
            line('—')

    def copy(self, *args, **kwargs):
        cls = copy.copy(self)
        cls.queries = self.queries.copy()
        if self.state:
            cls._state = self.state.copy()
        return cls


class QueryState(object):
    """`Query State`
        Manages the ORM clauses, joins, parameters and subqueries.
    """
    __slots__ = ('orm', 'clauses', 'params', 'alias', 'one', 'is_subquery',
                 '_join')
    _multi_clauses = {'VALUES', 'JOIN'}

    def __init__(self, orm):
        self.orm = orm
        self._join = Joins(orm)
        self.clauses = OrderedDict()
        self.params = {}
        self.one = False
        #: Subqueries
        self.alias = None
        self.is_subquery = False

    @prepr('clauses', 'params', _no_keys=True)
    def __repr__(self): return

    def __iter__(self):
        """ Iterating the query state yields it's clauses"""
        for v in self.clauses.values():
            yield v

    def reset(self):
        """ Resets the query state """
        self.clauses = OrderedDict()
        self.params = {}
        #: Subqueries
        self.alias = None
        self.one = False
        self.is_subquery = False
        return self

    def has(self, name):
        """ Determines if the state has a clause with @name
            @name: (#str) Name of the :class:Clause to search for
        """
        name = name.upper().strip()
        return name in self.clauses

    __contains__ = has

    def get(self, name, default=None):
        """ Gets the clauses with @name within the state, defaulting to
            @default.

            @name: (#str) name of the clause to find
            @default: default value to returning

            -> :class:Clause if @name is not 'JOIN' or 'VALUES', otherwise
                a #list of :class:Clause objects will be returned
        """
        name = name.upper().strip()
        multi = name in self._multi_clauses
        if multi:
            return self.clauses.get(name, default) or []
        else:
            return self.clauses.get(name, default)

    def pop(self, name, default=None):
        """ Gets the clauses with @name within the state, defaulting to
            @default and removes the clause from the state.

            @name: (#str) name of the clause to find
            @default: default value to returning

            -> :class:Clause if @name is not 'JOIN' or 'VALUES', otherwise
                a #list of :class:Clause objects will be returned
        """
        name = name.upper().strip()
        multi = name in self._multi_clauses
        if multi:
            return self.clauses.pop(name, default or [])
        else:
            return self.clauses.pop(name, default)

    def add(self, *clauses):
        """ Adds :class:Clause(s) to the :class:ORM query state
            @*clauses: (:class:Clause)
        """
        for clause in clauses:
            clause_name = clause.clause
            if 'JOIN' in clause_name:
                clause_name = 'JOIN'
            self.params.update(clause.params)
            if clause_name in self._multi_clauses:
                #: VALUES & JOIN clauses get appended
                if clause_name in self.clauses:
                    self.clauses[clause_name].append(clause)
                else:
                    self.clauses[clause_name] = [clause]
            elif clause_name == 'WHERE' and clause_name in self.clauses:
                #: WHERE clauses get extended
                _clause = self.clauses[clause_name]
                _clause.args = list(_clause.args)
                _clause.args.extend(clause.args)
                _clause.compile()
            else:
                #: Overwrite
                self.clauses[clause_name] = clause

    def join(self, *args, **kwargs):
        """ :see::meth:ORM.join """
        self.add(self._join(*args, **kwargs))
        return self

    def copy(self, *args, **kwargs):
        cls = copy.copy(self)
        cls.clauses = self.clauses.copy()
        cls.params = self.params.copy()
        return cls


class Joins(object):
    __slots__ = ('orm',)

    def __init__(self, orm):
        """ :see::meth:ORM.join """
        self.orm = orm

    def expression_on_alias(self, exp, join_table, alias):
        """ Recursively sets table aliases (@alias) on :class:Field objects if
            the table of the field matches @join_table within the @exp
            expression.
        """
        try:
            if exp.left.table == join_table:
                exp.left = exp.left.copy()
                exp.left.set_alias(table=alias)
                exp.left = aliased(exp.left)
        except AttributeError:
            self.expression_on_alias(exp.left, join_table, alias)
        try:
            if exp.right.table == join_table:
                exp.right = exp.right.copy()
                exp.right.set_alias(table=alias)
                exp.right = aliased(exp.right)
        except AttributeError:
            self.expression_on_alias(exp.right, join_table, alias)
        return exp

    def with_on(self, a, on, alias):
        """ An 'on' expression was given, so we can use that expression
            to join the 'a' table.
        """
        table = a.table
        if not isinstance(on, Expression):
            if alias:
                on = [
                    self.expression_on_alias(o, table, alias)
                    for o in on]
            on = Clause('ON', *on)
        else:
            if alias:
                self.expression_on_alias(on, table, alias)
            on = Clause('ON', on)
        return table, on

    def with_field(self, a, alias):
        """ Only a :class:Field object was given as the |JOIN| context,
            so we search the field for matching field names and matching
            field types. The best guess is returned. If no guess is found,
            a :class:ValueError is raised.
        """
        table = a.table
        on = None
        if hasattr(a, 'ref'):
            on = a.eq(a.ref.field)
        elif hasattr(self.orm, 'fields') and a.index or a.unique or a.primary:
            named_match = None
            typed_match = None
            for field in self.orm.best_indexes:
                if field.field_name == a.field_name:
                    named_match = field
                    break
                elif isinstance(field, a.__class__) and \
                        typed_match is None and is_index:
                    typed_match = field_name
            on = a.eq(named_match or typed_match)
        if on is None:
            raise ValueError(
                'Could not find join field in `{}` for ' +
                'field `{}`'.format(table, a.name))
        if alias:
            on = self.expression_on_alias(on, table, alias)
        return table, Clause('ON', on)

    def with_fields(self, a, b, alias):
        """ 'a' and 'b' are given, they are assumed to be related fields """
        table = b.table if a.table == self.orm.table else a.table
        if alias:
            on = self.expression_on_alias(a.eq(b), table, alias)
        else:
            on = a.eq(b)
        return table, Clause('ON', on)

    def with_model(self, a, alias):
        """ The only context given was a :class:Model. The model is searched
            for the local model in a :class:Relationship or :class:ForeignKey.
            If none are found, the best field guess is used based on
            :meth:with_field.
        """
        for fkey in a.foreign_keys:
            #: Search foreign keys
            if fkey.ref.model.table == self.orm.table:
                return self.with_field(fkey, alias)

        for relationship in a.relationships:
            #: Search relationships
            if relationship.table == self.orm.table:
                return self.with_fields(
                    relationship.join_field, relationship.foreign_key, alias)

        for field in a.fields:
            #: Search fields
            try:
                return self.with_field(field, alias)
            except ValueError:
                pass
        raise ValueError('Could not find join field for ' +
                         'model `{}`'.format(a.__class__.__name__))

    def with_expression(self, a, alias):
        """ 'a' was a :class:Expression object, so we use that
            as the ON clause and the left or right field's table
            as the join table
        """

        d = 0
        x = a
        table = None
        while table is None:
            if hasattr(x.left, 'table'):
                table = x.left.table
            elif hasattr(x.right, 'table'):
                table = x.right.table
            else:
                x = x.left
            d += 1
            if d > 10:
                raise RuntimeError(
                    'Maximum expression search depth exceeded in JOIN.')
        if alias:
            a = self.expression_on_alias(a, table, alias)
        on = Clause('ON', a)
        return table, on

    def join(self, a=None, b=None, on=None, using=None, type="", alias=None):
        """ :see::meth:ORM.join """
        clause = "{} JOIN".format(type).strip()
        using = [using] if isinstance(using, Field) else using
        if using:
            #: Join 'using' the given fields of the same name
            table = using[0].table
            on = CommaClause('USING', *using, wrap=True)
            clause = Clause(clause, safe(table, alias=alias), on,
                            use_field_name=True)
        else:
            if on:
                table, on = self.with_on(a, on, alias)
            else:
                if b is not None:
                    #: 'a' and 'b' are given, they are assumed to be related
                    # fields
                    table, on = self.with_fields(a, b, alias)
                else:
                    #: Only 'a' is given, check for foreign keys and
                    # to help determine what to join. Otherwise it will attempt
                    # to join the first occurence of the same field type
                    if isinstance(a, Field):
                        table, on = self.with_field(a, alias)
                    elif isinstance(a, (Model, Relationship)):
                        #: Only a :class:Model was provided
                        table, on = self.with_model(a, alias)
                    elif isinstance(a, BaseExpression):
                        #: 'a' was a :class:Expression object, so we use that
                        # as the ON clause and the left or right field's table
                        # as the join table
                        table, on = self.with_expression(a, alias)
            clause = Clause(clause, safe(table, alias=alias), on)
        return clause

    __call__ = join


#
#  ``Models``
#


class Model(ORM):
    """ ===================================================================
        ``Usage Examples``

        Creates a new model with two fields
        ..
            from bloom import Model, Text, Int

            class MyModel(Model):
                table = 'my_model'  # If no table attribute is specified,
                                    # the model will assume your table name
                                    # is the underscored/lowercase version of
                                    # your class name. In this case, the
                                    # specified table is the same as the
                                    # autogenerated one would be
                my_id = Int(primary=True, not_null=True)
                my_text = Text(minlen=10, not_null=True)
        ..

        ===================================================================
        Adds data to the model
        ..
            my_model = MyModel(my_id=4, my_text="Hello world")
            my_model.save()
            # -> {"my_text": "Hello world", "my_id": 4}

            print(my_model['my_text'])
        ..
        |Hello world|

        ===================================================================
        Selects data from the model
        ..
            my_model = MyModel()
            my_model.where(my_model.my_id == 4)
            my_model.select(my_model.my_text)
        ..
        |[{"my_text": "Hello world"}]|

        ..
            my_model = MyModel(my_id=4)
            my_model.get()
        ..
        |{"my_text": "Hello world", "my_id": 4}|

        ===================================================================
        Updates data in the model using the :prop:best_available_index
        ..
            my_model = MyModel(my_id=4, my_text="Hello world")
            my_model.update(my_model.my_text)
        ..
        |{"my_text": "Hello world"}|

        ===================================================================
        Deletes data from the model using the :prop:best_available_index
        ..
            my_model = MyModel(my_id=4, my_text="Hello world")
            my_model.delete()
        ..
    """
    ordinal = []

    def __init__(self, client=None, cursor_factory=None, naked=None,
                 schema=None, debug=False, **kwargs):
        """ `Basic Model`
            A basic model for Postgres tables.

            :see::class:ORM
            @naked: (#bool) True to default to raw query results in the form
                of the :prop:_cursor_factory, rather than the default which
                is to return query results as copies of the current model.
        """
        if schema is None and hasattr(self, 'schema'):
            schema = self.schema
        super().__init__(client, cursor_factory=cursor_factory, schema=schema,
                         debug=debug)
        self.table = self.table or camel_to_underscore(self.__class__.__name__)
        self._fields = []
        self._relationships = []
        self._foreign_keys = []
        self._alias = None
        self._always_naked = naked or False
        self._compile()
        self.fill(**kwargs)

    @prepr('field_names', _no_keys=True)
    def __repr__(self): return

    def __str__(self):
        return self.__repr__()

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            try:
                setattr(result, k, v.copy())
            except AttributeError:
                setattr(result, k, copy.copy(v))
        result.fill(**self.to_dict())
        return result

    def __getitem__(self, name):
        """ -> value of the field at @name """
        try:
            return getattr(self, name).value
        except AttributeError:
            raise KeyError("Field `{}` not in {}".format(name, self.table))

    def __setitem__(self, name, value):
        """ Sets the field value for @name to @value """
        try:
            getattr(self, name).__call__(value)
        except AttributeError:
            raise KeyError("Field `{}` not in {}".format(name, self.table))

    def __delitem__(self, name):
        """ Deletes the field value for @name, the field remains in the model
        """
        try:
            getattr(self, name).__call__(None)
        except AttributeError:
            raise KeyError("Field `{}` not in {}".format(name, self.table))

    def __iter__(self):
        """ -> #iter of :prop:fields in the model """
        return iter(self.fields)

    def set_table(self, table):
        """ Sets the ORM table to @table (#str) """
        self.table = table
        for field in self.fields:
            field.table = table

    def _getmembers(self):
        for attr, field in {
            attr: field
            for mro in reversed(self.__class__.__mro__[:-1])
            for attr, field in mro.__dict__.items()
        }.items():
            yield attr, field

    def _compile(self):
        """ Sets :class:Field, :class:Relationship and :class:ForeignKey
            attributes
        """
        fields = {}
        relationships = {}
        foreign_keys = {}
        for field_name, field in self._getmembers():
            if isinstance(field, Field):
                fields[field_name] = field
            elif isinstance(field, Relationship):
                relationships[field_name] = field
            elif isinstance(field, ForeignKey):
                foreign_keys[field_name] = field
        self._add_field(**fields)
        self._add_foreign_key(**foreign_keys)
        self.add_relationship(**relationships)

    def _make_nt(self):
        """ Makes a :class:namedtuple object with :prop:field names
            as its field names, and stores it in self.ModelNameRecord
            i.e., if your model is named Users, self.UsersRecord
        """
        cname = self.__class__.__name__
        if not hasattr(self, cname):
            setattr(self,
                    cname + 'Record',
                    namedtuple(cname + 'Record', self.field_names))

    def naked(self):
        """ Causes the ORM to return the default cursor factory as results
            rather than copies of the model.
        """
        self._naked = True
        return self

    def new(self, **kwargs):
        """ Causes the ORM to create an exact copy of this model and reset
            this  model. Calling this will NOT add the new record to the
            database.

            @**kwargs: |field_name=field_value| pairs to optionally create
                the new model with

            ..
                new_model = model.new().where(True).get()
                new_model_b = model.new(foo=bar)
            ..
        """
        cls = self.copy().fill(**kwargs)
        self.reset(multi=True)
        return cls

    def models(self):
        """ Causes the ORM to return copies of the model rather than
            :prop:naked results. This is only useful when the default
            behavior of the model is to return naked results
        """
        self._naked = False
        return self

    def fill(self, **kwargs):
        """ Populates the model with |name=value| pairs
            @**kwargs: |field_name=field_value| pairs
        """
        field_names = self.field_names
        for k, v in kwargs.items():
            if k in field_names:
                self[k] = v
            else:
                raise KeyError("Field `{}` not in {}".format(k, self.table))
        return self

    def filter(self, *exps, **filters):
        """ @*exps: (:class:Expression(s))
            @**filters: |field_name__field_method=value| keyword argument
                filters tied to the fields within this model.
                ..
                    from bloom import Model

                    class Users(Model):
                        username = Username()

                    User = Users()
                    User.filter(username__startswith='J')
                    results = User.get()
                ..
                |SELECT * FROM users WHERE username ILIKE 'J%'|

                ..
                    User.filter(username.not_eq('Jared'),
                                username='Jared').get()
                ..
                |SELECT * FROM users       |
                | WHERE username <> 'Jared'|
                |   AND username = 'Jared' |
        """
        exps = list(exps)
        add_exp = exps.append
        for field, value in filters.items():
            field_name, *method = field.split("__")
            field = getattr(self, field_name)
            if not method:
                method = field.eq(value)
            else:
                method = getattr(field, "".join(method))(value)
            add_exp(method)
        return self.where(*exps)

    def to_json(self, *args, **kwargs):
        """ Returns a JSON dump'd #str of the state of the current model
            with all field: value pairs included.

            @*args and @**kwargs are passed to :func:dumps
        """
        return dumps(self.to_dict(), *args, **kwargs)

    def from_json(self, val):
        """ JSON loads @val into the current model.
            -> self
        """
        self.fill(**loads(val))
        return self

    def to_dict(self):
        """ -> #dict of |{field_name: field_value}| pairs within the model """
        return {field.field_name: field() for field in (self.fields or [])}

    def to_namedtuple(self):
        """ -> :func:namedtuple representation of the model """
        if not hasattr(self, self.__class__.__name__ + 'Record'):
            self._make_nt()
        return getattr(self, self.__class__.__name__ + 'Record')(
            **self.to_dict())

    def from_namedtuple(self, nt):
        """ -> self """
        field_names = self.field_names
        getattr_ = nt.__getattribute__
        for field in nt._fields:
            if field in field_names:
                self[field] = getattr_(field)
            else:
                raise KeyError('Field `{}` not found in `{}`'.format(
                               field, self.table))
        return self

    def set_alias(self, alias=None):
        """ Sets table aliases on all fields in the model and on the model
            itself.

            :see::meth:Field.set_alias
        """
        self._alias = alias
        for field in self.fields:
            field.set_alias(table=alias)

    def _register_field_type(self, field):
        if hasattr(field, 'register'):
            self.db.after('connect', field.register)
        elif hasattr(field, 'type') and hasattr(field.type, 'register'):
            self.db.after('connect', field.type.register)

    def _add_field(self, **fields):
        """ Adds one or several @fields to the model.
            @**fields: |field_name=<Field>| keyword arguments

            ..
                model = Model()
                model.add_field(city=Text())
            ..
        """
        add_field = self._fields.append
        sa = self.__setattr__
        for field_name, field in fields.items():
            field = field.copy()
            field.table = self.table
            field.field_name = field_name
            self._register_field_type(field)
            sa(field_name, field)
            add_field(field)
        self._order_fields()

    def _add_foreign_key(self, **foreign_keys):
        """ Adds one or several @foreign_keys to the model
            @**relationships: (:class:bloom.ForeignKey) keyword
                arguments |field_name=ForeignKey|
        """
        for name, foreign_key in foreign_keys.items():
            foreign_key = foreign_key.forge(self, name)

    def add_relationship(self, **relationships):
        """ Adds one or several @relationships to the model
            @**relationships: (:class:bloom.Relationship) keyword
                arguments |rel_name=Relationship|
        """
        for name, relationship in relationships.items():
            relationship = relationship.copy()
            relationship.forge(self, name)
        return self

    def approx_size(self, alias=None):
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
        q = Select(self, safe('reltuples::bigint %s' % (alias or "count")))
        result = q.execute().fetchone()
        self.reset_naked()
        if alias is None:
            try:
                result = result.count
            except AttributeError:
                result = result['count']
        return result

    def exact_size(self, alias=None):
        """ Performs a |COUNT| query on the :prop:best_index in the
            model, so in most cases the primary key. Respects |WHERE| clauses
            within the query state.

            |SELECT COUNT(primary_key) FROM table_foo|

            @alias: #str alias name to label the 'count' as

            -> #int number of results counted if no alias is given, otherwise
                the raw cursor factory result
        """
        result = self.one().naked()._select(self.best_index.count(alias=alias))
        if alias is None:
            try:
                result = result.count
            except AttributeError:
                result = result['count']
        return result

    def _order_fields(self, ordinal=None):
        if ordinal or self.ordinal:
            self._fields = [self.__getattribute__(name)
                            for name in self.ordinal]
        return self._fields

    @property
    def fields(self):
        """ -> yields the fields in the model based on :desc:__dict__ or
                :attr:ordinal if it exists
        """
        return self._fields

    @cached_property
    def field_names(self):
        """ -> #tuple of all field names (without the table name) within the
                model
        """
        return tuple(field.field_name for field in self.fields)

    @cached_property
    def names(self):
        """ -> #tuple of all the full field names, table included """
        return tuple(field.name for field in self.fields)

    @property
    def field_values(self):
        """ -> #tuple of all field values within the model """
        return tuple(field.value for field in self.fields)

    @property
    def relationships(self):
        """ -> yields all :class:Relationship(s) within the model """
        return self._relationships

    @cached_property
    def foreign_keys(self):
        """ -> yields all :class:ForeignKeyField(s) fields within the model """
        return self._foreign_keys

    @cached_property
    def indexes(self):
        """ -> #tuple of all indexes within the model """
        return tuple(field for field in self.fields if field.index)

    @cached_property
    def plain_indexes(self):
        """ -> #tuple of all plain indexes within the model
                (not unique, foreign or primary keys)
        """
        return tuple(field for field in self.fields
                     if field.index and not field.unique)

    @cached_property
    def unique_indexes(self):
        """ -> #tuple of all unique indexes within the model """
        return tuple(field for field in self.indexes if field.unique)

    @cached_property
    def unique_fields(self):
        """ -> #tuple of all unique indexes within the model """
        return tuple(field for field in self.fields if field.unique)

    @cached_property
    def primary_key(self):
        """ -> the model's primary key as :class:Field or
                #tuple of :class:Field if the primary key is spanned
        """
        primaries = tuple(field for field in self.fields if field.primary)
        if len(primaries) == 1:
            return primaries[0]
        if not primaries:
            return None
        return primaries

    @cached_property
    def best_indexes(self):
        """ -> #tuple of best indexes in the model in order, first best and
                worst last
        """
        indexes = []
        add_index = indexes.append
        if self.primary_key is not None:
            if isinstance(self.primary_key, tuple):
                for key in self.primary_key:
                    add_index(key)
            else:
                add_index(self.primary_key)
        better, worse = [], []
        add_better, add_worse = better.append, worse.append
        betters = category.NUMERIC.union({TIME, TIMESTAMP, DATE, BOOL})
        for index in self.unique_indexes:
            if index.OID in category.INTS:
                add_index(index)
            elif index.OID in betters:
                add_better(index)
            else:
                add_worse(index)
        indexes.extend(index for index in better + worse)
        better.clear()
        worse.clear()
        for index in self.plain_indexes:
            if index.OID in category.INTS:
                add_index(index)
            elif index.OID in betters:
                add_better(index)
            else:
                add_worse(index)
        indexes.extend(index for index in better + worse)
        return tuple(indexes)

    @property
    def best_index(self):
        """ Looks for the best index in the model, looking for a primary key,
            followed by unique keys, followed by any index

            -> :class:bloom.Field object
        """
        return self.best_indexes[0]

    @property
    def best_available_index(self):
        """ Looks for the best available index in the model where the index
            field has a value, looking for a primary key, followed by unique
            keys, followed by any index

            -> :class:bloom.Expression object |index_field == index_value|
        """
        _zero = {0, 0.0}

        def validate(index):
            return (index.value or index.value in _zero) and index.validate()

        def indexable(index):
            if isinstance(index, tuple):
                for key in index:
                    if not validate(key):
                        return False
                return True
            else:
                return validate(index)

        # Primary Keys
        if self.primary_key is not None and indexable(self.primary_key):
            if not isinstance(self.primary_key, tuple):
                return self.primary_key.eq(self.primary_key.value)
            else:
                exp = self.primary_key[0].eq(self.primary_key[0].value)
                for key in self.primary_key[1:]:
                    exp = exp.and_(key.eq(key.value))
                return exp

        # Unique Indexes
        _unique_index = None
        for index in list(self.unique_indexes) + self.foreign_keys:
            if indexable(index):
                exp = index.eq(index.value)
                if _unique_index:
                    _unique_index = _unique_index.and_(exp)
                else:
                    _unique_index = exp

        if _unique_index:
            return _unique_index

        # Plain indexes
        _index = None
        for index in self.indexes:
            if indexable(index):
                exp = index.eq(index.value)
                if _index:
                    _index = _index.and_(exp)
                else:
                    _index = exp

        return _index

    @property
    def best_unique_index(self):
        """ Looks for the best available index in the model where the index
            field has a value, and the index is unique.

            -> :class:bloom.Expression object |index_field == index_value|
        """
        best_available = self.best_available_index
        if best_available is not None:
            best_available_ = best_available.left
            try:
                while True:
                    best_available_ = best_available_.left
                    if isinstance(best_available_, Field):
                        break
            except AttributeError:
                pass
            if best_available_.unique or best_available_.primary:
                return best_available
        return None

    def reset_fields(self):
        """ Sets all of the :class:Field values within the model to their
            defaults.
        """
        for field in self.fields:
            field.clear()
        return self

    def reset_relationships(self):
        """ Resets all :class:Relationship(s) within the model """
        for relationship in self.relationships:
            relationship.clear()

    def clear(self):
        """ Sets all of the :class:Field values within the model to |None|,
            resets the :prop:state.
        """
        self.reset()
        self.reset_fields()
        self.reset_relationships()
        return self

    def _is_naked(self):
        return self._naked if self._naked is not None else self._always_naked

    def run_iter(self, *queries, **kwargs):
        """ :see::meth:ORM.run_iter
            -> yields :prop:_cursor_factory if this isn't a :meth:many query
                or :prop:_naked is |True|, otherwise returns :class:Model
        """
        multi = self._multi
        if multi:
            self.new()
        for x in ORM.run_iter(self, *queries, **kwargs):
            yield x

    def run(self, *queries):
        """ :see::meth:ORM.run

            -> #list of raw :prop:_cursor_factory query results if :prop:_naked
                is |True|, otherwise :class:Model
        """
        results = [result
                   for result in self.run_iter(*queries, fetch=True)]
        if len(results) == 1:
            return results[0]
        else:
            return results

    def add(self, **kwargs):
        """ Adds a new record to the DB without affecting the current model.
            @**kwargs: |field=value| pairs to insert

            -> result of INSERT query
        """
        fields = []
        add_field = fields.append
        for name, val in kwargs.items():
            field = getattr(self, name).copy()
            field(val)
            add_field(field)
        return self.returning().one().new().insert(*fields)

    def save(self, *fields, **kwargs):
        """ Inserts a single record into the DB if it doesn't already exist
            and a unique key is not empty, otherwise updates the record in
            the DB with the current model values

            @*fields: optionally only save specified :class:Field objects
                during updates

            -> self
        """
        best_index = self.best_unique_index
        if best_index is not None:
            if not self.unique_indexes and self.primary_key is not None\
               and self.primary_key.value in {None, self.primary_key.empty}:
                raise ORMIndexError("Could not find a unique index in `{}`"
                                    .format(self.table))
            exists = self.copy().reset_dry().naked().where(best_index).get()
            if exists:
                #: UPDATE
                return self.one().update(*fields, **kwargs)
        #: INSERT
        return self.insert(*fields, **kwargs)

    upsert = save

    def insert(self, *fields, **kwargs):
        """ @fields: (:class:Field) objects to insert. If none are given, all
                fields with user-defined values in the model will be used.

            See also: :meth:_insert
        """
        if not self.state.has('RETURNING'):
            self.returning(*fields)
        if not self.state.one and not self._many and not self._multi:
            self.one()
        fields = fields or filter(lambda x: x.value is not x.empty,
                                  self.fields)
        return self._insert(*fields)

    def select(self, *fields, **kwargs):
        """ Gets a single record from the DB based on the
            :prop:best_available_index within the current model if no
            |WHERE| clauses is already specified, otherwise obeys the
            |WHERE| clause in the :prop:state.

            See also: :meth:_select

            -> #list of results of query as copies of this :class:Model if
                :prop:_naked is |False|, otherwise the #list will be of
                :prop:_cursor_factory. Will return :class:Select objects
                if :prop:_dry is |True|.
        """
        if not self.state.has('WHERE'):
            self.where(self.best_available_index or True)
        return self._select(*fields, **kwargs)

    def get(self, *fields, **kwargs):
        """ Gets a single record from the DB based on the
            :prop:best_available_index if no |WHERE| clause has been
            specified and fills the current model if :prop:_naked is |False|
            and :prop:dry is |False|.

            See also: :meth:select

            -> |self| if :prop:_naked is false, otherwise will return
                :prop:_cursor_factory. Will return :class:Select object
                if :prop:_dry is |True|.
        """
        return self.one().select(*fields, **kwargs)

    get_one = get

    def _explicit_where(self):
        if not self.state.has('WHERE'):
            best_index = self.best_unique_index
            if best_index is not None:
                self.where(best_index)
            else:
                raise ORMIndexError('WHERE clauses on DELETE queries must ' +
                                    'be explicit. No WHERE clause was found ' +
                                    'and no unique index was found.')

    def delete(self, *args, **kwargs):
        """ :see::meth:_delete

            Deletes records from the DB based on the
            :prop:best_unique_index within the current model if no
            |WHERE| clause has been specified.

            -> |self| if :prop:_naked is false, otherwise will return
                :prop:_cursor_factory or the cursor object if no |RETURNING|
                clause was given. Will return :class:Delete object
                if :prop:_dry is |True|.
        """
        self._explicit_where()
        if not self.state.has('RETURNING'):
            self.returning()
        return self._delete(*args, **kwargs)

    def remove(self, *args, **kwargs):
        """ :see::meth:_delete

            Deletes one record from the DB based on the
            :prop:best_unique_index within the current model if no
            |WHERE| clause has been specified.

            -> :meth:clear(ed) self
        """
        naked = self._is_naked()
        result = self.one().delete(*args, **kwargs)
        if not naked:
            return self.clear()
        return result

    def pop(self, *args, **kwargs):
        """ :see::meth:_delete

            Deletes one record from the DB based on the
            :prop:best_unique_index within the current model if no
            |WHERE| clause has been specified. Sets a |RETURNING *|
            clause.

            -> a copy of |self| populated with the deleted field info
        """
        return self.new().one().delete(*args, **kwargs)

    def update(self, *fields, **kwargs):
        """ Updates @fields within records from the DB based on the
            :prop:best_unique_index within the current model.

            @fields: (:class:bloom.Field) objects within the Model,
                if None are specified, all will be updated

            -> result of query, differs depending on cursor settings
        """
        if not self.state.has('RETURNING'):
            return_fields = filter(lambda x: isinstance(x, Field), fields)
            self.returning(*return_fields)
        self._explicit_where()
        return self._update(*fields, **kwargs)

    def pull(self, *args, dry=False, **kwargs):
        """ :see::meth:Relationship.pull """
        return [
            relationship.pull(*args, dry=dry, **kwargs)
            for relationship in self.relationships]

    def iternaked(self, offset=0, limit=0, buffer=100, order_field=None,
                  reverse=False):
        """ Yields cursor factory until there are no more results to fetch.

            @offset: (#int) cursor start position
            @limit: (#int) total number of results to fetch
            @buffer: (#int) cursor max results to fetch in one page
            @reverse: (#bool) True if returning in descending order
            @order_field: (:class:bloom.Field) object to order the
                query by
        """
        for item in self.naked().iter(offset=offset,
                                      limit=limit,
                                      buffer=buffer,
                                      order_field=order_field,
                                      reverse=reverse):
            yield item

    def iter(self, offset=0, limit=0, buffer=100, order_field=None,
             reverse=False):
        """ Yields populated models until there are no more
            results to fetch.

            @offset: (#int) cursor start position
            @limit: (#int) total number of results to fetch
            @buffer: (#int) cursor max results to fetch in one page
            @reverse: (#bool) True if returning in descending order
            @order_field: (:class:bloom.Field) object to order the
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
        q = self.dry()._select().execute()
        while True:
            results = q.fetchmany(buffer)
            if not results:
                break
            for result in results:
                yield result
        self.reset()

    def copy(self):
        """ Returns a safe copy of the model """
        cls = copy.copy(self)
        cls._fields = []
        add_field = cls._fields.append
        setattr_ = cls.__setattr__
        setattr_('db', self.db)
        setattr_('queries', self.queries.copy())
        if self._state:
            setattr_('_state', self._state.copy())
        for field in self.fields:
            field = field.copy()
            setattr_(field.field_name, field)
            add_field(field)
        for relationship in self.relationships:
            setattr_(relationship._owner_attr, relationship.copy())
        for foreign_key in self.foreign_keys:
            setattr_(foreign_key.field_name, foreign_key.copy())
        return cls


class RestModel(Model):

    def __init__(self, *args, **kwargs):
        """ `RESTful Model`
            An implementation of :class:Model with a RESTful interface. Also
            provides the :meth:patch feature.

            :see::class:Model
        """
        super().__init__(*args, **kwargs)

    def patch(self, field, **kwargs):
        """ Updates one @field in the model based on the
            :prop:best_available_index

            @field: an instance of :class:bloom.Field

            -> result of query, differs depending on cursor settings
        """
        return self.update(field, **kwargs)

    put = Model.update
    post = Model.insert
    GET = Model.get
    POST = post
    PUT = put
    PATCH = patch
    DELETE = Model.remove
