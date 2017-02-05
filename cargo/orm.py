"""

  `Cargo SQL ORM`
  ``SQL table object modelling classes``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import re
import copy
import sqlparse
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
from psycopg2.extensions import cursor as _cursor

from vital.cache import cached_property
from vital.tools.strings import camel_to_underscore
from vital.tools.lists import grouped
from vital.debug import prepr, preprX, line, logg

from cargo.clients import *
from cargo.cursors import CNamedTupleCursor, ModelCursor
from cargo.etc.types import *
from cargo.exceptions import *
from cargo.expressions import *
from cargo.statements import *
from cargo.relationships import *
from cargo.relationships import _ForeignObject
from cargo.fields import Field


__all__ = (
    "ORM",
    "QueryState",
    "Model",
    "RestModel"
)


class ORM(object):
    table = None
    schema = None

    def __init__(self, client=None, cursor_factory=None, schema=None,
                 table=None, debug=False):
        """``SQL ORM``
            ==================================================================
            The base structure which interacts with :class:Postgres
            and provides the ORM interface. This object stores and maintains
            the :class:QueryState which is parsed by the :mod:cargo.statements
            objects to create your individual SQL queries. Its methods add
            SQL :class:Clause(s) and :class:Expression(s) to your
            :class:QueryState.
            ==================================================================
            @client: (:class:Postgres or :class:PostgresPool)
            @cursor_factory: :mod:psycopg2.extras cursor factories
            @schema: (#str) the name of the schema search path
            @table: (#str) default table
            @debug: (#bool) prints mogrified queries when queries are
                executed, severely hampers performance and should only be
                used for, well, debugging purposes.
        """
        self._client = client
        self.queries = []
        self.schema = schema or self.schema
        self._state = QueryState()
        self._join = Joins(self)
        self._multi = False
        self._dry = False
        self._naked = None
        self._new = False
        self._cursor_factory = cursor_factory
        self.table = table or self.table
        self._debug = debug

    __repr__ = preprX('db')

    # ``Connection handing``

    @property
    def db(self):
        client = self._client or local_client.get('db') or create_client()
        return client

    client = db

    def __enter__(self):
        """ Context manager, connects to :prop:cargo.ORM.db
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
        """ Context manager, closes the :prop:cargo.ORM.db connection """
        self.close()

    def connect(self, **options):
        """ Connects the :prop:cargo.ORM.db.

            Passes @*args and @**kwargs to the local instance of
            :class:Postgres
        """
        return self.db.connect(**options)

    def close(self):
        """ Closes the :prop:cargo.ORM.db, resets the :prop:state """
        self.reset()
        return self.db.close()

    def set_table(self, table):
        """ Sets the ORM table to @table (#str) """
        self.table = table

    def set_cursor_factory(self, factory):
        """ Sets the ORM default cursor factory to @factory """
        self._cursor_factory = factory

    def set_schema(self, name):
        """ Changes the default search path for this model to @name """
        self.schema = name

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
        elif self._multi:
            self.add_query(q)
            self.reset()
            return self
        else:
            self.add_query(q)
            return self.run()

    def insert(self, *fields, **kwargs):
        """ Interprets the :prop:state as an :class:Insert statement

            @*fields: (:class:Field) fields to set VALUES for, if |None| are
                given, all fields in a given model will be evaluated, unless
                fields were provided in a :prop:payload
            @**kwargs: keyword arguments to pass to :class:Insert

            -> result of :class:Insert query
        """
        len_values = len(self.state.get('VALUES', []))
        if not len_values:
            self.values(*fields)
            if not self._multi:
                self.one()
        elif len_values == 1 and not self._multi:
            self.one()
        if not self.state.has('RETURNING'):
            self.returning(*fields)
        if fields and all(isinstance(f, Field) for f in fields):
            self.state.fields = list(fields)
        self._set_into_if_empty(fields)
        return self._cast_query(Insert(self, **kwargs))

    def select(self, *fields, **kwargs):
        """ Interprets the :prop:state as a :class:Select statement. If a table
            is specified within the model and a |FROM| clause hasn't been
            added to the state, a |FROM| clause will automatically be added
            using |self.table|

            @*fields: :class:Field(s) or :mod:cargo.expressions
                to retrieve values for
            @**kwargs: keyword arguments to pass to :class:Select

            -> result of :class:Select query, :class:Subquery object if this
                :prop:state is a subquery, or :class:Query if :prop:_dry
                is True
        """
        if not self.state.has('FROM'):
            self.from_(alias=self._alias if hasattr(self, '_alias') else None)
        self.state.fields = fields
        return self._cast_query(Select(self, **kwargs))

    def get(self, *fields, **kwargs):
        """ Gets a single record from the database
            :see::meth:_select
        """
        return self.one().select(*fields, **kwargs)

    def update(self, *fields, **kwargs):
        """ Interprets the :prop:state as an :class:Update statement.
            An |UPDATE| will be performed on the fields given in @*fields

            @*fields: (:class:cargo.Field or :mod:cargo.expressions)
            @**kwargs: keyword arguments to pass to :class:Select

            -> result of :class:Update query, :class:Subquery object if this
                :prop:state is a subquery, or the :class:Query if :prop:_dry
                is |True|

            ..
                model.update(model.field_a, model.field_b.eq(2001))
            ..
        """
        if not self.state.has('SET'):
            if not fields:
                try:
                    fields = self.fields
                except AttributeError:
                    pass
            self.set(*fields)
        self._set_into_if_empty(fields)
        return self._cast_query(Update(self, **kwargs))

    def delete(self, **kwargs):
        """ Interprets the :prop:state as a :class:Delete statement. If a table
            is specified within the model and a |FROM| clause hasn't been
            added to the state, a |FROM| clause will automatically be added
            using |self.table|

            @**kwargs: keyword arguments to pass to :class:Select

            -> result of :class:Delete query, :class:Subquery object if this
                :prop:state is a subquery, or the :class:Query if :prop:_dry
                is |True|
        """
        if not self.state.has('FROM'):
            self.from_(alias=self._alias if hasattr(self, '_alias') else None)
        if not self.state.has('WHERE'):
            raise ORMIndexError('Deleting all table rows must be done '
                                'explicitly by creating a `WHERE true` '
                                'statement. This is for your own protection. '
                                'e.g. `MyModel.where(True).delete()`')
        return self._cast_query(Delete(self, **kwargs))

    remove = delete

    def upsert(self, *fields, conflict_field=None, conflict_action=None,
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
        return self.insert(*fields, **kwargs)

    def _raw(self, *args, **kwargs):
        """ Interprets the :prop:state as a :class:Raw statement. All clauses
            added to the ORM :prop:state will be parsed in the order of
            which they were added.

            @**kwargs: keyword arguments to pass to :class:Select

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
            self.state.add(Clause("FROM", safe(table), alias=alias))
        return self

    use = from_

    def distinct(self, field, *args, **kwargs):
        self.state.add(Clause('DISTINCT', field, *args, **kwargs))
        return self

    def distinct_on(self, field, *args, **kwargs):
        self.state.add(Clause('DISTINCT',
                              Function('ON', field),
                              *args,
                              **kwargs))
        return self

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
                             order_by=model.order_field.desc())
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
                :class:cargo.Expression(s) |JOIN| @a |ON|
            @using: (:class:Field or #tuple) of :class:Field objects, sets a
                |USING| clause for the |JOIN| using the given fields.

            -> @self
            ===================================================================
            ``Usage Example``
            ..
                m1 = Model()
                m2 = SomeOtherModel()
                m1.join(m2, on=(m1.user_id.eq(m2.id),), alias="_m2")
            ..
            |JOIN m2 _m2 ON m1.user_id = _m2.id|
        """
        self.state.add(self._join(a, b, on, using, type, alias))
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
            :class:Select, :class:Update, and :class:Delete queries

            @*exps: :mod:cargo.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self

            ===================================================================
            ``Usage Example``

            Groups results by the field 'user_id'
            ..
                m = Model()
                m.where(
                    (m.name >> ['Ringo Starr', 'Paul McCartney']) &
                    ((m.band_name.eq('Beatles')) |
                     (m.band_name.eq('Ringo Starr & His All-Starr Band')))
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

    def into(self, table, alias=None):
        """ Sets an |INTO| :class:Clause in the query :prop:state for
            :class:Insert queries. This can also be used for |UPDATE| queries,
            while an |INTO| clause will not be created, the ORM will update
            INTO whatever table is specified.

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
            self.state.add(Clause("INTO", safe(table), alias=alias))
        else:
            raise ValueError("INTO clause cannot be 'None'")
        return self

    def _set_into_if_empty(self, fields=None):
        if not self.state.has('INTO'):
            try:
                fields = fields or self.fields
            except AttributeError:
                pass
            if self.table:
                self.into(self.table)
            elif fields:
                for field in fields:
                    try:
                        self.into(field.table)
                        return
                    except AttributeError:
                        continue

    def values(self, *vals):
        """ Sets a |VALUES| :class:Clause in the query :prop:state for
            :class:Insert queries.

            @*vals: one or several values to set

            -> @self

            ===================================================================
            ``Usage Example``
            ..
                m.payload(m.field1, m.field2, m.field3, m.field4)
                m.insert()
            ..
            |(f1, f2, f3, f4) VALUES (1, 2, 3, 4)|

            ..
                m.payload(1, 2, 3, 4)
                m.payload(5, 6, 7, 8)
                m.insert(m.field1, m.field2, m.field3, m.field4)
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
        if not self.state.fields and all(isinstance(f, Field) for f in vals):
            self.state.fields = list(vals)
        self.state.add(ValuesClause("VALUES", *map(_do_insert, vals)))
        return self

    payload = values

    def set(self, *exps, **kwargs):
        """ Sets a |SET| :class:Clause in the query :prop:state for
            :class:Update queries.

            @*exps: :mod:cargo.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self

            ===================================================================
            ``Usage Example``

            Increments the field 'views' in the model by one
            ..
                m = Model()
                m.where(m.uid.eq(700))
                m.set(m.views.eq(m.views + 1))
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
        exps = list(filter(lambda x: x is not None,  map(_do_update, exps)))
        self.state.add(CommaClause("SET", *exps, use_field_name=True,
                                   **kwargs))
        return self

    def group_by(self, *exps, **kwargs):
        """ Sets a |GROUP BY| :class:Clause in the query :prop:state for
            :class:Select queries.

            @*exps: :mod:cargo.expressions objects
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
            :class:Select queries.

            @*exps: :mod:cargo.expressions objects
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
            :class:Select queries, and optionally an |OFFSET| if @limit2 is
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
            :class:Select queries. The @type and @direction arguments
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
            :class:Select queries

            @offset: #int number of records to offset the query by

            -> @self
        """
        self.state.add(CommaClause("OFFSET", offset))
        return self

    skip = offset

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

            @*exps: :mod:cargo.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(CommaClause("HAVING", *exps, **kwargs))
        return self

    def for_update(self, *exps, **kwargs):
        """ Sets a |FOR UPDATE| :class:Clause in the query :prop:state

            @*exps: :mod:cargo.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(Clause("FOR", Clause("UPDATE"), *exps, **kwargs))
        return self

    def for_share(self, *exps, **kwargs):
        """ Sets a |FOR SHARE| :class:Clause in the query :prop:state

            @*exps: :mod:cargo.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(Clause("FOR", Clause("SHARE"), *exps, **kwargs))
        return self

    def returning(self, *exps, **kwargs):
        """ Sets a |RETURNING| :class:Clause in the query :prop:state

            @*exps: :mod:cargo.expressions objects
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

            @*exps: :mod:cargo.expressions objects
            @**kwargs: keyword arguments to pass to the :class:Clause

            -> @self
        """
        self.state.add(Clause("ON", *exps, join_with=join_with, **kwargs))
        return self

    # ``Query execution``

    def run_iter(self, *queries, fetch=False):
        """ Runs all of the queries in @*qs or in :prop:queries,
            fetches all of the results if there are any and @fetch is |True|

            @*queries: (:class:cargo.Query) one or several objects
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
                try:
                    #: Fetches the result
                    result = result.__getattribute__('fetchone'
                                                     if q.one else
                                                     'fetchall')()
                except psycopg2.ProgrammingError:
                    #: No results to fetch
                    pass
            yield result
        self._reset_accordingly(multi, queries, conn)

    def _reset_accordingly(self, multi, queries, conn):
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
                    conn.put()
                    raise QueryError(e.args[0].strip(),
                                     code=ERROR_CODES.COMMIT,
                                     root=e)
        conn.put()

    def run(self, *queries):
        """ Runs all of the queries in @*qs or in :prop:queries,
            fetches all of the results if there are any

            @*queries: (:class:cargo.Query) one or several

            -> #list of results if more than one query is executed, otherwise
                the cursor factory
        """
        results = [result for result in self.run_iter(*queries, fetch=True)]
        if len(results) == 1:
            return results[0]
        else:
            return results

    def _prepend_search_path_to(self, query):
        search_path = self.db.get_search_paths(self.schema)
        if search_path:
            return 'SET search_path TO %s; %s;' % (", ".join(search_path),
                                                   query)
        return query

    def get_cursor(self, conn, *args, **kwargs):
        """ Gets a database cursor from @conn
            -> (:class:psycopg2.cursor)
        """
        return conn.cursor(*args, cursor_factory=self._cursor_factory,
                           **kwargs)

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
        #: Gets a client connection if one wasn't passed as an argument
        _conn = conn
        if conn is None:
            _conn = self.db.get()
        cursor = self.get_cursor(_conn)
        #: Sets the search path to the locally defined schema
        query = self._prepend_search_path_to(query)
        #: For debug mode
        self.debug(cursor, query, params)
        try:
            #: Executes the cursor
            cursor.execute(query, params or tuple())
        except Psycopg2QueryErrors as e:
            #: Rolls back the transaction in the event of a failure
            _conn.rollback()
            if conn is None:
                _conn.put()
            raise QueryError(e.args[0].strip(),
                             code=ERROR_CODES.EXECUTE,
                             root=e)
        #: Commits if the client connection is not set to autocommit
        #  and the 'commit' argument is true
        if commit and not _conn.autocommit:
            try:
                _conn.commit()
            except Psycopg2QueryErrors as e:
                #: Rolls back the transaction in the event of a failure
                _conn.rollback()
                if conn is None:
                    _conn.put()
                raise QueryError(e.args[0].strip(),
                                 code=ERROR_CODES.COMMIT,
                                 root=e)
        #: Puts a client connection away if it is a pool and no connection
        #  was passed in arguments. If a connection object is passed,
        #  it's the user's responsibility to put it away.
        if conn is None:
            _conn.put()
        return cursor

    def subquery(self, alias=None):
        """ Interprets the query :prop:state as a subquery. This query will
            not be executed and can be passed around like other
            :class:cargo.expressions.

            Also see: :class:Subquery

            -> @self
            =================================================================
            ``Usage Example``
            ..
                m = Model()
                m.subquery()
                subquery = m.select(m.user_id.max())

                m.where(m.user_id.eq(subquery))
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

    def reset_new(self):
        """ Resets the new option """
        self._new = False
        return self

    def reset_state(self):
        """ Resets the :prop:state object """
        self.state.reset()
        return self

    def reset_multi(self):
        """ Removes multi mode from the ORM state """
        self._multi = False
        self.queries = []
        self.reset_dry()
        self.reset_naked()
        self.reset_new()
        self.reset_state()
        return self

    def reset(self, multi=False):
        """ Resets the query :prop:state, many mode and optionally
            multi-mode if @multi is set to |True|
        """
        if not self._multi:
            self.queries = []
            self.reset_dry()
            self.reset_naked()
            self.reset_new()
        if multi:
            self.reset_multi()
        self.reset_state()
        return self

    def debug(self, cursor, query, params):
        """ Prints query information including :prop:state,
            :prop:params, and :prop:queries
        """
        if self._debug:
            pre = re.compile(r'''(%\(0x[0-9a-f]+\)s)''')
            line('—')
            logg().warning("Cargo Query")
            line('—')
            l = logg(pre.sub(r'\033[1m\1\033[1;m',
                             sqlparse.format(query.replace('; ', ';\n'),
                                             reindent=True)),
                     params,
                     pretty=True)
            l.log("Parameterized", force=True)
            line('—')
            mog = sqlparse.format(
                cursor.mogrify(query.replace('; ', ';\n'), params).decode(),
                reindent=True)
            l = logg(mog, pretty=True)
            l.log("Mogrified", force=True)
            line('—')

    def copy(self, *args, clear=False, **kwargs):
        """ -> a safe copy of the model """
        cls = self.__class__(*args,
                             cursor_factory=self._cursor_factory,
                             schema=self.schema,
                             debug=self._debug,
                             client=self._client,
                             **kwargs)
        if not clear:
            cls.queries = self.queries.copy()
            if self.state:
                cls._state = self.state.copy()
            cls._multi = self._multi
            cls._dry = self._dry
            cls._naked = self._naked
            cls._new = self._new
        cls.table = self.table
        return cls

    def clear_copy(self, *args, **kwargs):
        """ -> a safe and cleared copy of the model """
        return self.copy(*args, clear=True, **kwargs)

    __copy__ = copy


class QueryState(object):
    """`Query State`
        Manages the ORM clauses, joins, parameters and subqueries.
    """
    __slots__ = ('clauses', 'params', 'alias', 'one', 'is_subquery', 'fields')
    _multi_clauses = {'VALUES', 'JOIN'}

    def __init__(self):
        self.clauses = OrderedDict()
        self.params = {}
        self.one = False
        #: Subqueries
        self.alias = None
        self.is_subquery = False
        self.fields = []

    __repr__ = preprX('clauses', 'params', keyless=True)

    def __iter__(self):
        """ Iterating the query state yields it's clauses"""
        for v in self.clauses.values():
            yield v

    def add_fields(self, *fields):
        """ Adds :class:Fields for :prop:ORM.insert and :prop:ORM.select """
        self.fields.extend(fields)

    def reset(self):
        """ Resets the query state """
        self.clauses = OrderedDict()
        self.params = {}
        #: Subqueries
        self.alias = None
        self.one = False
        self.is_subquery = False
        self.fields = []
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
        """ Adds or replaces :class:Clause(s) to the :class:ORM query state.
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

    replace = add

    def copy(self, *args, **kwargs):
        cls = self.__class__()
        cls.clauses = self.clauses.copy()
        cls.params = self.params.copy()
        cls.fields = self.fields.copy()
        cls.alias = self.alias
        cls.one = self.one
        cls.is_subquery = self.is_subquery
        return cls

    __copy__ = copy


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
            if named_match is not None:
                on = a.eq(named_match)
            elif typed_match is not None:
                on = a.eq(typed_match)
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
                raise RuntimeError('Maximum expression search depth exceeded '
                                   'in JOIN.')
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
                #: Auto-sets aliases when they exist on models
                if not alias and a._alias is not None:
                    alias = a._alias
                table, on = self.with_on(a, on, alias)
            else:
                table = None
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
                    elif isinstance(a, (Model, Relationship, Reference)):
                        #: Auto-sets aliases when they exist on models
                        if not alias and a._alias is not None:
                            alias = a._alias
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
_adapted_fields = set()


class Model(ORM):
    """ ===================================================================
        ``Usage Examples``

        Creates a new model with two fields
        ..
            from cargo import Model, Text, Int

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
            my_model.where(my_model.my_id.eq(4))
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
    table = None
    ORDINAL = []  # The order of the fields within the DB
    PRIVATE = set()  # Fields which should not be exposed by :meth:for_json

    def __init__(self, client=None, cursor_factory=None, naked=None,
                 schema=None, debug=False, **field_data):
        """``Basic Model``
            =======================================================================
            @naked: (#bool) True to default to raw query results in the form
                of the :prop:_cursor_factory, rather than the default which
                is to return query results as copies of the current model.
            @**field_data: |field_name=field_value| key/value pairs
            =======================================================================
            :see::class:ORM
        """
        super().__init__(client,
                         cursor_factory=cursor_factory,
                         schema=schema,
                         table=self.table or camel_to_underscore(
                            self.__class__.__name__),
                         debug=debug)
        self._fields = []
        self._relationships = []
        self._alias = None
        self._always_naked = naked or False
        if not field_data.get('__nocompile__'):
            self._compile()
        else:
            del field_data['__nocompile__']
        self.fill(**field_data)

    __repr__ = preprX('field_names', keyless=True)

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
            return self.__getattribute__(name).__getattribute__('value')
        except AttributeError:
            raise KeyError("Field `{}` not in {}".format(name, self.table))

    def __setitem__(self, name, value):
        """ Sets the field value for @name to @value """
        try:
            self.__getattribute__(name).__call__(value)
        except AttributeError:
            raise KeyError("Field `{}` not in {}".format(name, self.table))

    def __delitem__(self, name):
        """ Deletes the field value for @name, the field remains in the model
        """
        try:
            self.__getattribute__(name).__getattribute__('clear')()
        except AttributeError:
            raise KeyError("Field `{}` not in {}".format(name, self.table))

    def __iter__(self):
        """ -> iterates through records in the model """
        return self.iter()

    def set_table(self, table):
        """ Sets the ORM table to @table (#str) """
        self.table = table
        for field in self.fields:
            field.table = table
        return self

    def _getmembers(self):
        mro = self.__class__.__mro__
        mro_len = len(mro) - 3  # 3 = object, ORM, Model
        for x in range(mro_len):
            for items in mro[mro_len - x - 1].__dict__.items():
                yield items

    def _compile(self):
        """ Sets :class:Field, :class:Relationship and :class:ForeignKey
            attributes
        """
        for field_name, field in self._getmembers():
            if isinstance(field, Field):
                field = field.copy()
                field.field_name = field_name
                self._add_field(field)
            elif isinstance(field, (ForeignKey, Relationship)):
                field.forge(self, field_name)
        self._order_fields()

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
        self._new = True
        return self

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
                    from cargo import Model

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
            field = getattr(self, field_name).copy()
            field(value)
            if not method:
                method = field.eq(field.value)
            else:
                method = getattr(field, "".join(method))(field.value)
            add_exp(method)
        return self.where(*exps)

    def to_json(self, *args, **kwargs):
        """ Returns a JSON dump'd #str of the state of the current model
            with all field: value pairs included.

            @*args and @**kwargs are passed to :func:dumps
        """
        return dumps(self.for_json(), *args, **kwargs)

    def for_json(self, filter=None, ignore_private=True):
        """ Prepares the model for JSON encoding.  By default this method will
            ignore all (#str) field names given in the :attr:PRIVATE constant.
            -> (#dict) the model as represented by a dictionary, after calling
               each field's |for_json| method to extract the respective JSON
               values
        """
        filter = (filter if filter is not None else lambda x: True)
        return {field.field_name: field.for_json()
                for field in self.fields
                if filter(field) and
                not ignore_private or field.field_name not in self.PRIVATE}

    def from_json(self, val):
        """ JSON loads @val into the current model.
            -> self
        """
        self.fill(**loads(val))
        return self

    def to_dict(self):
        """ -> (#dict) of |{field_name: field_value}| pairs within the model
        """
        if self.ORDINAL:
            return OrderedDict((field.field_name, field())
                               for field in (self.fields or []))
        else:
            return {field.field_name: field() for field in (self.fields or [])}

    def to_namedtuple(self):
        """ -> (:class:namedtuple) representation of the model """
        if not hasattr(self, self.__class__.__name__ + 'Record'):
            self._make_nt()
        if self.ORDINAL:
            return getattr(self, self.__class__.__name__ + 'Record')(
                *self.field_values)
        return getattr(self, self.__class__.__name__ + 'Record')(
            **self.to_dict())

    def from_namedtuple(self, nt):
        """ Fills the model using a namedtuple
            -> self
        """
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

    def _register_field(self, field):
        try:
            confield = (self.db, field.__class__)
            if confield not in _adapted_fields:
                field.register_adapter()
                _adapted_fields.add(confield)
        except AttributeError:
            pass
        try:
            # self.db.after('connect', )
            field.type.register_type(self.db)
        except AttributeError:
            try:
                field.register_type(self.db)
            except AttributeError:
                pass

    def _add_field(self, field):
        """ Adds a field to the model to the model.
        """
        field.table = self.table
        self._register_field(field)
        setattr(self, field.field_name, field)
        self._fields.append(field)

    def add_relationship(self, name, relationships):
        """ Adds a relationship to the model
            @name: (#str) name of the relationship
            @relationship: (:class:cargo.Relationship)
        """
        relationship.forge(self, name)
        return self

    def approx_size(self, alias=None):
        """ Selects the approximate size of the model. This is not filterable.
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
        result = q.execute().fetchone()
        self.reset_naked()
        if alias is None:
            try:
                result = result['count']
            except (TypeError, KeyError):
                try:
                    result = result.count
                except KeyError:
                    result = result[0]
        self.reset()
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
        self.one().naked()
        result = super().select(self.best_index.count(alias=alias))
        if alias is None:
            try:
                result = result['count']
            except (TypeError, KeyError):
                result = result.count
        return result

    def _order_fields(self, ORDINAL=None):
        ORDINAL = ORDINAL or self.ORDINAL
        if ORDINAL:
            self._fields = list(map(self.__getattribute__, ORDINAL))
        return self._fields

    @property
    def fields(self):
        """ -> (#list) of the fields in the model based on :desc:__dict__ or
                :attr:ORDINAL if it exists
        """
        return self._fields

    @cached_property
    def field_names(self):
        """ -> (#tuple) of all field names (without the table name) within the
                model
        """
        return tuple(field.field_name for field in self.fields)

    @cached_property
    def private_fields(self):
        return tuple(field
                     for field in self.fields
                     if field.field_name in self.PRIVATE)

    @cached_property
    def names(self):
        """ -> (#tuple) of all the full field names, table included """
        return tuple(field.name for field in self.fields)

    @property
    def field_values(self):
        """ -> (#tuple) of all field values within the model """
        return tuple(field.value for field in self.fields)

    @property
    def relationships(self):
        """ -> (#list) all :class:Relationship(s) within the model """
        return self._relationships

    @cached_property
    def foreign_keys(self):
        """ -> (#list) all :class:ForeignKeyField(s) fields within the model
        """
        return tuple(field
                     for field in self.fields
                     if isinstance(field, _ForeignObject))

    @cached_property
    def indexes(self):
        """ -> (#tuple) of all indexes within the model excluding primary keys
        """
        return tuple(field for field in self.fields if field.index)

    @cached_property
    def plain_indexes(self):
        """ -> (#tuple) of all plain indexes within the model
                (not unique or primary keys)
        """
        return tuple(field for field in self.fields
                     if field.index and not field.unique)

    @cached_property
    def unique_indexes(self):
        """ -> (#tuple) of all unique indexes within the model """
        return tuple(field for field in self.indexes if field.unique)

    @cached_property
    def unique_fields(self):
        """ -> (#tuple) of all unique constrainted fields within the model """
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
        """ -> (#tuple) of best indexes in the model in order, first best and
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

            -> (:class:cargo.Field) object
        """
        return self.best_indexes[0]

    @property
    def best_available_index(self):
        """ Looks for the best available index in the model where the index
            field has a value, looking for a primary key, followed by unique
            keys, followed by any index

            -> (:class:cargo.Expression) object |index_field.eq(index_value)|
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
        for index in list(self.unique_indexes):
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

            -> (:class:cargo.Expression) object |index_field.eq(index_value)|
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
            field.reset()
        return self

    def reset_relationships(self):
        """ Resets all :class:Relationship(s) within the model """
        for relationship in self.relationships:
            relationship.clear()
        return self

    def reset(self, *args, **kwargs):
        """ :see::meth:ORM.reset """
        self._alias = None
        return super().reset(*args, **kwargs)

    def clear(self):
        """ Sets all of the :class:Field values within the model to |None|,
            resets the :prop:state.
        """
        self.reset()
        self.reset_fields()
        self.reset_relationships()
        return self

    def _is_naked(self):
        """ -> (#bool) |True| if the cursor factory in :prop:_cursor_factory
                should be used as opposed to :class:ModelCursor
        """
        return self._naked if self._naked is not None else self._always_naked

    def get_cursor(self, conn, *args, **kwargs):
        """ Gets a database cursor from @conn
            -> (:class:psycopg2.cursor)
        """
        if not self._is_naked():
            cursor = conn.cursor(*args, cursor_factory=ModelCursor, **kwargs)
        else:
            cursor = conn.cursor(*args, cursor_factory=self._cursor_factory,
                                 **kwargs)
        try:
            cursor._cargo_model = self
        except:
            pass
        return cursor

    def add(self, *args, **kwargs):
        """ Adds a new record to the DB without affecting the current model.
            @*args: values to insert according to the model's ORDINAL
                defined by :attr:ORDINAL
            @**kwargs: |field=value| pairs to insert, will be ignored if
                @*args are supplied

            -> result of INSERT query
            ..
                class MyModel(Model):
                    ORDINAL = ['f1', 'f2']
                    f1 = Int()
                    f2 = Int()
                    ...
                Model = MyModel()
                Model.add(1, 2, 3, 4)
                # INSERT INTO my_model (f1, f2) VALUES (1, 2), (3, 4)
                Model.add(f1=1, f2=2)
                # INSERT INTO my_model (f1, f2) VALUES (1, 2)
            ..
        """
        if self._multi:
            model = self.reset_fields()
        else:
            model = self.copy().reset_fields()
            self.reset()
        if args:
            for cargo in grouped(args, len(model.fields)):
                model.payload(*cargo)
            if len(model.state.get('VALUES')) <= 1:
                model.one()
        else:
            for name, val in kwargs.items():
                model[name] = val
            model.one()
        return model.returning().insert()

    __call__ = add

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
        return super().insert(*(fields or self.fields))

    def select(self, *fields, **kwargs):
        """ Selects records from the DB based on the
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
        return super().select(*fields, **kwargs)

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
        return super().one().select(*fields, **kwargs)

    def _explicit_where(self):
        if not self.state.has('WHERE'):
            best_index = self.best_unique_index
            if best_index is not None:
                self.where(best_index)
            else:
                raise ORMIndexError('WHERE clauses on Model queries must ' +
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
        return super().delete(*args, **kwargs)

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

            @fields: (:class:cargo.Field) objects within the Model,
                if None are specified, all will be updated

            -> result of query, differs depending on cursor settings
        """
        if not self.state.has('RETURNING'):
            return_fields = filter(lambda x: isinstance(x, Field), fields)
            self.returning(*return_fields)
        self._explicit_where()
        return super().update(*fields, **kwargs)

    def pull_all(self, *args, dry=False, **kwargs):
        """ Pulls all the relationships in the model.
            :see::meth:Relationship.pull

            -> (#dict) |{relationship_name: results}|
        """
        return {
            relationship._owner_attr:
                relationship.pull(*args, dry=dry, **kwargs)
            for relationship in self.relationships}

    def last(self, count=1, *args, **kwargs):
        """ Selects the last @count rows from the model based on the
            :prop:best_index ordered |DESC|. If @count is |1| a single result
            will be returned, otherwise a list of results will be returned.

            @count: (#int) the number of results to fetch
            @args and @kwargs are passed to :meth:select
        """
        self.new()
        if count == 1:
            self.one()
        self.limit(count)
        self.order_by(self.best_index.desc())
        return super().select(*args, **kwargs)

    def first(self, count=1, *args, **kwargs):
        """ Selects the first @count rows from the model based on the
            :prop:best_index ordered |ASC|. If @count is |1| a single result
            will be returned, otherwise a list of results will be returned.

            @count: (#int) the number of results to fetch
            @args and @kwargs are passed to :meth:select
        """
        self.new()
        if count == 1:
            self.one()
        self.limit(count)
        self.order_by(self.best_index.asc())
        return super().select(*args, **kwargs)

    def iternaked(self, offset=0, limit=0, buffer=100, order_field=None,
                  reverse=False):
        """ Yields cursor factory until there are no more results to fetch.

            @offset: (#int) cursor start position
            @limit: (#int) total number of results to fetch
            @buffer: (#int) cursor max results to fetch in one page
            @reverse: (#bool) True if returning in descending order
            @order_field: (:class:cargo.Field) object to order the
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
        q = super().dry().select().execute()
        while True:
            results = q.fetchmany(buffer)
            if not results:
                break
            for result in results:
                yield result
        self.reset()

    def copy(self, *args, clear=False, **kwargs):
        """ Returns a safe copy of the model """
        cls = ORM.copy(self, *args, __nocompile__=True, clear=clear, **kwargs)
        cls.field_names = self.field_names
        cls.names = self.names
        cls._alias = self._alias
        cls._always_naked = self._always_naked
        cls._fields = list(map(
            lambda x: getattr(cls, x.field_name)
            if not setattr(cls, x.field_name, x.copy()) else None,
            self._fields))
        cls._relationships = []
        for rel in self._relationships:
            rel.forge(cls, rel._owner_attr)
        return cls

    def clear_copy(self, *args, **kwargs):
        """ Returns a safe and cleared copy of the model """
        cls = self.copy(*args, clear=True, **kwargs)
        cls.reset_fields()
        return cls

    __copy__ = copy


class RestModel(Model):

    def __init__(self, *args, **kwargs):
        """`RESTful Model`
            =======================================================================
            An implementation of :class:Model with a REST-like interface. Also
            provides the :meth:patch feature.
            =======================================================================
            :see::class:Model
        """
        super().__init__(*args, **kwargs)

    def patch(self, field, **kwargs):
        """ Updates one @field in the model based on the
            :prop:best_available_index

            @field: an instance of :class:cargo.Field

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
