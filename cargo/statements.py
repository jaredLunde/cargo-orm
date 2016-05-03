"""

  `Cargo ORM Statements`
  ``Objects for SQL statement generation``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import re
from copy import copy

try:
    import ujson as json
except ImportError:
    import json

import psycopg2
import psycopg2.extras

import sqlparse

from vital.cache import local_property
from vital.debug import Logg as logg
from vital.tools.dicts import merge_dict
from vital.debug import preprX, line

from cargo.clients import db
from cargo.etc.types import *
from cargo.exceptions import *
from cargo.expressions import *
from cargo.fields import *


__all__ = (
    "Delete",
    "Except",
    "Insert",
    "Intersect",
    "SetOperations",
    "Query",
    "Raw",
    "Select",
    "Union",
    "Update")


#
#  `` Query Objects ``
#


class BaseQuery(StringLogic, NumericLogic):
    """ Base query object, provides several common methods for the
        various query statement types
    """
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string',
                 'result')

    def __init__(self, query=None, params=None, orm=None):
        self.orm = orm or db
        try:
            self.params = params or self.orm.state.params
            self.is_subquery = self.orm.state.is_subquery
            self.one = self.orm.state.one
        except AttributeError:
            self.params = params or {}
            self.is_subquery = False
            self.one = False
        self.alias = None
        self.string = query.strip() if query else query

    def __str__(self):
        return self.query if self.query else self.__repr__()

    def _filter_empty(self, els):
        return filter(lambda x: x is not _empty, els)

    @property
    def query(self):
        try:
            return self.string.strip()
        except AttributeError:
            pass

    @property
    def mogrified(self):
        """ -> (#str) the query post-parameterization """
        client = self.orm.db.get()
        cur = client.cursor()
        res = sqlparse.format(cur.mogrify(self.string, self.params).decode(),
                              reindent=True)
        client.put()
        return res

    def compile(self):
        return self.query

    def execute(self):
        """ Executes :prop:query in the :prop:orm """
        return self.orm.execute(self.query, self.params)

    def debug(self):
        """ Prints the query string with its parameters """
        client = self.orm.db.get()
        cur = client.cursor()
        self.orm.debug(cur, self.string, self.params)
        client.put()


class Query(BaseQuery):
    """ ======================================================================
        ``Usage Example``
        ..
            q = Query(ORM(), query="SELECT * FROM users WHERE true")
            cursor = q.execute()
        ..
        |<cursor object at 0x7f844e676900; closed: 0>|

        ..
            cursor.fetchall()
        ..
        |{......}|
    """
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string')

    def __init__(self, query=None, params=None, alias=None, orm=None):
        """`Query Statement`
            @query: (#str) raw query string
            @params: (#dict) for parameterizing the query
                |"SELECT %(abc)" % {'abc': 123}|
            @alias: (#str) query alias name if it's a :class:WITH or
                :class:SetOperations query
            @orm: (:class:ORM) object
        """
        super().__init__(query, params, orm=orm)
        try:
            alias = alias or self.orm.state.alias
        except AttributeError:
            pass
        self.alias = alias

    __repr__ = preprX('query', 'params', keyless=True, address=False)

    def exists(self, alias=None):
        return Functions.exists(self, alias=alias)

    def not_exists(self, alias=None):
        return Functions.not_exists(self, alias=alias)


class SetOperations(Query):
    """ Base structure for :class:UNION, :class:EXCEPT and :class:INTERSECTION
        queries.
    """
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string',
                 'operations', 'all', 'distinct')

    def __init__(self, orm, *args, all=None, distinct=None, **kwargs):
        super().__init__(orm=orm, **kwargs)
        self.operations = args or []
        self.all = all
        self.distinct = distinct

    def __and__(self, other):
        """ Creates |UNION| query

            -> :class:UNION query object
            ===================================================================
            ``Usage Example``

            Creates the union query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                union = q1 & q2
            ..
            |<cargo.statements.Union(                                |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true UNION             |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the union query
            ..
                union.execute()
            ..
        """
        return Union(self.orm, self, other)

    union = __and__

    def __add__(self, other):
        """ Creates |UNION ALL| query

            -> :class:UNION query object
            ===================================================================
            ``Usage Example``

            Creates the union query
            ..
                q1 = Query("SELECT * FROM users WHERE true")
                q2 = Query("SELECT * FROM users_followers WHERE true")
                union = q1 + q2
            ..
            |<cargo.statements.Union(                                |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true  UNION ALL        |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the union query
            ..
                union.execute()
            ..
        """
        return Union(self.orm, self, other, all=True)

    union_all = __add__

    def __sub__(self, other):
        """ Creates |UNION DISTINCT| query

            -> :class:UNION query object
            ===================================================================
            ``Usage Example``

            Creates the union query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                union = q1 - q2
            ..
            |<cargo.statements.Union(                                |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true  UNION DISTINCT   |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the union query
            ..
                union.execute()
            ..
        """
        return Union(self.orm, self, other, distinct=True)

    union_distinct = __sub__

    def __lt__(self, other):
        """ Creates |EXCEPT| query

            -> :class:EXCEPT query object
            ===================================================================
            ``Usage Example``

            Creates the except query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                _except = q1 < q2
            ..
            |<cargo.statements.Except(                               |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true EXCEPT            |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the except query
            ..
                _except.execute()
            ..
        """
        return Except(self.orm, self, other)

    except_ = __lt__

    def __le__(self, other):
        """ Creates |EXCEPT ALL| query

            -> :class:EXCEPT query object
            ===================================================================
            ``Usage Example``

            Creates the except query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                _except = q1 <= q2
            ..
            |<cargo.statements.Except(                               |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true EXCEPT ALL        |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the except query
            ..
                _except.execute()
            ..
        """
        return Except(self.orm, self, other, all=True)

    except_all = __le__

    def __lshift__(self, other):
        """ Creates |EXCEPT DISTINCT| query

            -> :class:EXCEPT query object
            ===================================================================
            ``Usage Example``

            Creates the except query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                _except = q1 << q2
            ..
            |<cargo.statements.Except(                               |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true EXCEPT DISTINCT   |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the except query
            ..
                _except.execute()
            ..
        """
        return Except(self.orm, self, other, distinct=True)

    except_distinct = __lshift__

    def __gt__(self, other):
        """ Creates |INTERSECT| query

            -> :class:INTERSECT query object
            ===================================================================
            ``Usage Example``

            Creates the intersect query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                intersect = q1 > q2
            ..
            |<cargo.statements.Intersect(                            |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true INTERSECT         |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the intersect query
            ..
                intersect.execute()
            ..
        """
        return Intersect(self.orm, self, other)

    intersect = __gt__

    def __ge__(self, other):
        """ Creates |INTERSECT ALL| query

            -> :class:INTERSECT query object
            ===================================================================
            ``Usage Example``

            Creates the intersect query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                intersect = q1 >= q2
            ..
            |<cargo.statements.Intersect(                            |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true INTERSECT ALL     |
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the intersect query
            ..
                intersect.execute()
            ..
        """
        return Intersect(self.orm, self, other, all=True)

    intersect_all = __ge__

    def __rshift__(self, other):
        """ Creates |INTERSECT DISTINCT| query

            -> :class:INTERSECT query object
            ===================================================================
            ``Usage Example``

            Creates the intersect query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                intersect = q1 >> q2
            ..
            |<cargo.statements.Intersect(                            |
            |   query_string=`                                       |
            |       SELECT * FROM users WHERE true INTERSECT DISTINCT|
            |       SELECT * FROM users_followers WHERE true`,       |
            |   params={}                                            |
            |):0x7f83e1f21940>                                       |

            Executes the intersect query
            ..
                intersect.execute()
            ..
        """
        return Intersect(self.orm, self, other, distinct=True)

    intersect_distinct = __rshift__

    def _set_intersection_str(self, all, distinct):
        """ Formats the statement with |ALL| and |DISTINCT| when specified """
        intersection_str = "{} {}"\
            .format(self.__querytype__, "ALL" if all else "").strip()
        intersection_str = " {} {} "\
            .format(intersection_str, "DISTINCT" if distinct else "")
        return intersection_str.rstrip() + ' '

    def _attach_alias(self):
        """ Attaches alias when specified """
        for intersection in self.operations:
            self.alias = intersection.alias if hasattr(intersection, 'alias') \
                else None
            if self.alias:
                break

    def compile(self, operations=None, all=None, distinct=None):
        """ :see::meth:SELECT.compile """
        self.operations = \
            operations if operations is not None else self.operations
        self.all = all if all is not None else self.all
        self.distinct = distinct if distinct is not None else self.distinct
        intersection_str = self._set_intersection_str(self.all, self.distinct)
        new_intersection_str = intersection_str.join(
            i.query if i.query else ""
            for i in self.operations
        )
        self.string = "%s %s %s" % (
            self.string if self.string else "",
            intersection_str if self.string else "",
            new_intersection_str
        )
        self.params = merge_dict(self.params,
                                 *[q.params for q in self.operations])
        self._attach_alias()
        # self.orm.reset()
        return self.string


class Union(SetOperations):
    """ Creates a UNION statement between multiple :class:BaseQuery objects

        ======================================================================
        ``Usage Example``
        Creates the |UNION|
        ..
            q1 = Select(SomeORM())
            q2 = Select(SomeORM())
            union = Union(SomeORM(), q1, q2)
        ..
        |q1.query UNION q2.query|

        Executes the |UNION|
        ..
            cursor = union.execute()
        ..
    """
    __querytype__ = "UNION"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string',
                 'operations', 'all', 'distinct')

    def __init__(self, orm, *unions, all=False, distinct=False, **kwargs):
        """`UNION`
            ==================================================================
            @orm: :class:ORM object
            @*unions: :class:BaseQuery objects
            @all: #bool True if |UNION ALL|
            @distinct: #bool True if |UNION DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:SetOperations query
        """
        super().__init__(orm, *unions, all=all, distinct=distinct,
                         **kwargs)
        self.compile()


class Intersect(SetOperations):
    """ Creates an |INTERSECT| statement between multiple :class:BaseQuery
        objects

        ======================================================================
        ``Usage Example``

        Creates the |INTERSECT|
        ..
            q1 = Select(SomeORM())
            q2 = Select(SomeORM())
            intersect = Intersect(SomeORM(), q1, q2)
        ..
        |q1.query INTERSECT q2.query|

        Executes the |INTERSECT|
        ..
            cursor = intersect.execute()
        ..
    """
    __querytype__ = "INTERSECT"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string',
                 'operations', 'all', 'distinct')

    def __init__(self, orm, *intersects, all=False, distinct=False, **kwargs):
        """`INTERSECT`
            ==================================================================
            @orm: :class:ORM object
            @*intersects: :class:BaseQuery objects
            @all: #bool True if |INTERSECT ALL|
            @distinct: #bool True if |INTERSECT DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:SetOperations query
        """
        super().__init__(orm, *intersects, all=all, distinct=distinct,
                         **kwargs)
        self.compile()


class Except(SetOperations):
    """ Creates an |EXCEPT| statement between multiple :class:BaseQuery objects

        ======================================================================
        ``Usage Example``

        Creates the |EXCEPT|
        ..
            q1 = Select(SomeORM())
            q2 = Select(SomeORM())
            except_ = Except(SomeORM(), q1, q2)
        ..
        |q1.query EXCEPT q2.query|

        Executes the |EXCEPT|
        ..
            cursor = except_.execute()
        ..
    """
    __querytype__ = "EXCEPT"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string',
                 'operations', 'all', 'distinct')

    def __init__(self, orm, *excepts, all=False, distinct=False, **kwargs):
        """`EXCEPT`
            ==================================================================
            @orm: :class:ORM object
            @*excepts: :class:BaseQuery objects
            @all: #bool True if |EXCEPT ALL|
            @distinct: #bool True if |EXCEPT DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:SetOperations query
        """
        super().__init__(orm, *excepts, all=all, distinct=distinct,
                         **kwargs)
        self.compile()


class Raw(SetOperations):
    """ Evaluates :class:QueryState clauses within the :mod:orm
        in the order in which they are declared, does not attempt to
        self-order at all.

        ======================================================================
        ``Usage Example``
        ..
            Raw(ORM().values(1))
        ..
        |<cargo.statements.Raw(query_string=`VALUES (%(c5R053jXaKT4)s)`,      |
        |   params={'c5R053jXaKT4': 1}):0x7f5f022ddf60>                       |
    """
    __querytype__ = "RAW"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string',
                 'operations', 'all', 'distinct')

    def __init__(self, *args, **kwargs):
        """`RAW`
            ==================================================================
            :see::meth:Query.__init__
        """
        super().__init__(*args, **kwargs)
        self.compile()

    def evaluate_state(self):
        """ :see::meth:SELECT.evaluate_state """
        for state in self.orm.state:
            try:
                yield state.string
            except AttributeError:
                for s in state:
                    yield s.string

    def compile(self):
        """ :see::meth:SELECT.compile """
        self.string = " ".join(self.evaluate_state())
        return self.string


#
#  `` Query Objects by Type ``
#


class Insert(Query):
    """ ======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        ======================================================================

        Creates and executes a simple |INSERT| query
        ..
            # Populates the username field of the ORM
            orm['username'] = 'FriskyWolf'

            # Inserts the record into the 'users' model
            q = Insert(orm)
            q.query
        ..
        |'INSERT INTO users (username) VALUES ('FriskyWolf')'|

        ..
            cursor = q.execute()
            cursor.fetchall()
        ..
        |[{'username': 'FriskyWolf'}]|
    """
    __querytype__ = "INSERT"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string')

    def __init__(self, orm, **kwargs):
        """`INSERT`
            ==================================================================
            @orm: :class:ORM object
        """
        super().__init__(orm=orm, **kwargs)
        self.compile()

    clauses = ('INTO', 'VALUES', 'RETURNING', 'ON')

    def evaluate_state(self):
        """ :see::meth:SELECT.evaluate_state """
        query_clauses = [_empty] * len(self.clauses)
        values = []
        for state in self.orm.state:
            try:
                query_clauses[self.clauses.index(state.clause)] = state.string
            except AttributeError:
                values = state
        # Insert VALUES
        query_clauses[1] = "(%s) VALUES %s" % (
                           ", ".join(f.field_name
                                     for f in self.orm.state.fields),
                           ", ".join(val.string.replace("VALUES ", "", 1)
                                     for val in values))
        # self.params.update(self.orm.state.params)
        return self._filter_empty(query_clauses)

    def compile(self):
        """ :see::meth:SELECT.compile """
        self.string = "INSERT %s" % " ".join(self.evaluate_state())
        return self.string


class Select(SetOperations):
    """ ======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        ======================================================================
        Creates and executes a simple |SELECT| query
        ..
            # Selects the field 'username' from the users model
            q = Select(orm, orm.username)
            q.query
        ..
        |'SELECT users.username FROM users'|

        ..
            cursor = q.execute()
            cursor.fetchall()
        ..
        |[{'username': 'FriskyWolf'}, {'username': 'test'}]|

        ======================================================================
        Creates and executes a simple |SELECT| query with a |WHERE| clause
        ..
            # Sets the 'WHERE' clause
            orm.where(orm.username == 'FriskyWolf')

            # Selects the field 'username' from the users model
            q = Select(orm, orm.username)
            q.query
        ..
        |"SELECT users.username FROM users WHERE username = 'FriskyWolf'"|

        ..
            cursor = q.execute()
            cursor.fetchall()
        ..
        |[{'username': 'FriskyWolf'}]|
    """
    __querytype__ = "SELECT"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string',
                 'operations', 'all', 'distinct')

    def __init__(self, orm, **kwargs):
        """`SELECT`
            ==================================================================
            @orm: :class:ORM object
        """
        super().__init__(orm=orm, **kwargs)
        self.compile()

    @property
    def fields(self):
        """ Formats the @fields received, merges their :prop:params with
            |self.params|
        """
        update_params = self.params.update
        for field in self.orm.state.fields:
            if isinstance(field, Field):
                yield aliased(field).string
            elif isinstance(field, BaseLogic):
                yield str(field)
                try:
                    update_params(field.params)
                except AttributeError:
                    pass
            else:
                field = parameterize(field)
                update_params(field.params)
                yield field.string

    clauses = ('DISTINCT', 'FROM', 'JOIN', 'WHERE', 'GROUP BY', 'HAVING',
               'WINDOW', 'ORDER BY', 'LIMIT', 'OFFSET', 'FETCH', 'FOR')

    def evaluate_state(self):
        """ Evaluates the :class:Clause objects in :class:QueryState """
        query_clauses = [_empty] * len(self.clauses)
        joins = None
        for state in self.orm.state:
            try:
                query_clauses[self.clauses.index(state.clause)] = state.string
            except (ValueError, AttributeError):
                joins = state
        if joins is not None:
            query_clauses[2] = " ".join(c.string for c in joins)
        query_clauses.insert(1, ", ".join(self.fields) or "*")
        return self._filter_empty(query_clauses)

    def compile(self):
        """ Compiles a query string from the :class:QueryState """
        self.string = ("SELECT %s" % (" ".join(self.evaluate_state())))
        return self.string


class Update(Query):
    """ ======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        ======================================================================
        Creates and executes a simple |UPDATE| query
        ..
            # Tells the ORM which fields to update
            orm.set(orm.username == 'TurkeyTom')
            # Tells the ORM to set a WHERE clause
            orm.where(orm.username == 'FriskyWolf')
            # Updates the field 'username' from the users model
            q = Update(orm)
            q.query
            # Executes the query
            cursor = q.execute()
        ..
        |"UPDATE users SET username = 'TurkeyTom' |
        | WHERE username = 'FriskyWolf'"          |
    """
    __querytype__ = "UPDATE"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string')

    def __init__(self, orm, **kwargs):
        """ `UPDATE`
            ``Query Statement``
            @orm: :class:ORM objects
        """
        super().__init__(orm=orm, **kwargs)
        self.compile()

    clauses = ('SET', 'FROM', 'WHERE', 'RETURNING')

    def evaluate_state(self):
        """ :see::meth:SELECT evaluate_state """
        get_state = self.orm.state.get
        for clause in self.clauses:
            v = get_state(clause, None)
            if v is not None:
                yield v.string

    def compile(self):
        """ :see::meth:SELECT.compile """
        try:
            table = self.orm.state.get('INTO').args[0]
        except AttributeError:
            table = self.orm.table
        self.string = ("UPDATE %s %s" %
                       (table, " ".join(self.evaluate_state()))).strip()
        return self.string


class Delete(Query):
    """ ======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        ======================================================================
        Creates and executes a simple |DELETE| query
        ..
            # Tells the ORM to set a WHERE clause
            orm.where(orm.username == 'TurkeyTom')
            # Deletes the user 'TurkeyTom' from the 'users' model
            q = Delete(orm)
            q.query
            # Executes the query
            cursor = q.execute()
        ..
        |"DELETE FROM users WHERE username = 'TurkeyTom'"|
    """
    __querytype__ = "DELETE"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', 'one', 'string')

    def __init__(self, orm, **kwargs):
        """`DELETE`
            ==================================================================
            @orm: :class:ORM object
        """
        super().__init__(orm=orm, **kwargs)
        self.compile()

    clauses = ('FROM', 'USING', 'WHERE', 'RETURNING')

    def evaluate_state(self):
        """ :see::meth:SELECT.evaluate_state """
        get_state = self.orm.state.get
        for clause in self.clauses:
            v = get_state(clause, None)
            if v is not None:
                yield v.string

    def compile(self):
        """ :see::meth:SELECT.compile """
        self.string = "DELETE %s" % (" ".join(self.evaluate_state()).strip())
        return self.string
