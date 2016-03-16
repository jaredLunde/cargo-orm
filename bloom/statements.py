"""

  `Bloom ORM Statements`
  ``Objects for SQL statement generation``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import re
from copy import copy

try:
    import ujson as json
except ImportError:
    import json

import psycopg2
import psycopg2.extras

from vital.cache import local_property
from vital.debug import Logg as logg
from vital.tools.dicts import merge_dict
from vital.debug import prepr, line

from bloom.clients import db
from bloom.etc.types import *
from bloom.exceptions import *
from bloom.expressions import *
from bloom.fields import *


__all__ = (
    "DELETE",
    "Delete",
    "EXCEPT",
    "Except",
    "INSERT",
    "Insert",
    "INTERSECT",
    "Intersect",
    "SetOperations",
    "Query",
    "RAW",
    "Raw",
    "SELECT",
    "Select",
    "UNION",
    "Union",
    "UPDATE",
    "Update",
    "WITH",
    "With"
)


#
#  `` Query Objects ``
#


class BaseQuery(StringLogic, DateTimeLogic):
    """ Base query object, provides several common methods for the
        various query statement types
    """
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'result')
    newline_re = re.compile(r"""\n+""")

    def __init__(self, query=None, params=None, orm=None):
        self.orm = orm or db
        try:
            self.params = params or self.orm.state.params
            self.is_subquery = self.orm.state.is_subquery
            self.one = self.orm.state.one
            self._with = self.orm._with
        except AttributeError:
            self.params = params
            self.is_subquery = False
            self.one = False
            self._with = False
        self.alias = None
        self.recursive = None
        self.string = query.strip() if query else query

    def __str__(self):
        return self.query if self.query else self.__repr__()

    def __enter__(self):
        """ Creates a :class:WITH statement

            -> |self.orm|
            ===================================================================
            ``Usage Example``
            ..
                tn = safe('tn')
                n = safe('n')
                with (
                  RAW(ORM().values(1), alias=tn, recursive=(n,)) +
                  SELECT(ORM().use(tn), n+1)
                ) as sub:
                    sub.use(tn).limit(10).select(n)
                    '''
                    WITH RECURSIVE tn(n) AS (
                        VALUES (1)
                        UNION ALL
                        SELECT n + 1 FROM tn
                    )
                    SELECT n FROM tn LIMIT 10
                    '''
                print(sub.result)
            ..
            |[{'n': 1}, {'n': 2}, {'n': 3}, {'n': 4}, {'n': 5},   |
            |   {'n': 6}, {'n': 7}, {'n': 8}, {'n': 9}, {'n': 10}]|
        """
        if not isinstance(self, WITH):
            WITH(self.orm, self)
            return self.orm
        else:
            return self.orm

    def __exit__(self, type=None, value=None, tb=None):
        """ Executes the :class:WITH statement, creates a 'result'
            attribute in |self| with access to the fetched data.
        """
        self.result = self.orm.result = self.execute().fetchall()
        self.orm.reset()

    def _filter_empty(self, els):
        return filter(lambda x: x is not _empty, els)

    @property
    def query(self):
        if self.string:
            return self.string.strip()

    @property
    def mogrified(self):
        """ -> (#str) the query post-parameterization """
        cur = self.orm.db.cursor()
        return cur.mogrify(self.string, self.params).decode()

    def compile(self):
        return self.query

    def execute(self):
        """ Executes :prop:query in the :prop:orm """
        if self.orm._with:
            query = " ".join(q.query for q in self.orm.queries)
            params = merge_dict(*list(q.params for q in self.orm.queries))
        else:
            query = self.query
            params = self.params
        return self.orm.execute(query, params)

    def debug(self):
        """ Prints the query string with its parameters """
        logg("\n" + self.string).notice('Query')
        logg(self.params).notice('Query Params')


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
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string')

    def __init__(self, query=None, params=None, alias=None, recursive=None,
                 orm=None,):
        """`Query Statement`
            @query: (#str) raw query string
            @params: (#dict) for parameterizing the query
                |"SELECT %(abc)" % {'abc': 123}|
            @alias: (#str) query alias name if it's a :class:WITH or
                :class:SetOperations query
            @recursive: (#list) or #tuple used for :class:WITH and
                :class:SetOperations queries, creates a |RECURSIVE|
                clause
            @orm: (:class:ORM) object
        """
        super().__init__(query, params, orm=orm)
        if orm:
            self.alias = alias or self.orm.state.alias
        else:
            self.alias = alias
        self.alias = str(self.alias) if self.alias else None
        self.recursive = recursive

    @prepr('query', 'params', _no_keys=True)
    def __repr__(self): return

    def exists(self, alias=None):
        return Functions.exists(self, alias=alias)

    def not_exists(self, alias=None):
        return Functions.not_exists(self, alias=alias)


class SetOperations(Query):
    """ Base structure for :class:UNION, :class:EXCEPT and :class:INTERSECTION
        queries.
    """
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'operations', 'all', 'distinct')

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
            |<bloom.statements.UNION(                                |
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
        return UNION(self.orm, self, other)

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
            |<bloom.statements.UNION(                                |
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
        return UNION(self.orm, self, other, all=True)

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
            |<bloom.statements.UNION(                                |
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
        return UNION(self.orm, self, other, distinct=True)

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
            |<bloom.statements.EXCEPT(                               |
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
        return EXCEPT(self.orm, self, other)

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
            |<bloom.statements.EXCEPT(                               |
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
        return EXCEPT(self.orm, self, other, all=True)

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
            |<bloom.statements.EXCEPT(                               |
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
        return EXCEPT(self.orm, self, other, distinct=True)

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
            |<bloom.statements.INTERSECT(                            |
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
        return INTERSECT(self.orm, self, other)

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
            |<bloom.statements.INTERSECT(                            |
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
        return INTERSECT(self.orm, self, other, all=True)

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
            |<bloom.statements.INTERSECT(                            |
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
        return INTERSECT(self.orm, self, other, distinct=True)

    intersect_distinct = __rshift__

    def _set_intersection_str(self, all, distinct):
        """ Formats the statement with |ALL| and |DISTINCT| when specified """
        intersection_str = "{} {}"\
            .format(self.__querytype__, "ALL" if all else "").strip()
        intersection_str = " {} {} "\
            .format(intersection_str, "DISTINCT" if distinct else "")
        return intersection_str.rstrip() + ' '

    def _attach_alias(self):
        """ Attaches alias and |RECURSIVE| statement when specified """
        for intersection in self.operations:
            self.alias = intersection.alias if hasattr(intersection, 'alias') \
                else None
            self.recursive = intersection.recursive \
                if hasattr(intersection, 'recursive') else None
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
        self.params = merge_dict(
            self.params, *[q.params for q in self.operations])
        self._attach_alias()
        self.orm.reset()
        return self.string


class UNION(SetOperations):
    """ Creates a UNION statement between multiple :class:BaseQuery objects

        =======================================================================
        ``Usage Example``
        Creates the |UNION|
        ..
            q1 = SELECT(SomeORM())
            q2 = SELECT(SomeORM())
            union = UNION(SomeORM(), q1, q2)
        ..
        |q1.query UNION q2.query|

        Executes the |UNION|
        ..
            cursor = union.execute()
        ..
    """
    __querytype__ = "UNION"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'operations', 'all', 'distinct')

    def __init__(self, orm, *unions, all=False, distinct=False, **kwargs):
        """ `UNION`
            ``Query Statement``
            @orm: :class:ORM object
            @*unions: :class:BaseQuery objects
            @all: #bool True if |UNION ALL|
            @distinct: #bool True if |UNION DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:SetOperations query
            @recursive: #list or #tuple used for :class:WITH and
                :class:SetOperations queries, creates a |RECURSIVE|
                clause
        """
        super().__init__(orm, *unions, all=all, distinct=distinct,
                         **kwargs)
        self.compile()


# PEP 8 compliance
Union = UNION


class INTERSECT(SetOperations):
    """ Creates an |INTERSECT| statement between multiple :class:BaseQuery
        objects

        =======================================================================
        ``Usage Example``

        Creates the |INTERSECT|
        ..
            q1 = SELECT(SomeORM())
            q2 = SELECT(SomeORM())
            intersect = INTERSECT(SomeORM(), q1, q2)
        ..
        |q1.query INTERSECT q2.query|

        Executes the |INTERSECT|
        ..
            cursor = intersect.execute()
        ..
    """
    __querytype__ = "INTERSECT"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'operations', 'all', 'distinct')

    def __init__(self, orm, *intersects, all=False, distinct=False, **kwargs):
        """ `INTERSECT`
            ``Query Statement``
            @orm: :class:ORM object
            @*intersects: :class:BaseQuery objects
            @all: #bool True if |INTERSECT ALL|
            @distinct: #bool True if |INTERSECT DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:SetOperations query
            @recursive: #list or #tuple used for :class:WITH and
                :class:SetOperations queries, creates a |RECURSIVE|
                clause
        """
        super().__init__(orm, *intersects, all=all, distinct=distinct,
                         **kwargs)
        self.compile()


# PEP 8 compliance
Intersect = INTERSECT


class EXCEPT(SetOperations):
    """ Creates an |EXCEPT| statement between multiple :class:BaseQuery objects

        =======================================================================
        ``Usage Example``

        Creates the |EXCEPT|
        ..
            q1 = SELECT(SomeORM())
            q2 = SELECT(SomeORM())
            except_ = EXCEPT(SomeORM(), q1, q2)
        ..
        |q1.query EXCEPT q2.query|

        Executes the |EXCEPT|
        ..
            cursor = except_.execute()
        ..
    """
    __querytype__ = "EXCEPT"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'operations', 'all', 'distinct')

    def __init__(self, orm, *excepts, all=False, distinct=False, **kwargs):
        """ `INTERSECT`
            ``Query Statement``
            @orm: :class:ORM object
            @*excepts: :class:BaseQuery objects
            @all: #bool True if |EXCEPT ALL|
            @distinct: #bool True if |EXCEPT DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:SetOperations query
            @recursive: #list or #tuple used for :class:WITH and
                :class:SetOperations queries, creates a |RECURSIVE|
                clause
        """
        super().__init__(orm, *excepts, all=all, distinct=distinct,
                         **kwargs)
        self.compile()


# PEP 8 compliance
Except = EXCEPT


class RAW(SetOperations):
    """ Evaluates :class:QueryState clauses within the :mod:orm
        in the order in which they are declared, does not attempt to
        self-order at all.

        =======================================================================
        ``Usage Example``
        ..
            RAW(ORM().values(1))
        ..
        |<bloom.statements.RAW(query_string=`VALUES (%(c5R053jXaKT4)s)`,      |
        |   params={'c5R053jXaKT4': 1}):0x7f5f022ddf60>                       |
    """
    __querytype__ = "RAW"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'operations', 'all', 'distinct')

    def __init__(self, *args, **kwargs):
        """ `RAW`
            :see::meth:Query.__init__
        """
        super().__init__(*args, **kwargs)
        self.compile()

    def evaluate_state(self):
        """ :see::meth:SELECT.evaluate_state """
        self.params.update(self.orm.state.params)
        for state in self.orm.state:
            if isinstance(state, list):
                for s in state:
                    yield s.string
            else:
                yield state.string

    def compile(self):
        """ :see::meth:SELECT.compile """
        self.string = " ".join(self.evaluate_state())
        return self.string


# PEP 8 compliance
Raw = RAW


#
#  `` Query Objects by Type ``
#


class INSERT(Query):
    """ =======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        =======================================================================

        Creates and executes a simple |INSERT| query
        ..
            # Populates the username field of the ORM
            orm['username'] = 'FriskyWolf'

            # Inserts the record into the 'users' model
            q = INSERT(orm)
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
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'fields')

    def __init__(self, orm, **kwargs):
        """ `INSERT`
            ``Query Statement``
            @orm: :class:ORM object
        """
        super().__init__(orm=orm, **kwargs)
        self.compile()

    clauses = ('INTO', '_FIELDS', 'VALUES', 'RETURNING', 'ON')

    def evaluate_state(self):
        """ :see::meth:SELECT.evaluate_state """
        vals = []
        query_clauses = [_empty] * len(self.clauses)
        for state in self.orm.state:
            if isinstance(state, list):
                vals = state
            else:
                query_clauses[self.clauses.index(state.clause)] = state.string
        # Insert fields
        query_clauses[1] = "(%s)" % ", ".join(f.field_name
                                              for f in self.orm.state.fields)
        # Insert VALUES
        query_clauses[2] = "VALUES %s" % ", ".join(
            val.string.replace("VALUES ", "", 1) for val in vals)
        self.params.update(self.orm.state.params)
        return self._filter_empty(query_clauses)

    def compile(self):
        """ :see::meth:SELECT.compile """
        self.string = "INSERT %s" % " ".join(self.evaluate_state())
        return self.string


# PEP 8 compliance
Insert = INSERT


class SELECT(SetOperations):
    """ =======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        =======================================================================
        Creates and executes a simple |SELECT| query
        ..
            # Selects the field 'username' from the users model
            q = SELECT(orm, orm.username)
            q.query
        ..
        |'SELECT users.username FROM users'|

        ..
            cursor = q.execute()
            cursor.fetchall()
        ..
        |[{'username': 'FriskyWolf'}, {'username': 'test'}]|

        =======================================================================
        Creates and executes a simple |SELECT| query with a |WHERE| clause
        ..
            # Sets the 'WHERE' clause
            orm.where(orm.username == 'FriskyWolf')

            # Selects the field 'username' from the users model
            q = SELECT(orm, orm.username)
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
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'operations', 'all', 'distinct')

    def __init__(self, orm, **kwargs):
        """ `SELECT`
            ``Query Statement``
            @orm: :class:ORM object
            @fields: :class:Field fields to SELECT from the table
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
                yield field.name
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

    clauses = ('FROM', 'JOIN', 'WHERE', 'GROUP BY', 'HAVING', 'WINDOW',
               'ORDER BY', 'LIMIT', 'OFFSET', 'FETCH', 'FOR')

    def evaluate_state(self):
        """ Evaluates the :class:Clause objects in :class:QueryState """
        query_clauses = [_empty] * len(self.clauses)
        joins = []
        for state in self.orm.state:
            try:
                query_clauses[self.clauses.index(state.clause)] = state.string
            except (ValueError, AttributeError):
                joins = (c.string for c in state)

        self.params.update(self.orm.state.params)
        if joins:
            query_clauses[1] = " ".join(joins)
        return self._filter_empty(query_clauses)

    def compile(self):
        """ Compiles a query string from the :class:QueryState """
        self.string = (
            "SELECT %s %s" % (", ".join(self.fields) or "*",
                              " ".join(self.evaluate_state()))).strip()
        return self.string


# PEP 8 compliance
Select = SELECT


class UPDATE(Query):
    """ =======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        =======================================================================
        Creates and executes a simple |UPDATE| query
        ..
            # Tells the ORM which fields to update
            orm.set(orm.username == 'TurkeyTom')
            # Tells the ORM to set a WHERE clause
            orm.where(orm.username == 'FriskyWolf')
            # Updates the field 'username' from the users model
            q = UPDATE(orm)
            q.query
            # Executes the query
            cursor = q.execute()
        ..
        |"UPDATE users SET username = 'TurkeyTom' |
        | WHERE username = 'FriskyWolf'"          |
    """
    __querytype__ = "UPDATE"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string')

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
        self.params.update(self.orm.state.params)
        get_state = self.orm.state.get
        for clause in self.clauses:
            v = get_state(clause, _empty).string
            if v is not _empty:
                yield v

    def compile(self):
        """ :see::meth:SELECT.compile """
        self.string = (
            "UPDATE %s %s" % (self.orm.state.get('INTO', self.orm.table),
                              " ".join(self.evaluate_state()))
        ).strip()
        return self.string


# PEP 8 compliance
Update = UPDATE


class DELETE(Query):
    """ =======================================================================
        ``Usage Example``

        Creates a simple 'users' :class:Model with a :class:Username
        :class:Field
        ..
            orm = Model()
            orm.table = 'users'
            orm.add_field(username=Username())
        ..

        =======================================================================
        Creates and executes a simple |DELETE| query
        ..
            # Tells the ORM to set a WHERE clause
            orm.where(orm.username == 'TurkeyTom')
            # Deletes the user 'TurkeyTom' from the 'users' model
            q = DELETE(orm)
            q.query
            # Executes the query
            cursor = q.execute()
        ..
        |"DELETE FROM users WHERE username = 'TurkeyTom'"|
    """
    __querytype__ = "DELETE"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string')

    def __init__(self, orm, **kwargs):
        """ `DELETE`
            ``Query Statement``
            @orm: :class:ORM object
        """
        super().__init__(orm=orm, **kwargs)
        self.compile()

    clauses = ('FROM', 'USING', 'WHERE', 'RETURNING')

    def evaluate_state(self):
        """ :see::meth:SELECT.evaluate_state """
        self.params.update(self.orm.state.params)
        get_state = self.orm.state.get
        for clause in self.clauses:
            v = get_state(clause, _empty).string
            if v is not _empty:
                yield v

    def compile(self):
        """ :see::meth:SELECT.compile """
        self.string = "DELETE %s" % (" ".join(self.evaluate_state()).strip())
        return self.string


# PEP 8 compliance
Delete = DELETE


class WITH(Query):
    """ Creates a |WITH| statement
        =======================================================================
        ``Usage Examples``
        ..
            t = safe('t')
            n = safe('n')
            q = WITH(
                self.orm,
                RAW(ORM().values(1), alias=t, recursive=(n,)) +
                SELECT(ORM().use(t), n+1))
            q.orm.use(t).limit(10).select(n)
            result = q.execute().fetchall()
        ..
        |[{'n': 1}, {'n': 2}, {'n': 3}, {'n': 4}, {'n': 5},   |
        |   {'n': 6}, {'n': 7}, {'n': 8}, {'n': 9}, {'n': 10}]|

        ..
            t = safe('t')
            n = safe('n')
            with (
              RAW(ORM().values(1), alias=t, recursive=(n,)) +
              SELECT(ORM().use(t), n+1)
            ) as orm:
                orm.use(t).limit(10).select(n)
            print(sub.result)
        ..
        |[{'n': 1}, {'n': 2}, {'n': 3}, {'n': 4}, {'n': 5},   |
        |   {'n': 6}, {'n': 7}, {'n': 8}, {'n': 9}, {'n': 10}]|

    """
    __querytype__ = "WITH"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'queries')

    def __init__(self, orm, *queries, **kwargs):
        """ `WITH`
            :see::meth:Query.__init__
        """
        recursive = None
        for r in queries:
            try:
                recursive = r.recursive
            except AttributeError:
                continue
        super().__init__(orm=orm, recursive=recursive, **kwargs)
        self.queries = queries
        self.orm._with = True
        self.orm.alias = kwargs.get('alias') or self.alias
        self.compile()

    def __str__(self):
        return self.query if self.query else ""

    def compile(self):
        """ :see::meth:SELECT.compile """
        as_ = []
        add_as = as_.append
        for query in self.queries:
            alias = query.alias
            recursive = ""
            if isinstance(self.recursive, (list, tuple)):
                recursive = ", ".join(map(str, self.recursive))
            add_as("{}{} AS (\n{}\n)".format(
              alias, "({})".format(recursive), query.query
            ))
            self.params.update(query.params)
        recursive = ""
        if self.recursive:
            recursive = "RECURSIVE "
        self.string = "WITH {}{}".format(recursive, ", ".join(as_))
        self.orm.add_query(self)

# PEP 8 compliance
With = WITH
