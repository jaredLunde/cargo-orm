#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital ORM Statements`
  ``Objects for SQL statement generation``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

"""
import re
from copy import copy

try:
    import ujson as json
except ImportError:
    import json

import psycopg2
import psycopg2.extras

from vital import logg
from vital.tools.dicts import merge_dict
from vital.debug import prepr, line

from vital.sql.etc.types import *
from vital.sql.exceptions import *
from vital.sql.expressions import *
from vital.sql.fields import *


__all__ = (
    "DELETE",
    "Delete",
    "EXCEPT",
    "Except",
    "INSERT",
    "Insert",
    "INTERSECT",
    "Intersect",
    "Intersections",
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


class BaseQuery(BaseLogic, StringLogic, NumericLogic, DateTimeLogic):
    """ Base query object, provides several common methods for the
        various query statement types
    """
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string')
    newline_re = re.compile(r"""\n+""")

    def __init__(self, orm, params=None):
        self.orm = orm
        self.params = params or orm.state.params
        self.alias = None
        self.is_subquery = orm.state.is_subquery
        self._with = orm._with
        self.recursive = None
        self.one = orm.state.one
        self.string = None

    def __str__(self):
        return self.query if self.query else self.__repr__()

    def __enter__(self):
        """ Creates a :class:WITH statement

            -> |self.orm|
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``
            ..
                tn = aliased('tn')
                n = aliased('n')
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

    @property
    def query_string(self):
        """ Used for :desc:__repr__ display """
        return "\n" + self.string

    @property
    def query(self):
        if self.string:
            return self.newline_re.sub(r" ", self.string).strip()

    def _set_clauses(self, query_clauses):
        """ Removes empty clause zones
            -> :class:psycopg2.extensions.cursor
        """
        self.ordered_clauses = list(filter(
            lambda x: x is not _empty, query_clauses))

    def execute(self):
        """ Executes :prop:query in the :prop:orm """
        if self.orm._with:
            query = " ".join(q.query for q in self.orm.queries)
            params = merge_dict(*list(q.params for q in self.orm.queries))
        else:
            query = self.query
            params = self.params
        if not self.orm._many:
            return self.orm.execute(query, params)
        else:
            return self.orm.run_many(fetchall=False)

    def debug(self):
        """ Prints the query string with its parameters """
        logg("\n" + self.string).notice('Query')
        logg(self.params).notice('Query Params')


class Query(BaseQuery):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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

    def __init__(self, orm, query=None, alias=None, recursive=None,
                 params=None):
        """ ``Query Statement``
            @orm: (:class:ORM) object
            @query: (#str) raw query string
            @alias: (#str) query alias name if it's a :class:WITH or
                :class:Intersections query
            @recursive: (#list) or #tuple used for :class:WITH and
                :class:Intersections queries, creates a |RECURSIVE|
                clause
            @params: (#dict) for parameterizing the query
                |"SELECT %(abc)" % {'abc': 123}|
        """
        super().__init__(orm, params=params)
        self.string = query.strip() if query else query
        self.alias = alias or self.orm.state.alias
        self.alias = str(self.alias) if self.alias else None
        self.recursive = recursive

    @prepr(('query', 'purple'), 'params', _break=True, _pretty=True)
    def __repr__(self): return


class Intersections(Query):
    """ Base structure for :class:UNION, :class:EXCEPT and :class:INTERSECTION
        queries.
    """
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'intersections', 'all', 'distinct')

    def __init__(self, orm, *args, all=None, distinct=None, **kwargs):
        super().__init__(orm, **kwargs)
        self.intersections = args or []
        self.all = all
        self.distinct = distinct

    def __and__(self, other):
        """ Creates |UNION| query

            -> :class:UNION query object
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the union query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                union = q1 & q2
            ..
            |<vital.sql.statements.UNION(                            |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the union query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                union = q1 + q2
            ..
            |<vital.sql.statements.UNION(                            |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the union query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                union = q1 - q2
            ..
            |<vital.sql.statements.UNION(                           |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the except query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                _except = q1 < q2
            ..
            |<vital.sql.statements.EXCEPT(                          |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the except query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                _except = q1 <= q2
            ..
            |<vital.sql.statements.EXCEPT(                          |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the except query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                _except = q1 << q2
            ..
            |<vital.sql.statements.EXCEPT(                          |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the intersect query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                intersect = q1 > q2
            ..
            |<vital.sql.statements.INTERSECT(                       |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the intersect query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                intersect = q1 >= q2
            ..
            |<vital.sql.statements.INTERSECT(                       |
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
            - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            ``Usage Example``

            Creates the intersect query
            ..
                q1 = Query(ORM(), query="SELECT * FROM users WHERE true")
                q2 = Query(
                    ORM(), query="SELECT * FROM users_followers WHERE true")
                intersect = q1 >> q2
            ..
            |<vital.sql.statements.INTERSECT(                       |
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
        for intersection in self.intersections:
            self.alias = intersection.alias if hasattr(intersection, 'alias') \
                else None
            self.recursive = intersection.recursive \
                if hasattr(intersection, 'recursive') else None
            if self.alias:
                break

    def compile(self, intersections=None, all=None, distinct=None):
        """ :see::meth:SELECT.compile """
        self.intersections = \
            intersections if intersections is not None else self.intersections
        self.all = all if all is not None else self.all
        self.distinct = distinct if distinct is not None else self.distinct
        intersection_str = self._set_intersection_str(self.all, self.distinct)
        new_intersection_str = intersection_str.join(
            i.query if i.query else ""
            for i in self.intersections
        )
        self.string = "{} {} {}".format(
            self.string if self.string else "",
            intersection_str if self.string else "",
            new_intersection_str
        )
        self.params = merge_dict(
            self.params, *[q.params for q in self.intersections])
        self._attach_alias()
        self.orm.reset()
        return self.string


class UNION(Intersections):
    """ Creates a UNION statement between multiple :class:BaseQuery objects

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
                 'one', 'string', 'intersections', 'all', 'distinct')

    def __init__(self, orm, *unions, all=False, distinct=False, **kwargs):
        """ `UNION`
            ``Query Statement``
            @orm: :class:ORM object
            @*unions: :class:BaseQuery objects
            @all: #bool True if |UNION ALL|
            @distinct: #bool True if |UNION DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:Intersections query
            @recursive: #list or #tuple used for :class:WITH and
                :class:Intersections queries, creates a |RECURSIVE|
                clause
        """
        super().__init__(orm, *unions, all=all, distinct=distinct, **kwargs)
        self.compile()


# PEP 8 compliance
Union = UNION


class INTERSECT(Intersections):
    """ Creates an |INTERSECT| statement between multiple :class:BaseQuery
        objects

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
                 'one', 'string', 'intersections', 'all', 'distinct')

    def __init__(self, orm, *intersects, all=False, distinct=False, **kwargs):
        """ `INTERSECT`
            ``Query Statement``
            @orm: :class:ORM object
            @*intersects: :class:BaseQuery objects
            @all: #bool True if |INTERSECT ALL|
            @distinct: #bool True if |INTERSECT DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:Intersections query
            @recursive: #list or #tuple used for :class:WITH and
                :class:Intersections queries, creates a |RECURSIVE|
                clause
        """
        super().__init__(orm, *intersects, all=all, distinct=distinct,
                         **kwargs)
        self.compile()


# PEP 8 compliance
Intersect = INTERSECT


class EXCEPT(Intersections):
    """ Creates an |EXCEPT| statement between multiple :class:BaseQuery objects

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
                 'one', 'string', 'intersections', 'all', 'distinct')

    def __init__(self, orm, *excepts, all=False, distinct=False, **kwargs):
        """ `INTERSECT`
            ``Query Statement``
            @orm: :class:ORM object
            @*excepts: :class:BaseQuery objects
            @all: #bool True if |EXCEPT ALL|
            @distinct: #bool True if |EXCEPT DISTINCT|
            @alias: #str query alias name if it's a :class:WITH or
                :class:Intersections query
            @recursive: #list or #tuple used for :class:WITH and
                :class:Intersections queries, creates a |RECURSIVE|
                clause
        """
        super().__init__(orm, *excepts, all=all, distinct=distinct, **kwargs)
        self.compile()


# PEP 8 compliance
Except = EXCEPT


class RAW(Intersections):
    """ Evaluates :class:QueryState clauses within the :mod:orm
        in the order in which they are declared, does not attempt to
        self-order at all.

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            RAW(ORM().values(1))
        ..
        |<vital.sql.statements.RAW(query_string=`VALUES (%(c5R053jXaKT4)s)`, |
        |   params={'c5R053jXaKT4': 1}):0x7f5f022ddf60>                       |
    """
    __querytype__ = "RAW"
    __slots__ = ('orm', 'params', 'alias', 'is_subquery', '_with', 'recursive',
                 'one', 'string', 'intersections', 'all', 'distinct')

    def __init__(self, orm, *args, **kwargs):
        """ `RAW`
            :see::meth:Query.__init__
        """
        super().__init__(orm, *args, **kwargs)
        self.compile()

    def _evaluate_state(self):
        """ :see::meth:SELECT._evaluate_state """
        self.params.update(self.orm.state.params)
        query_clauses = []
        add_clause = query_clauses.append
        for state in self.orm.state:
            if isinstance(state, list):
                query_clauses.extend((s.string for s in state))
            else:
                add_clause(state.string)
        self._set_clauses(query_clauses)

    def compile(self):
        """ :see::meth:SELECT.compile """
        self._evaluate_state()
        self.string = " ".join(self.ordered_clauses)
        return self.string


# PEP 8 compliance
Raw = RAW


#
#  `` Query Objects by Type ``
#


class INSERT(Query):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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

    def __init__(self, orm, *fields, **kwargs):
        """ `INSERT`
            ``Query Statement``
            @orm: :class:ORM object
            @fields: :class:Field fields to include in the |INSERT|
                with |VALUES|. If none are given, all of @orm.fields are used.
        """
        super().__init__(orm, **kwargs)
        self.fields = list(fields)
        if not orm.state.has('VALUES') or (self.orm._many and fields)\
           or self.orm._many:
            self._set_values()
        self.compile()

    def _set_values(self):
        """ Prepares the model :class:Field values for insertion into the DB
        """
        if not self.fields and hasattr(self.orm, 'fields'):
            self.fields = self.orm.fields
        vals = self.orm.values(*self.fields)
        if self.orm._many == 'BEGIN':
            self.orm.add_query(self)
            self.orm._many = True

    clauses = ('INTO', '_FIELDS', 'VALUES', 'RETURNING', 'ON')

    def _evaluate_state(self):
        """ :see::meth:SELECT._evaluate_state """
        if not self.orm.state.has('INTO'):
            if self.orm.table:
                table = self.orm.table
            elif self.fields:
                table = self.fields[0].table
            self.orm.into(table)
        vals = []
        query_clauses = [_empty] * len(self.clauses)
        for x, clause in enumerate(self.clauses):
            state = self.orm.state.get(clause, _empty)
            if isinstance(state, list):
                vals = state
            else:
                query_clauses[x] = state.string
        # Insert fields
        insert_fields = ", ".join(f.field_name for f in self.fields)
        query_clauses[1] = "({})".format(insert_fields)
        # Insert VALUES
        vals = (val.string.replace("VALUES ", "", 1) for val in vals)
        query_clauses[2] = "VALUES {}".format(", ".join(vals))
        self._set_clauses(query_clauses)
        self.params.update(self.orm.state.params)

    def compile(self):
        """ :see::meth:SELECT.compile """
        self._evaluate_state()
        self.string = "INSERT {}".format(
          " ".join(self.ordered_clauses)
        )
        return self.string


# PEP 8 compliance
Insert = INSERT


class SELECT(Intersections):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
                 'one', 'string', 'intersections', 'all', 'distinct', 'fields')

    def __init__(self, orm, *fields, **kwargs):
        """ `SELECT`
            ``Query Statement``
            @orm: :class:ORM object
            @fields: :class:Field fields to SELECT from the table
        """
        super().__init__(orm, **kwargs)
        self.fields = []
        self._set_fields(*fields)
        self.compile()

    def _set_fields(self, *fields):
        """ Formats the @fields received, merges their :prop:params with
            |self.params|
        """
        self.fields = []
        add_field = self.fields.append
        for field in fields:
            if isinstance(field, Field):
                add_field(field.name)
            elif isinstance(field, (Subquery, Query)):
                add_field(field.query)
            elif isinstance(field, (BaseExpression, aliased, safe)):
                add_field(str(field))
            else:
                field = Parameterize(field)
                add_field(field.string)
            if hasattr(field, 'params'):
                self.params.update(field.params)

    clauses = ('FROM', 'JOIN', 'WHERE', 'GROUP BY', 'HAVING', 'WINDOW',
               'ORDER BY', 'LIMIT', 'OFFSET', 'FETCH', 'FOR')

    def _evaluate_state(self):
        """ Evaluates the :class:Clause objects in :class:QueryState """
        query_clauses = [_empty] * len(self.clauses)
        joins = []
        for x, clause in enumerate(self.clauses):
            state = self.orm.state.get(clause, _empty)
            if isinstance(state, list):
                joins = (c.string for c in state)
            else:
                query_clauses[x] = state.string
        self.params.update(self.orm.state.params)
        joins = " ".join(joins)
        if len(joins):
            query_clauses[1] = joins
        self._set_clauses(query_clauses)

    def compile(self):
        """ Compiles a query string from the :class:QueryState """
        self._evaluate_state()
        if self.fields:
            fields = ", ".join(map(str, self.fields))
        else:
            fields = "*"
        self.string = "SELECT {} {}".format(
            fields,
            " ".join(self.ordered_clauses) if self.ordered_clauses else ""
        )
        return self.string


# PEP 8 compliance
Select = SELECT


class UPDATE(Query):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
                 'one', 'string', 'fields')

    def __init__(self, orm, *fields, **kwargs):
        """ `UPDATE`
            ``Query Statement``
            @orm: :class:ORM object
        """
        super().__init__(orm, **kwargs)
        self.fields = []
        if not self.orm.state.has('SET'):
            self._set_set(*fields)
        self.compile()

    def _set_set(self, *fields):
        """ Formats the @fields received, merges their :prop:params with
            |self.params|
        """
        if hasattr(self.orm, 'fields') and not fields:
            fields = self.orm.fields
        if len(fields):
            self.fields = fields
            self.orm.set(*self.fields)

    clauses = ('SET', 'FROM', 'WHERE', 'RETURNING')

    def _evaluate_state(self):
        """ :see::meth:SELECT._evaluate_state """
        self.params.update(self.orm.state.params)
        self._set_clauses((self.orm.state.get(clause, _empty).string
                           for clause in self.clauses))

    def compile(self):
        """ :see::meth:SELECT.compile """
        self._evaluate_state()
        table = None
        if not self.orm.table:
            #: Sets the UPDATE table
            for field in self.fields:
                if hasattr(field, 'table'):
                    table = field.table
                    break
        self.string = "UPDATE {} {}".format(
          table or self.orm.table,
          " ".join(self.ordered_clauses) if self.ordered_clauses else ""
        )
        return self.string


# PEP 8 compliance
Update = UPDATE


class DELETE(Query):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
        super().__init__(orm, **kwargs)
        self.compile()

    clauses = ('FROM', 'USING', 'WHERE', 'RETURNING')

    def _evaluate_state(self):
        """ :see::meth:SELECT._evaluate_state """
        if not self.orm.state.has('FROM'):
            self.orm.from_()
        self.params.update(self.orm.state.params)
        self._set_clauses((self.orm.state.get(clause, _empty).string
                           for clause in self.clauses))

    def compile(self):
        """ :see::meth:SELECT.compile """
        self._evaluate_state()
        self.string = "DELETE {}".format(
          " ".join(self.ordered_clauses) if self.ordered_clauses else ""
        )
        return self.string


# PEP 8 compliance
Delete = DELETE


class WITH(Query):
    """ Creates a |WITH| statement
        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Examples``
        ..
            t = aliased('t')
            n = aliased('n')
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
            t = aliased('t')
            n = aliased('n')
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
            if hasattr(r, 'recursive') and r.recursive:
                recursive = r.recursive
        super().__init__(orm, recursive=recursive, **kwargs)
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
            if isinstance(self.recursive, (list, set, tuple)):
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
