"""

  `Cargo SQL Expressions`
  ``Objects for creating SQL expressions, functions and clauses``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import random
import string
from hashlib import sha1

import psycopg2.extensions

from vital.debug import preprX
from cargo.etc import passwords, usernames, operators


__all__ = (
    "BaseExpression",
    "BaseLogic",
    "StringLogic",
    "BaseNumericLogic",
    "NumericLogic",
    "Case",
    "Clause",
    'CommaClause',
    'WrappedClause',
    'ValuesClause',
    "parameterize",
    "Expression",
    "Function",
    "WindowFunctions",
    "Functions",
    "F",
    "Subquery",
    "aliased",
    "safe",
    "_empty"
)


class BaseLogic(object):
    """ Logical expression wrappers for PostgreSQL """
    __slots__ = tuple()

    def and_(self, other):
        """ Creates an |AND| SQL expression

            -> (SQL) :class:Expression object
            ==================================================================

            ``Usage Example``

            *Note*: You must use parentheses around expressions to call this
                expression on them, as the '&' will otherwise operate on the 1
                in the example, rather than the Expression
            ..
                condition = (model.field == 1) & (model.field == 2)
                model.where(condition)
            ..
            |field = 1 AND field != 2|
        """
        return Expression(self, operators.AND, other)

    __and__ = and_
    also = and_

    def or_(self, other):
        """ Creates an |OR| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``

            *Note*: You must use parentheses around expressions to call this
                expression on them, as the '|' will otherwise operate on the 1
                in the example, rather than the Expression
            ..
                condition = (model.field == 1) | (model.field == 2)
                model.where(condition)
            ..
            |field = 1 OR field = 2|
        """
        return Expression(self, operators.OR, other)

    __or__ = or_
    or_else = or_

    def eq(self, other):
        """ Creates an |=| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field == 1
                model.where(condition)
            ..
            |field  = 1|
        """
        return Expression(self, operators.EQ, other)

    __eq__ = eq

    def not_eq(self, other):
        """ Creates a |<>| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field != 2
                model.where(condition)
            ..
            |field <> 2|
        """
        return Expression(self, operators.NE, other)

    __ne__ = not_eq

    def distinct(self, *args, **kwargs):
        """ :see::meth:F.distinct """
        return F.distinct(self, *args, **kwargs)

    def distinct_on(self, *args, **kwargs):
        """ :see::meth:F.distinct_on """
        return F.distinct_on(self, *args, **kwargs)

    def distinct_from(self, other):
        """ Creates a |DISTINCT FROM| SQL clause

            -> SQL :class:Clause object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.distinct_from()
                model.where(condition)
            ..
            |DISTINCT FROM field|
        """
        return Expression(self, "{} {} {}".format(
            operators.IS, operators.DISTINCT, operators.FROM), other)

    def not_distinct_from(self, other):
        """ Creates a |NOT DISTINCT FROM| SQL clause

            -> SQL :class:Clause object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.not_distinct_from()
                model.where(condition)
            ..
            |NOT DISTINCT FROM field|
        """
        op = "{} {} {} {}".format(
            operators.IS,
            operators.NOT,
            operators.DISTINCT,
            operators.FROM)
        return Expression(self, op, other)

    def in_(self, *others):
        """ Creates an |IN| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.in_(1, 2, 3, 4)
                model.where(condition)
            ..
            |field IN (1, 2, 3, 4)|
        """
        return Expression(self, operators.IN, others)

    is_in = in_

    def __rshift__(self, others):
        return self.is_in(*others)

    def not_in(self, *others):
        """ Creates a |NOT IN| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.not_in(1, 2, 3, 4)
                model.where(condition)
            ..
            |field NOT IN (1, 2, 3, 4)|
        """
        op = "{} {}".format(operators.NOT, operators.IN)
        return Expression(self, op, others)

    def __lshift__(self, others):
        return self.not_in(*others)

    def is_null(self):
        """ Creates a |IS NULL| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.is_null()
                model.where(condition)
            ..
            |field IS NULL|
        """
        return Expression(self, "{} {}".format(operators.IS, operators.NULL))

    def is_not_null(self):
        """ Creates a |IS NOT NULL| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.not_null()
                model.where(condition)
            ..
            |field IS NOT NULL|
        """
        return Expression(self, "{} {} {}".format(
            operators.IS, operators.NOT, operators.NULL))

    def asc(self, val=False):
        """ Creates an |ASC| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                model.order_by(model.field.asc())
            ..
            |field ASC|
        """
        return Expression(self if val is not True else self.real_value,
                          operators.ASC)

    def desc(self, val=False):
        """ Creates a |DESC| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                model.order_by(model.field.desc())
            ..
            |field DESC|
        """
        return Expression(self if val is not True else self.real_value,
                          operators.DESC)

    def nullif(self, other, alias=None, **kwargs):
        """ :see::meth:F.nullif """
        return F.nullif(self, other, alias=alias, **kwargs)

    def count(self, alias=None, **kwargs):
        """ :see::meth:F.count """
        return F.count(self, alias=alias, **kwargs)

    def using(self, b, **kwargs):
        """ :see::meth:F.using """
        return F.using(self, b, **kwargs)

    def lag(self, *expressions, offset=None, default=None,
            window_name=None, partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowF.lag """
        return Function('lag', self, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    def lead(self, *expressions, offset=None, default=None, window_name=None,
             partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowF.lead """
        return Function('lead', self, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    def first_value(self, *expressions, window_name=None,
                    partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowF.first_value """
        return Function('first_value', self).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    def last_value(self, *expressions, window_name=None,
                   partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowF.last_value """
        return Function('last_value', self).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    def nth_value(self, nth_integer, *expressions, window_name=None,
                  partition_by=None, order_by=None, alias=None):
        """ :see::meth:WindowF.nth_value """
        return Function('nth_value', self, nth_integer).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    def cast(self, as_):
        """ :see::meth:F.new """
        return F.new(self, as_)

    def func(self, *args, **kwargs):
        """ :see::meth:F.new """
        return F.new(args[0], self, *args[1:], **kwargs)


class BaseNumericLogic(BaseLogic):
    __slots__ = tuple()

    def lt(self, other):
        """ Creates a |<| (less than) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field < 4
                model.where(condition)
            ..
            |field < 4|
        """
        return Expression(self, operators.LT, other)

    __lt__ = lt

    def le(self, other):
        """ Creates a |<=| (less than or equal) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field <= 4
                model.where(condition)
            ..
            |field <= 4|
        """
        return Expression(self, operators.LE, other)

    __le__ = le

    def gt(self, other):
        """ Creates a |>| (greater than) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field > 4
                model.where(condition)
            ..
            |field > 4|
        """
        return Expression(self, operators.GT, other)

    __gt__ = gt

    def ge(self, other):
        """ Creates a |>=| (greater than or equal) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field >= 4
                model.where(condition)
            ..
            |field >= 4|
        """
        return Expression(self, operators.GE, other)

    __ge__ = ge

    def divide(self, other):
        """ Creates a |/| (division) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field / 4
                model.where(condition)
            ..
            |field / 4|
        """
        return Expression(self, operators.DIV, other)

    __truediv__ = divide
    __div__ = divide

    def multiply(self, other):
        """ Creates a |*| (multiplication) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field * 4
                model.where(condition)
            ..
            |field * 4|
        """
        return Expression(self, operators.MUL, other)

    __mul__ = multiply

    def add(self, other):
        """ Creates a |+| (addition) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field + 4
                model.where(condition)
            ..
            |field + 4|
        """
        return Expression(self, operators.ADD, other)

    __add__ = add

    def power(self, other):
        """ Creates a |^| (power) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = (model.field ** 2) > 144
                model.where(condition)
            ..
            |field ^ 2|
        """
        return Expression(self, operators.EXP, other)

    __pow__ = power

    def subtract(self, other):
        """ Creates a |-| (subtraction) SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field - 4
                model.where(condition)
            ..
            |field - 4|
        """
        return Expression(self, operators.SUB, other)

    __sub__ = subtract

    def max(self, alias=None, **kwargs):
        """ :see::meth:F.max """
        return F.max(self, alias=alias, **kwargs)

    def min(self, alias=None, **kwargs):
        """ :see::meth:F.min """
        return F.min(self, alias=alias, **kwargs)

    def avg(self, alias=None, **kwargs):
        """ :see::meth:F.avg """
        return F.avg(self, alias=alias, **kwargs)

    def between(self, *others):
        """ Creates a |BETWEEN| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.between(10, 20)
                model.where(condition)
            ..
            |field BETWEEN 10 AND 20|
        """
        return Expression(self,
                          operators.BETWEEN,
                          Expression(others[0], operators.AND, others[1]))

    def not_between(self, a, b):
        """ Creates a |NOT BETWEEN| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.not_between(10, 20)
                model.where(condition)
            ..
            |field NOT BETWEEN 10 AND 20|
        """
        return Expression(
            self, "{} {}".format(operators.NOT, operators.BETWEEN),
            Expression(a, operators.AND, b))

    def incr(self, by=1, **kwargs):
        """ Creates a SQL expression for incrementing numeric values

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.incr(2)
                model.where(condition)
            ..
            |field = field + 2|
        """
        return Expression(self,
                          operators.EQ,
                          Expression(self, operators.ADD, by),
                          **kwargs)

    def decr(self, by=1, **kwargs):
        """ Creates a SQL expression for decrementing numeric values

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.decr(2)
                model.where(condition)
            ..
            |field = field - 2|
        """
        return Expression(self,
                          operators.EQ,
                          Expression(self, operators.SUB, by),
                          **kwargs)


class NumericLogic(BaseNumericLogic):
    __slots__ = tuple()

    def sum(self, alias=None, **kwargs):
        """ :see::meth:F.sum """
        return F.sum(self, alias=alias, **kwargs)

    def abs(self, alias=None, **kwargs):
        """ :see::meth:F.abs """
        return F.abs(self, alias=alias, **kwargs)

    def atan(self, alias=None, **kwargs):
        """ :see::meth:F.atan """
        return F.atan(self, alias=alias, **kwargs)

    def atan2(self, y, alias=None, **kwargs):
        """ :see::meth:F.atan2 """
        return F.atan2(self, y, alias=alias, **kwargs)

    def acos(self, alias=None, **kwargs):
        """ :see::meth:F.acos """
        return F.acos(self, alias=alias, **kwargs)

    def asin(self, alias=None, **kwargs):
        """ :see::meth:F.asin """
        return F.asin(self, alias=alias, **kwargs)

    def cos(self, alias=None, **kwargs):
        """ :see::meth:F.cos """
        return F.cos(self, alias=alias, **kwargs)

    def cot(self, alias=None, **kwargs):
        """ :see::meth:F.cot """
        return F.cot(self, alias=alias, **kwargs)

    def degrees(self, alias=None, **kwargs):
        """ :see::meth:F.degrees """
        return F.degrees(self, alias=alias, **kwargs)

    def exp(self, alias=None, **kwargs):
        """ :see::meth:F.exp """
        return F.exp(self, alias=alias, **kwargs)

    def floor(self, alias=None, **kwargs):
        """ :see::meth:F.floor """
        return F.floor(self, alias=alias, **kwargs)

    def ceil(self, alias=None, **kwargs):
        """ :see::meth:F.ceil """
        return F.ceil(self, alias=alias, **kwargs)

    def log(self, *base, alias=None, **kwargs):
        """ :see::meth:F.log """
        return F.log(self, *base, alias=alias, **kwargs)

    def mod(self, dmod=0, alias=None, **kwargs):
        """ :see::meth:F.mod """
        return F.mod(self, dmod, alias=alias, **kwargs)

    __mod__ = mod

    def pow(self, power=0, alias=None, **kwargs):
        """ :see::meth:F.pow """
        return F.pow(self, power, alias=alias, **kwargs)

    def radians(self, alias=None, **kwargs):
        """ :see::meth:F.radians """
        return F.radians(self, alias=alias, **kwargs)

    def round(self, alias=None, **kwargs):
        """ :see::meth:F.round """
        return F.round(self, alias=alias, **kwargs)

    def sign(self, alias=None, **kwargs):
        """ :see::meth:F.sign """
        return F.sign(self, alias=alias, **kwargs)

    def sin(self, alias=None, **kwargs):
        """ :see::meth:F.sin """
        return F.sin(self, alias=alias, **kwargs)

    def sqrt(self, alias=None, **kwargs):
        """ :see::meth:F.sqrt """
        return F.sqrt(self, alias=alias, **kwargs)

    def tan(self, alias=None, **kwargs):
        """ :see::meth:F.tan """
        return F.tan(self, alias=alias, **kwargs)


class StringLogic(BaseLogic):
    __slots__ = tuple()

    def like(self, other):
        """ Creates a |LIKE| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.like('some%')
                model.where(condition)
            ..
            |field LIKE 'some%'|
        """
        return Expression(self, operators.LIKE, other)

    __mod__ = like

    def not_like(self, other):
        """ Creates a |NOT LIKE| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.not_like('some%')
                model.where(condition)
            ..
            |field NOT LIKE 'some%'|
        """
        op = "{} {}".format(operators.NOT, operators.LIKE)
        return Expression(self, op, other)

    def ilike(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.ilike('some%')
                model.where(condition)
            ..
            |field ILIKE 'some%'|
        """
        return Expression(self, operators.ILIKE, other)

    __xor__ = ilike

    def not_ilike(self, other):
        """ Creates a |NOT ILIKE| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.not_ilike('some%')
                model.where(condition)
            ..
            |field NOT ILIKE 'some%'|
        """
        op = "{} {}".format(operators.NOT, operators.ILIKE)
        return Expression(self, op, other)

    def startswith(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.startswith('hello')
                model.where(condition)
            ..
            |field ILIKE 'hello%'|
        """
        return Expression(self, operators.ILIKE, "{}%".format(other))

    def endswith(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.endswith('world')
                model.where(condition)
            ..
            |field ILIKE '%world'|
        """
        return Expression(self, operators.ILIKE, "%{}".format(other))

    def contains(self, other):
        """ Creates an |ILIKE| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.contains('llo wor')
                model.where(condition)
            ..
            |field ILIKE '%llo wor%'|
        """
        return Expression(self, operators.ILIKE, "%{}%".format(other))

    def similar_to(self, other):
        """ Creates a |SIMILAR TO| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.similar_to('%(b|d)%')
                model.where(condition)
            ..
            |field SIMILAR TO '%(b|d)%'|
        """
        return Expression(self, operators.SIMILAR_TO, other)

    def not_similar_to(self, other):
        """ Creates a |NOT SIMILAR TO| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.not_similar_to('%(b|d)%')
                model.where(condition)
            ..
            |field NOT SIMILAR TO '%(b|d)%'|
        """
        return Expression(
            self, "{} {}".format(operators.NOT, operators.SIMILAR_TO), other)

    def posix(self, other, op="~", invert=False):
        """ Creates a |POSIX| SQL expression

            -> SQL :class:Expression object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.posix('(b|d)', op='~')
                model.where(condition)
            ..
            |field ~ '(b|d)'|
        """
        return Expression(
            self if not invert else other, op, other if not invert else self)

    def concat_ws(self, *others, separator=',', **kwargs):
        """ :see::meth:F.concat_ws """
        return F.concat_ws(self, *others, separator=separator,
                                   **kwargs)

    def regexp_replace(self, pattern, repl, *flags, **kwargs):
        """ :see::meth:F.regexp_replace """
        return F.regexp_replace(self, pattern, repl, *flags, **kwargs)

    def regexp_matches(self, pattern, *flags, **kwargs):
        """ :see::meth:F.regexp_matches """
        return F.regexp_matches(self, pattern, *flags, **kwargs)

    def concat(self, *others, **kwargs):
        """ :see::meth:F.concat """
        return F.concat(self, *others, **kwargs)


class Subquery(NumericLogic, StringLogic):
    """ You must use this wrapper if you want to use subqueries within the
        models, as plain text is parameterized for safety.

        Incorrect::
        |model.where("(SELECT MAX(posts.likes) FROM posts) > 1000")|

        Corrected::
        ..
            model = Model()
            posts = PostsModel()

            # Creates a subquery which will not be executed until its parent
            # query is
            post.subquery()
            posts_subquery = posts.select(posts.likes.max())

            # Executes query in model
            model.where(posts_subquery > 1000)
            model.select()
        ..
    """
    __slots__ = ('subquery', 'alias', 'query', 'params')

    def __init__(self, query, alias=None):
        """ `Subquery`
            Thin wrapper for including subqueries within models.

            @query: :class:Query object
            @alias: #str name to alias the subquery with
        """
        self.subquery = query
        self.alias = str(alias) if alias else query.alias
        self.query = "({}) {}".format(
            query.query, self.alias if self.alias else "").strip()
        self.params = query.params

    __repr__ = preprX('query', 'params', keyless=True, address=False)

    def __str__(self):
        return self.query

    @property
    def string(self):
        return self.query

    def exists(self, alias=None):
        return F.exists(self, alias=alias)

    def not_exists(self, alias=None):
        return F.not_exists(self, alias=alias)

    def compile(self):
        return self


class BaseExpression(BaseLogic):
    __slots__ = tuple()

    def __str__(self):
        return self.string or ""

    def _get_param_key(self, item):
        """ Sets the parameter key for @item """
        key = hex(id(item))
        self.params[key] = item
        return "%(" + key + ")s"

    def _inherit_parameters(self, item):
        """ Inherits the parameters of other :mod:cargo objects into this
            one.

            @items: :mod:cargo objects
        """
        try:
            self.params.update(item.params)
        except (AttributeError, TypeError):
            pass

    def _parameterize(self, item, use_field_name=False):
        if isinstance(item, BaseLogic):
            #: These are already parameterized
            try:
                #: Expressions and functions
                exp = item.compile(use_field_name=use_field_name)
            except TypeError:
                #: Queries and other
                exp = item.string
            except AttributeError:
                #: Fields
                if item._alias:
                    exp = item._alias
                elif use_field_name:
                    exp = item.field_name
                else:
                    exp = item.name
        elif item is _empty:
            #: Empty item
            return ""
        else:
            #: Anything else gets parameterized
            exp = self._get_param_key(item)
        return exp

    def _compile_expressions(self, *items, use_field_name=False):
        """ Decides what to do with the @items this object receives based on
            the type of object it is receiving and formats accordingly,
            inheriting parameters when necessary.

            -> yields #str expression
        """
        for item in items:
            self._inherit_parameters(item)
            yield self._parameterize(item, use_field_name)

    def _join_expressions(self, string, *items, use_field_name=False):
        """ Joins @items with @string after @items have been parameterized
            and transformed to #str(s)

            -> #str joined expression string
        """
        return string.join(filter(None, self._compile_expressions(
                    *items, use_field_name=use_field_name)))


class __empty(object):
    __slots__ = tuple()
    __repr__ = preprX()

    @property
    def string(self):
        return self

    def __str__(self):
        return ""

    def __len__(self):
        return 0

    def __bool__(self):
        return False

    @staticmethod
    def to_db(val):
        return adapt(None).getquoted()


_empty = __empty()
psycopg2.extensions.register_adapter(__empty, __empty.to_db)


class Expression(BaseExpression, NumericLogic, StringLogic):
    """ You must use this wrapper if you want to make expressions within the
        models, as plain text is parameterized for safety.

        Incorrect::
        |model.where("model.id > 1000")|

        Corrected::
        |model.where(model.id > 1000)|
          or
         |model.where(model.id.gt(1000))|
           or
        |model.where(Expression(model.id, ">", 1000))|

        ==================================================================
        ``Usage Example``
        ..
            e = Expression(model.id, "BETWEEN", Expression(10, 'AND', 20))
        ..
        |model.id BETWEEN 10 AND 20|
    """
    __slots__ = ('string', 'left', 'right', 'operator', 'group_op', 'params',
                 'alias', 'use_field_name')

    def __init__(self, left, operator, right=_empty, params=None, alias=None,
                 use_field_name=False):
        """`Expression`
            ==================================================================
            Formats and parameterizes SQL expressions.
            ==================================================================
            @left: any object
            @operator: (#str) operator. This does NOT get parameterized so it
                is completely unsafe to put user submitted data here.
            @right: any object
        """
        self.string = None
        self.left = left
        self.right = right
        self.operator = operator
        self.group_op = None
        self.params = params or {}
        self.alias = alias
        self.use_field_name = use_field_name
        self.compile()

    __repr__ = preprX('operator', 'string', 'params', keyless=True,
                      address=False)

    def group_and(self):
        self.group_op = "AND"
        self.compile()
        return self

    def group_or(self):
        self.group_op = "OR"
        self.compile()
        return self

    def group(self):
        self.group_op = " "
        self.compile()
        return self

    def compile(self, use_field_name=None):
        """ Turns the expression into a string """
        if use_field_name is not None:
            self.use_field_name = use_field_name
        left, right = self._compile_expressions(
            self.left, self.right, use_field_name=self.use_field_name)
        self.string = ("%s %s %s" % (left, self.operator, right)).strip()
        if self.alias is not None:
            self.string = "%s %s" % (self.string, self.alias)
        if self.group_op is not None:
            self.string = ("(%s) %s" % (self.string, self.group_op)).rstrip()
        return self.string


class Clause(BaseExpression):
    """ You must use this wrapper if you want to create clauses within the
        models. Clauses are tied to the :class:Query objects to ensure
        proper formatting.
    """
    __slots__ = ('clause', 'params', 'args', 'alias', 'string',
                 'use_field_name', 'join_with', 'wrap')

    def __init__(self, clause, *args, join_with=" ", wrap=False, params=None,
                 use_field_name=False, alias=None):
        """`Clause`
            Formats SQL clauses. These are only intended for use within the
            :class:cargo.Query and cargo.QueryState objects, as they
            are for building the foundation of the SQL query.

            @clause: (#str) name of the clause - this does NOT get
                parameterized so it is completely unsafe to put user
                submitted data here.
            @*args: any objects to include in the clause
            @join_with: (#str) delimeter to join @*args with
            @wrap: (#bool) True to wrap @args with parantheses when formatting
            @use_field_name: (#bool) True to use :class:Field field names
                instead of full names when parsing expressions
            @alias: (#str) name to alias the clause with
        """
        self.clause = clause.upper().strip()
        self.params = params or {}
        self.args = args
        self.alias = alias
        self.string = None
        self.use_field_name = use_field_name
        self.join_with = join_with
        self.wrap = wrap
        self.compile()

    __repr__ = preprX('string', 'params', keyless=True, address=False)

    def __str__(self):
        return self.string

    def compile(self, join_with=None, wrap=None, use_field_name=None):
        """ Turns the clause into a string """
        if join_with is not None:
            self.join_with = join_with
        if wrap is not None:
            self.wrap = wrap
        if use_field_name is not None:
            self.use_field_name = use_field_name
        clause_params = self._join_expressions(
            self.join_with, *self.args, use_field_name=self.use_field_name)
        if self.wrap:
            clause_params = "(%s)" % clause_params
        self.string = (
            "%s %s %s" % (self.clause, clause_params, self.alias or "")
        ).strip()
        return self.string


class CommaClause(Clause):

    def __init__(self, *args, **kwargs):
        """ A :class:Clause which separates its @args with commas """
        super().__init__(*args, join_with=', ', **kwargs)


class WrappedClause(Clause):

    def __init__(self, *args, **kwargs):
        """ A :class:Clause which wraps itself in parentheses """
        super().__init__(*args, wrap=True, **kwargs)


class ValuesClause(Clause):

    def __init__(self, *args, **kwargs):
        """ A :class:Clause which wraps itself in parentheses and
            separates its @args with commas
        """
        super().__init__(*args, wrap=True, join_with=", ", **kwargs)


class Case(BaseExpression):
    """ ======================================================================
        ``Usage Example`
        ..
            orm.select(Case(fielda.eq(1), 'one',
                            fielda.eq(2), 'two',
                            el='three'))
        ..
        |SELECT CASE WHEN foo.bar = 1 THEN 'one'|
        |            WHEN foo.bar = 2 THEN 'two'|
        |            ELSE 'three'               |
        |       END                             |
    """
    __slots__ = ('conditions', '_el', 'params', 'string', 'use_field_name',
                 'alias')

    def __init__(self, *when_then, el=None, use_field_name=False, alias=None):
        """ `Case Statements`

            @*when_then: a series of |when, then, when, then| expressions
                e.g. |Case(fielda == 1, 'one', fielda == 2, 'two')|
            @el: the default value - creates the |ELSE| statement in the |CASE|
            @use_field_name: (#bool) True to use the short name |field| of the
                field rather than the full |table.field| name
        """
        self.conditions = list(when_then)
        self._el = parameterize(el) if el is not None else el
        self.params = {}
        self.alias = alias
        self.use_field_name = use_field_name
        self.compile()

    __repr__ = preprX('string', 'params', keyless=True, address=False)

    def __str__(self):
        return self.string

    def when(self, *when_then):
        """ Creates the |WHEN ... THEN| clause for the |CASE|

            @*when_then: a series of |when, then, when, then| expressions
                e.g. |Case(fielda == 1, 'one', fielda == 2, 'two')|
        """
        self.conditions.extend(when_then)
        self.compile()

    def el(self, val):
        """ Creates the |ELSE| clause for the |CASE|

            @val: the default value - creates the |ELSE| statement
                in the |CASE|
        """
        self._el = parameterize(val) if val is not None else val
        self.compile()

    def compile(self, use_field_name=None):
        """ Compiles the object to a string """
        if use_field_name is not None:
            self.use_field_name = use_field_name
        expressions = list(filter(None, self._compile_expressions(
            *self.conditions, use_field_name=self.use_field_name)))
        when_thens = " ".join(
            "WHEN {} THEN {}".format(when, then)
            for when, then in zip(expressions[0::2], expressions[1::2])
        )
        el = ""
        if self._el is not None:
            self._inherit_parameters(self._el)
            el = "ELSE " + self._el.string
        string = "CASE {} {}".format(when_thens, el)
        self.string = "{} END {}".format(
            string.strip(), self.alias or "").strip()
        return self.string


class Function(BaseExpression, NumericLogic, StringLogic):
    """ You must use this wrapper if you
        want to make function calls within the models, as plain text is
        parameterized for safety.

        Incorrect::
        |model.where("age(model.timestamp, '1980-03-19')")|

        Corrected::
        |model.where(Function('age', model.timestamp, '1980-03-19'))|
    """
    __slots__ = ('string', 'func', 'args', 'params', 'alias', 'use_field_name')

    def __init__(self, func, *args, use_field_name=False, params=None,
                 alias=None):
        """`Function`
            ==================================================================
            Formats and aliases SQL functions.
            ==================================================================
            @func: (#str) name of func - this does NOT get parameterized
                so it is completely unsafe to put user submitted data here
            @*args: arguments to pass to the function
            @use_field_name: (#bool) True to use the 'field_name' attribute in
                :class:Field objects instead of 'name'
            @alias: (#str) name to alias the function with
        """
        self.string = None
        self.func = func
        self.args = args
        self.alias = alias
        self.params = params or {}
        self.use_field_name = use_field_name
        self.compile()

    __repr__ = preprX('string', 'params', keyless=True, address=False)

    def __str__(self):
        return self.string

    def over(self, *expressions, window_name=None, partition_by=None,
             order_by=None, alias=None):
        """ Creates a |WINDOW| function |OVER| this function.

            @expressions: (:class:BaseExpression) creates an OVER clause
                with these expressions rather than @partition_by and
                @order_by
            @window_name: (#str) name of the WINDOW clause created with
                :meth:ORM.window
            @partition_by: (:class:Field) field to partition by
            @order_by: (:class:Field|:class:BaseExpression) expression clause
                for the ORDER BY clause e.g. |order_by=your_field.desc()|
            ..
            window = model.field.avg().over(partition_by=model.partition_field,
                                            order_by=model.field.desc(),
                                            alias='field_alias')
            model.select(window)
            ..
            |SELECT avg(foo.field) OVER (PARTITION BY foo.partition_field     |
            |                            ORDER BY foo.field DESC) field_alias |

            -> (:class:Expression) window function expression
        """
        wrap = True
        if not expressions:
            expressions = []
            if partition_by is not None:
                expressions.append(Clause('PARTITION BY', partition_by))
            if order_by is not None:
                expressions.append(Clause('ORDER BY', order_by))
        elif len(expressions) == 1 and isinstance(expressions[0], str):
            #: Window name alias
            expressions = [safe(expressions[0])]
            wrap = False
        return Expression(self,
                          Clause('OVER', *expressions, wrap=wrap),
                          alias=alias)

    def compile(self, use_field_name=None):
        """ Turns the function into a string """
        if use_field_name is not None:
            self.use_field_name = use_field_name
        self.string = (
            "%s(%s) %s" % (
                self.func,
                self._join_expressions(", ",
                                       *self.args,
                                       use_field_name=self.use_field_name),
                self.alias or "")
        ).rstrip()
        return self.string


class WindowFunctions(object):
    __slots__ = tuple()

    @staticmethod
    def row_number(*expressions, window_name=None, partition_by=None,
                   order_by=None, alias=None):
        """ Creates a |row_number()| window :class:Function.

            The function returns a bigint as the number of the current row
            within its partition, counting from 1

            -> (:class:Function) object
        """
        return Function('row_number').over(*expressions,
                                           window_name=window_name,
                                           partition_by=partition_by,
                                           order_by=order_by,
                                           alias=alias)

    @staticmethod
    def rank(*expressions, window_name=None, partition_by=None,
             order_by=None, alias=None):
        """ Creates a |rank()| window :class:Function.

            This function returns a bigint as the rank of the current row
            with gaps; same as row_number of its first peer.

            -> (:class:Function) object
        """
        return Function('rank').over(*expressions,
                                     window_name=window_name,
                                     partition_by=partition_by,
                                     order_by=order_by,
                                     alias=alias)

    @staticmethod
    def dense_rank(*expressions, window_name=None, partition_by=None,
                   order_by=None, alias=None):
        """ Creates a |dense_rank()| window :class:Function.

            This function returns a bigint as the rank of the current
            row without gaps; this function counts peer groups.

            -> (:class:Function) object
        """
        return Function('dense_rank').over(*expressions,
                                           window_name=window_name,
                                           partition_by=partition_by,
                                           order_by=order_by,
                                           alias=alias)

    @staticmethod
    def percent_rank(*expressions, window_name=None, partition_by=None,
                     order_by=None, alias=None):
        """ Creates a |percent_rank()| window :class:Function.

            This function returns a double precision as the relative
            rank of the current row: (rank - 1) / (total rows - 1)

            -> (:class:Function) object
        """
        return Function('percent_rank').over(*expressions,
                                             window_name=window_name,
                                             partition_by=partition_by,
                                             order_by=order_by,
                                             alias=alias)

    @staticmethod
    def cume_dist(*expressions, window_name=None, partition_by=None,
                  order_by=None, alias=None):
        """ Creates a |cume_dist()| window :class:Function.

            This function returns a double precision as the relative
            rank of the current row: (number of rows preceding or peer
            with current row) / (total rows)

            -> (:class:Function) object
        """
        return Function('cume_dist').over(*expressions,
                                          window_name=window_name,
                                          partition_by=partition_by,
                                          order_by=order_by,
                                          alias=alias)

    @staticmethod
    def ntile(num_buckets, *expressions, window_name=None,
              partition_by=None, order_by=None, alias=None):
        """ Creates a |ntile()| window :class:Function.

            This function returns a integer as the integer ranging from 1
            to the argument value, dividing the partition as equally
            as possible.

            @num_buckets: (#int) number of partitions to divide into

            -> (:class:Function) object
        """
        return Function('ntile', num_buckets).over(*expressions,
                                                   window_name=window_name,
                                                   partition_by=partition_by,
                                                   order_by=order_by,
                                                   alias=alias)

    @staticmethod
    def lag(field, *expressions, offset=None, default=None,
            window_name=None, partition_by=None, order_by=None, alias=None):
        """ Creates a |lag()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is offset rows before the current row
            within the partition; if there is no such row, instead return
            default (which must be of the same type as value). Both offset
            and default are evaluated with respect to the current row.
            If omitted, offset defaults to 1 and default to null.

            @field: (:class:Field)
            @offset: (#int) number of offset rows
            @default: (:class:Field) the default if no row is found

            -> (:class:Function) object
        """
        return Function('lag', field, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    @staticmethod
    def lead(field, *expressions, offset=None, default=None, window_name=None,
             partition_by=None, order_by=None, alias=None):
        """ Creates a |lead()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is offset rows after the current row
            within the partition; if there is no such row, instead return
            default (which must be of the same type as value). Both offset
            and default are evaluated with respect to the current row. If
            omitted, offset defaults to 1 and default to null.

            @field: (:class:Field)
            @offset: (#int) number of offset rows
            @default: (:class:Field) the default if no row is found

            -> (:class:Function) object
        """
        return Function('lead', field, offset or _empty, default or _empty)\
            .over(*expressions,
                  window_name=window_name,
                  partition_by=partition_by,
                  order_by=order_by,
                  alias=alias)

    @staticmethod
    def first_value(field, *expressions, window_name=None,
                    partition_by=None, order_by=None, alias=None):
        """ Creates a |first_value()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is the first row of the window frame.

            @field: (:class:Field)

            -> (:class:Function) object
        """
        return Function('first_value', field).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    @staticmethod
    def last_value(field, *expressions, window_name=None,
                   partition_by=None, order_by=None, alias=None):
        """ Creates a |last_value()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is the last row of the window frame.

            @field: (:class:Field)

            -> (:class:Function) object
        """
        return Function('last_value', field).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)

    @staticmethod
    def nth_value(field, nth_integer, *expressions, window_name=None,
                  partition_by=None, order_by=None, alias=None):
        """ Creates a |nth_value()| window :class:Function.

            This function returns a same type as value as the value
            evaluated at the row that is the nth row of the window
            frame (counting from 1); null if no such row.

            @field: (:class:Field)
            @nth_integer: (#int) nth row of the window frame

            -> (:class:Function) object
        """
        return Function('nth_value', field, nth_integer).over(
            *expressions,
            window_name=window_name,
            partition_by=partition_by,
            order_by=order_by,
            alias=alias)


class Functions(WindowFunctions):
    """ Wrapper functions for common SQL functions, providing a simpler path
        to respective :class:Function objects.
    """
    __slots__ = tuple()

    @staticmethod
    def coalesce(*vals, alias=None, **kwargs):
        """ Creates a |COALESCE| SQL expression.

            The |COALESCE| function returns the first of its arguments
            that is not null.

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(F.coalesce(1, 2, 3, 4))
            ..
            |COALESCE(1, 2, 3, 4)|
        """
        return Function("coalesce", *vals, alias=alias, **kwargs)

    @staticmethod
    def greatest(*series, alias=None, **kwargs):
        """ Creates a |GREATEST| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(F.greatest(1, 2, 3, 4))
            ..
            |GREATEST(1, 2, 3, 4)|
        """
        return Function("greatest", *series, alias=alias, **kwargs)

    @staticmethod
    def least(*series, alias=None, **kwargs):
        """ Creates a |LEAST| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(F.least(1, 2, 3, 4))
            ..
            |LEAST(1, 2, 3, 4)|
        """
        return Function("least", *series, alias=alias, **kwargs)

    @staticmethod
    def generate_series(start, stop, alias=None, **kwargs):
        """ Creates a |generate_series| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(F.generate_series(1, 5))
            ..
            |generate_series(1, 5)|
        """
        return Function("generate_series", start, stop, alias=alias, **kwargs)

    @staticmethod
    def generate_dates(*series, alias=None, **kwargs):
        """ Creates a |generate_dates| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(F.generate_dates(
                    '2009-01-01', '2009-12-31', 1))
            ..
            |generate_dates('2009-01-01', '2009-12-31', 1) |
        """
        return Function("generate_dates", *series, alias=alias, **kwargs)

    @staticmethod
    def max(val, alias=None, **kwargs):
        """ Creates a |MAX| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(model.field.max('max_posts'))
                # or
                model.select(F.max(model.field, 'max_posts'))
            ..
            |MAX(field) max_posts|
        """
        return Function("max", val, alias=alias, **kwargs)

    @staticmethod
    def min(val, alias=None, **kwargs):
        """ Creates a |MIN| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(model.field.min('min_posts'))
                # or
                model.select(F.min(model.field, 'min_posts'))
            ..
            |MIN(field) min_posts|
        """
        return Function("min", val, alias=alias, **kwargs)

    @staticmethod
    def avg(val, alias=None, **kwargs):
        """ Creates a |AVG| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(model.field.avg('avg_posts'))
                # or
                model.select(F.avg(model.field, 'avg_posts'))
            ..
            |AVG(field) avg_posts|
        """
        return Function("avg", val, alias=alias, **kwargs)

    @staticmethod
    def sum(val, alias=None, **kwargs):
        """ Creates a |SUM| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.select(model.field.sum('sum_posts'))
                # or
                model.select(F.sum(model.field, 'sum_posts'))
            ..
            |SUM(field) sum_posts|
        """
        return Function("sum", val, alias=alias, **kwargs)

    @staticmethod
    def abs(val, alias=None, **kwargs):
        """ Creates a |ABS| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.where(model.field.abs() == 1)
                # or
                model.where(F.abs(model.field)) == 1)
            ..
            |WHERE ABS(field) = 1|
        """
        return Function("abs", val, alias=alias, **kwargs)

    @staticmethod
    def pi():
        """ Creates a |pi()| SQL expression
            |pi()|
        """
        return Function("pi")

    @staticmethod
    def atan(val, alias=None, **kwargs):
        """ Creates a |atan| SQL expression
            |atan(val)|
        """
        return Function("atan", val, alias=alias, **kwargs)

    @staticmethod
    def atan2(x, y, alias=None, **kwargs):
        """ Creates a |atan2| SQL expression
            |atan2(val)|
        """
        return Function("atan2", x, y, alias=alias, **kwargs)

    @staticmethod
    def acos(val, alias=None, **kwargs):
        """ Creates a |acos| SQL expression
            |acos(val)|
        """
        return Function("acos", val, alias=alias, **kwargs)

    @staticmethod
    def asin(val, alias=None, **kwargs):
        """ Creates a |asin| SQL expression
            |asin(val)|
        """
        return Function("asin", val, alias=alias, **kwargs)

    @staticmethod
    def cos(val, alias=None, **kwargs):
        """ Creates a |cos| SQL expression
            |cos(val)|
        """
        return Function("cos", val, alias=alias, **kwargs)

    @staticmethod
    def cot(val, alias=None, **kwargs):
        """ Creates a |cot| SQL expression
            |cot(val)|
        """
        return Function("cot", val, alias=alias, **kwargs)

    @staticmethod
    def degrees(val, alias=None, **kwargs):
        """ Creates a |degrees| SQL expression
            |degrees(val)|
        """
        return Function("degrees", val, alias=alias, **kwargs)

    @staticmethod
    def exp(val, alias=None, **kwargs):
        """ Creates a |exp| SQL expression
            |exp(val)|
        """
        return Function("exp", val, alias=alias, **kwargs)

    @staticmethod
    def floor(val, alias=None, **kwargs):
        """ Creates a |floor| SQL expression
            |floor(val)|
        """
        return Function("floor", val, alias=alias, **kwargs)

    @staticmethod
    def ceil(val, alias=None, **kwargs):
        """ Creates a |ceil| SQL expression
            |ceil(val)|
        """
        return Function("ceil", val, alias=alias, **kwargs)

    @staticmethod
    def ln(val, alias=None, **kwargs):
        """ Creates a |ln| SQL expression
            |ln(val)|
        """
        return Function("ln", val, alias=alias, **kwargs)

    @staticmethod
    def log(val, *base, alias=None, **kwargs):
        """ Creates a |log| SQL expression
            |log(val, 2.0)|
        """
        return Function("log", val, *base, alias=alias, **kwargs)

    @staticmethod
    def mod(val, dmod=0, alias=None, **kwargs):
        """ Creates a |mod| SQL expression
            |mod(val)|
        """
        return Function("mod", val, dmod, alias=alias, **kwargs)

    @staticmethod
    def pow(val, power=2, alias=None, **kwargs):
        """ Creates a |pow| SQL expression
            |pow(val, 2)|
        """
        return Function("power", val, power, alias=alias, **kwargs)

    @staticmethod
    def radians(val, alias=None, **kwargs):
        """ Creates a |radians| SQL expression
            |radians(360)|
        """
        return Function("radians", val, alias=alias, **kwargs)

    @staticmethod
    def round(val, alias=None, **kwargs):
        """ Creates a |round| SQL expression
            |round(val)|
        """
        return Function("round", val, alias=alias, **kwargs)

    @staticmethod
    def sign(val, alias=None, **kwargs):
        """ Creates a |sign| SQL expression
            |sign(val)|
        """
        return Function("sign", val, alias=alias, **kwargs)

    @staticmethod
    def sin(val, alias=None, **kwargs):
        """ Creates a |sin| SQL expression
            |sin(val)|
        """
        return Function("sin", val, alias=alias, **kwargs)

    @staticmethod
    def sqrt(val, alias=None, **kwargs):
        """ Creates a |sqrt| SQL expression
            |sqrt(val)|
        """
        return Function("sqrt", val, alias=alias, **kwargs)

    @staticmethod
    def tan(val, alias=None, **kwargs):
        """ Creates a |tan| SQL expression
            |tan(val)|
        """
        return Function("tan", val, alias=alias, **kwargs)

    @staticmethod
    def nullif(*args, **kwargs):
        """ Creates a |NULLIF| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                condition = F.nullif('some value')
            ..
        """
        return Function(operators.NULLIF, *args, **kwargs)

    @staticmethod
    def count(val, alias=None, **kwargs):
        """ Creates a |COUNT| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.count()
                model.where(condition > 1)
            ..
            |COUNT(field)|
        """
        return Function("count", val, alias=alias, **kwargs)

    @staticmethod
    def distinct(val, alias=None, **kwargs):
        """ Creates a |DISTINCT| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.distinct().count()
                # or
                condition = F.distinct(model.field).count()
                model.where(condition > 1)
            ..
            |COUNT(DISTINCT(field))|
        """
        return Function("distinct", val, alias=alias, **kwargs)

    @staticmethod
    def distinct_on(val, alias=None, **kwargs):
        """ Creates a |DISTINCT ON| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.distinct_on().count()
                # or
                condition = F.distinct_on(model.field).count()
                model.where(condition > 1)
            ..
            |COUNT(DISTINCT ON(field))|
        """
        return Function("DISTINCT ON", val, alias=alias, **kwargs)

    @staticmethod
    def age(*args, alias=None, **kwargs):
        """ Creates a |age| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.age()
                # or
                condition = F.age(model.field)
                model.where(condition > 86400)
            ..
            |age(field)|
        """
        return Function("age", *args, alias=alias, **kwargs)

    @staticmethod
    def timeofday(**kwargs):
        """ Creates a |timeofday| SQL expression
            |timeofday()|
        """
        return Function("timeofday", **kwargs)

    @staticmethod
    def transaction_timestamp(**kwargs):
        """ Creates a |transaction_timestamp| SQL expression
            |transaction_timestamp()|
        """
        return Function("transaction_timestamp", **kwargs)

    @staticmethod
    def now(**kwargs):
        """ Creates a |now| SQL expression
            |now()|
        """
        return Function("now", **kwargs)

    @staticmethod
    def position():
        pass

    @staticmethod
    def any(val, alias=None, **kwargs):
        """ Creates an |ANY| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.field.any()
                # or
                F.any(model.field)
            ..
            |ANY(field)|
        """
        return Function("any", val, alias=alias, **kwargs)

    @staticmethod
    def all(val, alias=None, **kwargs):
        """ Creates an |ALL| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.field.all()
                # or
                F.all(model.field)
            ..
            |ALL(field)|
        """
        return Function("all", val, alias=alias, **kwargs)

    @staticmethod
    def some(val, alias=None, **kwargs):
        """ Creates a |SOME| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.field.some()
                # or
                F.some(model.field)
            ..
            |SOME(field)|
        """
        return Function("some", val, alias=alias, **kwargs)

    @staticmethod
    def using(a, b, **kwargs):
        """ Creates a |USING| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                model.field.using(field_b)
            ..
            |USING(field)|
        """
        return Function(operators.USING, a, b, **kwargs)

    @staticmethod
    def substring(a, b, **kwargs):
        """ Creates a |substring| SQL expression

            -> SQL :class:Function object
            ==================================================================

            ``Usage Example``
            ..
                condition = model.field.substring('o.b')
                model.where(condition)
            ..
            |substring(field FROM 'o.b')|
        """
        return Function('substring',
                        a,
                        Expression(_empty, operators.FROM, b),
                        **kwargs)

    @staticmethod
    def regexp_replace(string, *args, **kwargs):
        """ Creates a |regexp_replace| expression
            |regexp_replace('Thomas', '.[mN]a.', 'M')|
        """
        return Function('regexp_replace', string, *args, **kwargs)

    @staticmethod
    def regexp_matches(string, pattern, *flags, **kwargs):
        """ Creates a |regexp_matches| expression
            |regexp_matches('foobarbequebaz', '(bar)(beque)')|
        """
        return Function('regexp_matches', string, pattern, *flags, **kwargs)

    @staticmethod
    def concat(string, *others, **kwargs):
        """ Creates a |concat| expression
            |concat('abcde', 2, 22)|
        """
        return Function('concat', string, *others, **kwargs)

    @staticmethod
    def concat_ws(string, *others, separator=',', **kwargs):
        """ Creates a |concat_ws| (concat with separator) expression
            |concat_ws('abcde', 2, 22, separator=" ")|
        """
        return Function('concat_ws', separator, string, *others, **kwargs)

    @staticmethod
    def clock_timestamp(alias=None):
        return Function('clock_timestamp', alias=alias)

    @staticmethod
    def date_part(text, timestamp, **kwargs):
        """ date_part('hour', timestamp '2001-02-16 20:38:40') """
        return Function('date_part', text, timestamp, **kwargs)

    @staticmethod
    def date_trunc(text, timestamp, **kwargs):
        """ date_trunc('hour', timestamp '2001-02-16 20:38:40') """
        return Function('date_trunc', text, timestamp, **kwargs)

    @staticmethod
    def extract(text, timestamp, **kwargs):
        """ extract(hour from timestamp '2001-02-16 20:38:40') """
        return Function(
            'extract',
            Expression(
                text, Expression(_empty, operators.FROM, timestamp), **kwargs))

    @staticmethod
    def isfinite(timestamp, **kwargs):
        """ isfinite(timestamp '2001-02-16 21:28:30') """
        return Function('isfinite', timestamp, **kwargs)

    @staticmethod
    def exists(val, **kwargs):
        return Function('EXISTS', val, **kwargs)

    @staticmethod
    def not_exists(val, **kwargs):
        return Function('NOT EXISTS', val, **kwargs)

    @staticmethod
    def cast(field, as_, **kwargs):
        """ cast(mytable.myfield AS integer) """
        return Function('cast', Expression(field, 'AS', safe(as_)), **kwargs)

    #: Byte support functions

    @staticmethod
    def btrim(string, bytes, **kwargs):
        """ Remove the longest string consisting only of bytes in @bytes from
            the start and end of @string.
            -> (:class:Function)
        """
        return Function('btrim', string, bytes, **kwargs)

    @staticmethod
    def encode(data, format, **kwargs):
        """ Encode binary data into a textual representation. Supported
            formats are: base64, hex, escape. escape converts zero bytes and
            high-bit-set bytes to octal sequences (\nnn) and doubles
            backslashes.
            -> (:class:Function)
        """
        return Function('encode', data, format, **kwargs)

    @staticmethod
    def decode(data, format, **kwargs):
        """ Decode binary data from textual representation in string. Options
            for format are same as in encode.
            -> (:class:Function)
        """
        return Function('decode', data, format, **kwargs)

    @staticmethod
    def get_bit(string, offset, **kwargs):
        """ Extract bit from @string
            -> (:class:Function)
        """
        return Function('get_bit', string, offset, **kwargs)

    @staticmethod
    def get_byte(string, offset, **kwargs):
        """ Extract byte from @string
            -> (:class:Function)
        """
        return Function('get_byte', string, offset, **kwargs)

    @staticmethod
    def set_bit(string, offset, new_value, **kwargs):
        """ Set bit in @string
            -> (:class:Function)
        """
        return Function('set_bit', string, offset, new_value, **kwargs)

    @staticmethod
    def set_byte(string, offset, new_value, **kwargs):
        """ Set byte in @string
            -> (:class:Function)
        """
        return Function('set_byte', string, offset, new_value, **kwargs)

    @staticmethod
    def length(string, **kwargs):
        """ Length of binary @string
            -> (:class:Function)
        """
        return Function('length', string, **kwargs)

    @staticmethod
    def md5(string, **kwargs):
        """ Calculates the MD5 hash of @string, returning the result in
            hexadecimal.
            -> (:class:Function)
        """
        return Function('md5', string, **kwargs)

    #: JSON support functions

    @staticmethod
    def array_to_json(array, pretty=False, **kwargs):
        """ Returns the @array as a JSON array. A PostgreSQL multidimensional
            array becomes a JSON array of arrays. Line feeds will be added
            between dimension-1 elements if @pretty is |True|.
            -> (:class:Function)
        """
        exps = [array]
        if pretty:
            exps.append(pretty)
        return Function('array_to_json', *exps, **kwargs)

    @staticmethod
    def row_to_json(record, pretty=False, **kwargs):
        """ Returns the @record as a JSON object. Line feeds will be added
            between level-1 elements if @pretty is |True|.
            -> (:class:Function)
        """
        exps = [record]
        if pretty:
            exps.append(pretty)
        return Function('row_to_json', *exps, **kwargs)

    @staticmethod
    def to_json(element, **kwargs):
        """ Returns the value as json or jsonb. Arrays and composites are
            converted (recursively) to arrays and objects; otherwise, if there
            is a cast from the type to json, the cast function will be used
            to perform the conversion; otherwise, a scalar value is produced.
            For any scalar type other than a number, a Boolean, or a null
            value, the text representation will be used, in such a fashion
            that it is a valid json or jsonb value.
            -> (:class:Function)
        """
        return Function('to_json', element, **kwargs)

    @staticmethod
    def json_array_length(obj, **kwargs):
        """ Returns the number of elements in the outermost JSON array.
            -> (:class:Function)
        """
        return Function('json_array_length', obj, **kwargs)

    @staticmethod
    def json_each(obj, **kwargs):
        """ Expands the outermost JSON object into a set of key/value pairs.
            -> (:class:Function)
        """
        return Function('json_each', obj, **kwargs)

    @staticmethod
    def json_each_text(obj, **kwargs):
        """ Expands the outermost JSON object into a set of key/value pairs.
            The returned values will be of type text.
            -> (:class:Function)
        """
        return Function('json_each_text', obj, **kwargs)

    @staticmethod
    def json_object_keys(obj, **kwargs):
        """ Returns set of keys in the outermost JSON object.
            -> (:class:Function)
        """
        return Function('json_object_keys', obj, **kwargs)

    @staticmethod
    def json_populate_record(record, json, **kwargs):
        """ Expands the object in @json to a row whose columns match the
            @record type defined by base.
            -> (:class:Function)
        """
        return Function('json_populate_record', record, json, **kwargs)

    @staticmethod
    def json_populate_recordset(record, json, **kwargs):
        """ Expands the outermost array of objects in @json to a set of rows
            whose columns match the @record type defined by base.
            -> (:class:Function)
        """
        return Function('json_populate_recordset', record, json,  **kwargs)

    @staticmethod
    def json_array_elements(**kwargs):
        """ Returns the number of elements in the outermost JSON array.
            -> (:class:Function)
        """
        return Function('json_array_elements', obj, **kwargs)

    #: JSONb support functions

    @staticmethod
    def _json_to_jsonb(func):
        func.func = func.func.replace('json', 'jsonb')
        func.compile()
        return func

    @staticmethod
    def array_to_jsonb(*args, **kwargs):
        """ :see::meth:array_to_json """
        return F._json_to_jsonb(
            F.array_to_json(*args, **kwargs))

    @staticmethod
    def row_to_jsonb(*args, **kwargs):
        """ :see::meth:row_to_json """
        return F._json_to_jsonb(F.row_to_json(*args, **kwargs))

    @staticmethod
    def to_jsonb(*args, **kwargs):
        """ :see::meth:to_json """
        return F._json_to_jsonb(F.to_json(*args, **kwargs))

    @staticmethod
    def jsonb_array_length(*args, **kwargs):
        """ :see::meth:json_array_length """
        return F._json_to_jsonb(
            F.json_array_length(*args, **kwargs))

    @staticmethod
    def jsonb_each(*args, **kwargs):
        """ :see::meth:json_each """
        return F._json_to_jsonb(F.json_each(*args, **kwargs))

    @staticmethod
    def jsonb_each_text(*args, **kwargs):
        """ :see::meth:json_each_text """
        return F._json_to_jsonb(
            F.json_each_text(*args, **kwargs))

    @staticmethod
    def jsonb_object_keys(*args, **kwargs):
        """ :see::meth:json_object_keys """
        return F._json_to_jsonb(
            F.json_object_keys(*args, **kwargs))

    @staticmethod
    def jsonb_populate_record(*args, **kwargs):
        """ :see::meth:populate_record """
        return F._json_to_jsonb(
            F.json_populate_record(*args, **kwargs))

    @staticmethod
    def jsobn_populate_recordset(*args, **kwargs):
        """ :see::meth:json_populate_recordset """
        return F._json_to_jsonb(
            F.json_populate_recordset(*args, **kwargs))

    @staticmethod
    def jsonb_array_elements(*args, **kwargs):
        """ :see::meth:json_array_elements """
        return F._json_to_jsonb(
            F.json_array_elements(*args, **kwargs))

    #: Array support functions

    def array_length(array, dimension=1, **kwargs):
        """ Returns the length of the requested @array @dimension
            -> (:class:Function)
        """
        return Function('array_length', array, dimension, **kwargs)

    def array_append(array, element, **kwargs):
        """ Insert an @element at the end of an @array
            -> (:class:Function)
        """
        return Function('array_append', array, element, **kwargs)

    def array_prepend(array, element, **kwargs):
        """ Insert an @element at the beginning of an @array
            -> (:class:Function)
        """
        return Function('array_prepend', array, element, **kwargs)

    def array_remove(array, element, **kwargs):
        """ Remove all @element(s) equal to the given value from the @array
            (array must be one-dimensional)
            -> (:class:Function)
        """
        return Function('array_remove', array, element, **kwargs)

    def array_replace(array, element, new_element, **kwargs):
        """ Replace each @array @element equal to the given value with
            a @new_element
            -> (:class:Function)
        """
        return Function('array_replace', array, element, new_element, **kwargs)

    def array_position(array, element, **kwargs):
        """ Returns the subscript of the first occurrence of the @element
            in the @array, starting at the element indicated by the third
            argument or at the first element (array must be one-dimensional)
            -> (:class:Function)
        """
        return Function('array_position', array, element, **kwargs)

    def array_positions(array, element, **kwargs):
        """ Returns an array of subscripts of all occurrences of @element
            in the @array (array must be one-dimensional)
            -> (:class:Function)
        """
        return Function('array_positions', array, element, **kwargs)

    def array_ndims(array, **kwargs):
        """ Returns the number of dimensions of the @array
            -> (:class:Function)
        """
        return Function('array_ndims', array, **kwargs)

    def array_dims(array, **kwargs):
        """ Returns a text representation of @array's dimensions
            -> (:class:Function)
        """
        return Function('array_dims', array, **kwargs)

    def array_cat(array, other, **kwargs):
        """ Concatenate @other to @array
            -> (:class:Function)
        """
        return Function('array_cat', array, other, **kwargs)

    def unnest(array, **kwargs):
        """ Expand an @array to a set of rows
            -> (:class:Function)
        """
        return Function('unnest', array, **kwargs)

    @staticmethod
    def new(*args, **kwargs):
        """ Use a custom function or one that is not listed

            ..
                f = F.new('now')
            ..
            |now()|

            ..
                f = F.new('generate_series', 1, 2, 3, 4)
            ..
            |generate_series(1, 2, 3, 4)|
        """
        return Function(*args, **kwargs)


F = Functions


class aliased(NumericLogic, StringLogic):
    """ For field aliases. This object inherits all of the properties of
        the field passed to it.

        ``Usage Example``
        ..
            field.table = 'foo'
            print(field)
            # foo.field_name

            field.set_alias('foo_alias')
            print(aliased(field))
            # foo_alias.field_name

            print(aliased(field).alias('foo_alias_field_name'))
            # foo_alias.field_name AS foo_alias_field_name
        ..
    """
    __slots__ = ('string', 'field')

    def __init__(self, field, use_field_name=False):
        self.field = field
        if field._alias is not None:
            self.string = field._alias
        else:
            self.string = field.field_name if use_field_name else field.name

    __repr__ = preprX('field', 'string', keyless=True, address=False)

    def __str__(self):
        return str(self.string)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.field.__getattr__(name)

    def alias(self, alias):
        return safe('%s AS "%s"' %(str(self), alias))

    def compile(self):
        return self.string


class parameterize(BaseExpression, NumericLogic, StringLogic):
    """ ``Usage Example``
        ..
            ORM().subquery().where(parameterize(1)).select()
        ..
        |%(8ae08c)s| and |{'8ae08c': 1}|
    """
    __slots__ = ('string', 'params')

    def __init__(self, value, alias=None):
        """`parameterize`
            ==================================================================
            Parameterizes plain text
            @value: the values to parameterize
            @alias: (#str) an alias to apply to the value
        """
        try:
            self.params = value.params
        except AttributeError:
            self.params = {}
        self.string = "{} {}".format(
            self._parameterize(value),
            alias if alias else "").rstrip()

    __repr__ = preprX('string', 'params', keyless=True, address=False)

    def __str__(self):
        return self.string

    def compile(self):
        return self.string


class safe(NumericLogic, StringLogic):
    """ !! The value of this object will not be parameterized when
           queries are made. !!

        This object can be manipulated with expression :class:BaseLogic
    """
    __slots__ = ('string', 'params', 'alias')

    def __init__(self, value, alias=None):
        self.alias = alias or ""
        self.params = {}
        self.string = "{} {}".format(value, self.alias).rstrip()

    __repr__ = preprX('string', keyless=True, address=False)

    def __str__(self):
        return self.string

    def compile(self):
        return self.string
