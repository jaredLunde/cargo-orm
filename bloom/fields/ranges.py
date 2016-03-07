"""

  `Bloom SQL Range Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from psycopg2.extras import Range

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = (
    'Range',
    'RangeLogic',
    'IntRange',
    'BigIntRange',
    'NumericRange',
    'TimestampRange',
    'TimestampTZRange',
    'DateRange'
)


class RangeLogic(BaseLogic):
    '''
    =	equal	int4range(1,5) = '[1,4]'::int4range	t
    <>	not equal	numrange(1.1,2.2) <> numrange(1.1,2.3)	t
    <	less than	int4range(1,10) < int4range(2,3)	t
    >	greater than	int4range(1,10) > int4range(1,5)	t
    <=	less than or equal	numrange(1.1,2.2) <= numrange(1.1,2.2)	t
    >=	greater than or equal	numrange(1.1,2.2) >= numrange(1.1,2.0)	t
    @>	contains range	int4range(2,4) @> int4range(2,3)	t
    @>	contains element	'[2011-01-01,2011-03-01)'::tsrange @>
        '2011-01-10'::timestamp	t
    <@	range is contained by	int4range(2,4) <@ int4range(1,7)	t
    <@	element is contained by	42 <@ int4range(1,7)	f
    &&	overlap (have points in common)	int8range(3,7) && int8range(4,12)	t
    <<	strictly left of	int8range(1,10) << int8range(100,110)	t
    >>	strictly right of	int8range(50,60) >> int8range(20,30)	t
    &<	does not extend to the right of	int8range(1,20) &< int8range(18,20)	t
    &>	does not extend to the left of	int8range(7,20) &> int8range(5,10)	t
    -|-	is adjacent to	numrange(1.1,2.2) -|- numrange(2.2,3.3)	t
    +	union	numrange(5,15) + numrange(10,20)	[5,20)
    *	intersection	int8range(5,15) * int8range(10,20)	[10,15)
    -	difference	int8range(5,15) - int8range(10,20)	[5,10)

    lower(anyrange)	range's element type	lower bound of range
        lower(numrange(1.1,2.2))	1.1
    upper(anyrange)	range's element type	upper bound of range
        upper(numrange(1.1,2.2))	2.2
    isempty(anyrange)	boolean	is the range empty?
        isempty(numrange(1.1,2.2))	false
    lower_inc(anyrange)	boolean	is the lower bound inclusive?
        lower_inc(numrange(1.1,2.2))	true
    upper_inc(anyrange)	boolean	is the upper bound inclusive?
        upper_inc(numrange(1.1,2.2))	false
    lower_inf(anyrange)	boolean	is the lower bound infinite?
        lower_inf('(,)'::daterange)	true
    upper_inf(anyrange)	boolean	is the upper bound infinite?
        upper_inf('(,)'::daterange)	true
    '''


class IntRange(Field, RangeLogic):
    OID = INTRANGE
    _slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        '_validator', '_alias', 'default', 'table')

    def __init__(self, value=Field.empty, *args, **kwargs):
        """ `Integer Range`
            @value: (#tuple (lower_bound, upper_bound) or
                :class:psycopg2.extras.Range)
            :see::meth:Field.__init__

            See also: :class:Range and
                http://initd.org/psycopg/docs/extras.html#range-data-types
        """
        super().__init__(value, *args, **kwargs)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            try:
                self._set_value(Range(*value))
            except TypeError:
                if not isinstance(value, Range):
                    raise TypeError(('`%s` values must be of type tuple or ' +
                                     'psycopg2.extras.Range')
                                    % self.__class__.__name__)
                self._set_value(value)
        return self.value

    def set_upper(self, upper):
        try:
            self.value._upper = upper
        except AttributeError:
            self.__call__(Range(upper=upper))
        return

    def set_lower(self, lower):
        try:
            self.value._lower = lower
        except AttributeError:
            self.__call__(Range(lower=lower))
        return


class BigIntRange(IntRange):
    OID = BIGINTRANGE


class NumericRange(IntRange):
    OID = NUMRANGE


class TimestampRange(IntRange):
    OID = TSRANGE


class TimestampTZRange(IntRange):
    OID = TSTZRANGE


class DateRange(IntRange):
    OID = DATERANGE
