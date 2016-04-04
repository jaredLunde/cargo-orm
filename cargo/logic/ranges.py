"""
  `Range Logic and Operations`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.expressions import *


__all__ = ('RangeLogic',)


class RangeLogic(NumericLogic):
    __slots__ = tuple()
    '''
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

    
