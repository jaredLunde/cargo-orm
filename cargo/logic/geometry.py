"""
  `Geometry Logic and Operations`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.expressions import *


__all__ = ('GeometryLogic',)


class GeometryLogic(object):
    __slots__ = tuple()
    '''
    Operator	Description	Example
    +	Translation	box '((0,0),(1,1))' + point '(2.0,0)'
    -	Translation	box '((0,0),(1,1))' - point '(2.0,0)'
    *	Scaling/rotation	box '((0,0),(1,1))' * point '(2.0,0)'
    /	Scaling/rotation	box '((0,0),(2,2))' / point '(2.0,0)'
    #	Point or box of intersection	'((1,-1),(-1,1))' # '((1,1),(-1,-1))'
    #	Number of points in path or polygon	# '((1,0),(0,1),(-1,0))'
    @-@	Length or circumference	@-@ path '((0,0),(1,0))'
    @@	Center	@@ circle '((0,0),10)'
    ##	Closest point to first operand on second operand
        point '(0,0)' ## lseg '((2,0),(0,2))'
    <->	Distance between	circle '((0,0),1)' <-> circle '((5,0),1)'
    &&	Overlaps? (One point in common makes this true.)
        box '((0,0),(1,1))' && box '((0,0),(2,2))'
    <<	Is strictly left of?	circle '((0,0),1)' << circle '((5,0),1)'
    >>	Is strictly right of?	circle '((5,0),1)' >> circle '((0,0),1)'
    &<	Does not extend to the right of?
        box '((0,0),(1,1))' &< box '((0,0),(2,2))'
    &>	Does not extend to the left of?	box '((0,0),(3,3))' &> box '((0,0),(2,2))'
    <<|	Is strictly below?	box '((0,0),(3,3))' <<| box '((3,4),(5,5))'
    |>>	Is strictly above?	box '((3,4),(5,5))' |>> box '((0,0),(3,3))'
    &<|	Does not extend above?	box '((0,0),(1,1))' &<| box '((0,0),(2,2))'
    |&>	Does not extend below?	box '((0,0),(3,3))' |&> box '((0,0),(2,2))'
    <^	Is below (allows touching)?	circle '((0,0),1)' <^ circle '((0,5),1)'
    >^	Is above (allows touching)?	circle '((0,5),1)' >^ circle '((0,0),1)'
    ?#	Intersects?	lseg '((-1,0),(1,0))' ?# box '((-2,-2),(2,2))'
    ?-	Is horizontal?	?- lseg '((-1,0),(1,0))'
    ?-	Are horizontally aligned?	point '(1,0)' ?- point '(0,0)'
    ?|	Is vertical?	?| lseg '((-1,0),(1,0))'
    ?|	Are vertically aligned?	point '(0,1)' ?| point '(0,0)'
    ?-|	Is perpendicular?	lseg '((0,0),(0,1))' ?-| lseg '((0,0),(1,0))'
    ?||	Are parallel?	lseg '((-1,0),(1,0))' ?|| lseg '((-1,2),(1,2))'
    @>	Contains?	circle '((0,0),2)' @> point '(1,1)'
    <@	Contained in or on?	point '(1,1)' <@ circle '((0,0),2)'
    ~=	Same as?	polygon '((0,0),(1,1))' ~= polygon '((1,1),(0,0))'

    area(object)	double precision	area	area(box '((0,0),(1,1))')
    center(object)	point	center	center(box '((0,0),(1,2))')
    diameter(circle)	double precision	diameter of circle	diameter(circle '((0,0),2.0)')
    height(box)	double precision	vertical size of box	height(box '((0,0),(1,1))')
    isclosed(path)	boolean	a closed path?	isclosed(path '((0,0),(1,1),(2,0))')
    isopen(path)	boolean	an open path?	isopen(path '[(0,0),(1,1),(2,0)]')
    length(object)	double precision	length	length(path '((-1,0),(1,0))')
    npoints(path)	int	number of points	npoints(path '[(0,0),(1,1),(2,0)]')
    npoints(polygon)	int	number of points	npoints(polygon '((1,1),(0,0))')
    pclose(path)	path	convert path to closed	pclose(path '[(0,0),(1,1),(2,0)]')
    popen(path)	path	convert path to open	popen(path '((0,0),(1,1),(2,0))')
    radius(circle)	double precision	radius of circle	radius(circle '((0,0),2.0)')
    width(box)	double precision	horizontal size of box	width(box '((0,0),(1,1))')

    box(circle)	box	circle to box	box(circle '((0,0),2.0)')
    bound_box(box, box)	box	boxes to bounding box	bound_box(box '((0,0),(1,1))', box '((3,3),(4,4))')
    circle(box)	circle	box to circle	circle(box '((0,0),(1,1))')
    line(point, point)	line	points to line	line(point '(-1,0)', point '(1,0)')
    lseg(point, point)	lseg	points to line segment	lseg(point '(-1,0)', point '(1,0)')
    path(polygon)	path	polygon to path	path(polygon '((0,0),(1,1),(2,0))')
    point(polygon)	point	center of polygon	point(polygon '((0,0),(1,1),(2,0))')]
    polygon(path)	polygon	path to polygon
    '''
    def __iter__(self):
        return self.value.__iter__()
