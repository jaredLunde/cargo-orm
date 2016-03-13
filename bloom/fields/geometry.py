"""

  `Bloom SQL Geometric Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple

from collections import UserList

from psycopg2.extensions import adapt, register_adapter, AsIs, new_type,\
                                register_type, QuotedString
from vital.tools.lists import flatten

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields import Field


__all__ = (
    'GeometryLogic',
    'Box',
    'Circle',
    'Line',
    'LSeg',
    'Path',
    'Point',
    'Polygon')


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


class Point(Field, GeometryLogic):
    OID = POINT
    __slots__ = Field.__slots__

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                value = PointRecord(*value)
            self.value = value
        return self.value

    @property
    def x(self):
        try:
            return self.value.x
        except AttributeError:
            return Field.empty

    @property
    def y(self):
        try:
            return self.value.y
        except AttributeError:
            return Field.empty

    @staticmethod
    def to_db(point):
        p = b"(%s, %s)" % (adapt(point.x).getquoted(),
                           adapt(point.y).getquoted())
        return QuotedString(p.decode())

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return PointRecord(*eval(val))


PointRecord = namedtuple('PointRecord', ('x', 'y'))
register_adapter(PointRecord, Point.to_db)
POINTTYPE = reg_type('POINTTYPE', POINT, Point.to_python)
POINTARRAYTYPE = reg_array_type('POINTARRAYTYPE', POINTARRAY, POINTTYPE)


class Box(Field, GeometryLogic):
    OID = BOX
    __slots__ = Field.__slots__

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                value = BoxRecord(PointRecord(*value[0]),
                                  PointRecord(*value[1]))
            self.value = value
        return self.value

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    @property
    def points(self):
        return tuple(self.value)

    @staticmethod
    def to_db(box):
        box = b"((%s, %s), (%s, %s))" % (
            adapt(box.a.x).getquoted(), adapt(box.a.y).getquoted(),
            adapt(box.b.x).getquoted(), adapt(box.b.y).getquoted())
        return QuotedString(box.decode())

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return BoxRecord(*(PointRecord(*v) for v in eval(val)))


BoxRecord = namedtuple('BoxRecord', ('a', 'b'))
register_adapter(BoxRecord, Box.to_db)
BOXTYPE = reg_type('BOXTYPE', BOX, Box.to_python)
BOXARRAYTYPE = reg_array_type('BOXARRAYTYPE', BOXARRAY, BOXTYPE)


class Circle(Field, GeometryLogic):
    OID = CIRCLE
    __slots__ = Field.__slots__

    def __call__(self, value=Field.empty):
        """ @value: (#tuple) center, radius e.g., |((0, 5), 1)| """
        if value is not Field.empty:
            if value is not None:
                value = CircleRecord(PointRecord(*value[0]), value[1])
            self.value = value
        return self.value

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    @staticmethod
    def to_db(circle):
        circ = b"((%s, %s), %s)" % (
            adapt(circle.center.x).getquoted(),
            adapt(circle.center.y).getquoted(),
            adapt(circle.radius).getquoted())
        return QuotedString(circ.decode())

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        circ = eval(val.strip('<>'))
        return CircleRecord(PointRecord(*circ[0]), circ[1])


CircleRecord = namedtuple('CircleRecord', ('center', 'radius'))
register_adapter(CircleRecord, Circle.to_db)
CIRCLETYPE = reg_type('CIRCLETYPE', CIRCLE, Circle.to_python)
CIRCLEARRAYTYPE = reg_array_type('CIRCLEARRAYTYPE', CIRCLEARRAY, CIRCLETYPE)


class Line(Field, GeometryLogic):
    OID = LINE
    __slots__ = Field.__slots__

    def __call__(self, value=Field.empty):
        """ Lines are represented by the linear equation Ax + By + C = 0,
            where A and B are not both zero.

            @value: (#tuple) |(A, B, C)|
        """
        if value is not Field.empty:
            if value is not None:
                value = LineRecord(*value)
            self.value = value
        return self.value

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattribute__(name)

    @staticmethod
    def to_db(line):
        return AsIs("'{%s, %s, %s}'::line" % (adapt(line.a),
                                              adapt(line.b),
                                              adapt(line.c)))

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return LineRecord(*eval(val))


LineRecord = namedtuple('LineRecord', ('a', 'b', 'c'))
register_adapter(LineRecord, Line.to_db)
LINETYPE = reg_type('LINETYPE', LINE, Line.to_python)
LINEARRAYTYPE = reg_array_type('LINEARRAYTYPE', LINEARRAY, LINETYPE)


class LSeg(Box):
    OID = LSEG
    __slots__ = Field.__slots__

    def __call__(self, value=Field.empty):
        """ Lines are represented by the linear equation Ax + By + C = 0,
            where A and B are not both zero.

            @value: (#tuple) |(A, B, C)|
        """
        if value is not Field.empty:
            if value is not None:
                value = LSegRecord(PointRecord(value[0][0], value[0][1]),
                                   PointRecord(value[1][0], value[1][1]))
            self.value = value
        return self.value

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return LSegRecord(*eval(val))


LSegRecord = namedtuple('LSegRecord', ('a', 'b'))
register_adapter(LSegRecord, LSeg.to_db)
LSEGTYPE = reg_type('LSEGTYPE', LSEG, LSeg.to_python)
LSEGARRAYTYPE = reg_array_type('LSEGARRAYTYPE', LSEGARRAY, LSEGTYPE)


class Path(Field, GeometryLogic):
    OID = PATH
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'table',
                 '_closed')

    def __init__(self, closed=None, value=Field.empty, *args, **kwargs):
        """ Lines are represented by the linear equation Ax + By + C = 0,
            where A and B are not both zero.

            @value: (#tuple or #list) |((1, 2), (3, 4))| or |[(1, 2), (3, 4)]|
                where a #tuple constitutes a |CLOSED| path unless otherwise
                specified in @closed and #list constitutes an |OPEN| path.
            @closed: (#bool) |True| if the beginning and end of the path
                are connected
        """
        self._closed = closed
        super().__init__(value=value, **kwargs)

    def __call__(self, value=Field.empty):
        """ Lines are represented by the linear equation Ax + By + C = 0,
            where A and B are not both zero.

            @value: (#tuple) |(A, B, C)|
            @closed: (#bool) |True| if the beginning and end of the path
                are connected
        """
        if value is not Field.empty:
            if value is not None:
                closed = self._closed
                try:
                    value = PathRecord(*value[:-1], closed=value.closed)
                except (AttributeError, TypeError):
                    if closed is None:
                        closed = not isinstance(value, list)
                    value = PathRecord(*value, closed=closed)
            self.value = value
        return self.value

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattr__(name)

    def __getitem__(self, name):
        return self.value[name]

    def __setitem__(self, name, value):
        self.value[name] = value

    def __delitem__(self, name):
        del self.value[name]

    def __iter__(self):
        return self.value.__iter__()

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, closed=self._closed, **kwargs)
        return cls

    __copy__ = copy

    def close(self):
        self._closed = True

    @staticmethod
    def to_db(line):
        points = (b"(%s, %s)" % (adapt(point.x).getquoted(),
                                 adapt(point.y).getquoted())
                  for point in line[:-1])
        ctype = "(%s)" if line.closed else "[%s]"
        return QuotedString(ctype % b", ".join(points).decode())

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        closed = not val.startswith('[')
        return PathRecord(*eval(val), closed=closed)


class PathRecord(UserList):
    __slots__ = ('data',)

    def __init__(self, *paths, closed=False):
        p = list('_' for _ in paths)
        p.append('closed')
        nt = namedtuple('PathRecord', p, rename=True)
        f = list((PointRecord(x, y) for x, y in paths))
        f.append(closed)
        self.data = nt(*f)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.data.__getattribute__(name)


register_adapter(PathRecord, Path.to_db)
PATHTYPE = reg_type('PATHTYPE', PATH, Path.to_python)
PATHARRAYTYPE = reg_array_type('PATHARRAYTYPE', PATHARRAY, PATHTYPE)


class Polygon(Field, GeometryLogic):
    OID = POLYGON
    __slots__ = Field.__slots__

    def __call__(self, value=Field.empty):
        """ Lines are represented by the linear equation Ax + By + C = 0,
            where A and B are not both zero.

            @value: (#tuple) |((x1, y1), (x2, y2), ...)|
        """
        if value is not Field.empty:
            if value is not None:
                value = PolygonRecord(value)
            self.value = value
        return self.value

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.value.__getattr__(name)

    @staticmethod
    def to_db(poly):
        points = (b"(%s, %s)" % (adapt(point.x).getquoted(),
                                 adapt(point.y).getquoted())
                  for point in poly)
        return QuotedString("(%s)" % b", ".join(points).decode())

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return PolygonRecord(eval(val))


class PolygonRecord(UserList):
    __slots__ = ('data',)

    def __init__(self, poly):
        nt = namedtuple('PolygonRecord', ('_' for _ in poly), rename=True)
        self.data = nt(*(PointRecord(x, y) for x, y in poly))

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.data.__getattribute__(name)


register_adapter(PolygonRecord, Polygon.to_db)
POLYGONTYPE = reg_type('POLYGONTYPE', POLYGON, Polygon.to_python)
POLYGONARRAYTYPE = reg_array_type('POLYGONARRAYTYPE',
                                  POLYGONARRAY,
                                  POLYGONTYPE)
