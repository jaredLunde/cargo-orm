"""

  `Cargo SQL Geometric Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple

from collections import UserList

from psycopg2.extensions import adapt, register_adapter, AsIs, new_type,\
                                QuotedString
from vital.tools.lists import flatten

from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields import Field
from cargo.logic import GeometryLogic


__all__ = (
    'Box',
    'Circle',
    'Line',
    'LSeg',
    'Path',
    'Point',
    'Polygon')


class _PointAdapter(object):

    def __init__(self, value):
        self.value = value

    def prepare(self, conn):
        self.conn = conn

    def getquoted(self):
        ax = adapt(self.value.x)
        ay = adapt(self.value.y)
        p = b"(%s, %s)" % (ax.getquoted(), ay.getquoted())
        return b"'%s'::point" % p


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

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return tuple(self.value)
        return None
    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return PointRecord(*eval(val))

    @staticmethod
    def register_adapter():
        register_adapter(PointRecord, _PointAdapter)
        POINTTYPE = reg_type('POINTTYPE', POINT, Point.to_python)
        reg_array_type('POINTARRAYTYPE', POINTARRAY, POINTTYPE)


PointRecord = namedtuple('PointRecord', ('x', 'y'))


class _BoxAdapter(_PointAdapter):

    def getquoted(self):
        ax = adapt(self.value.a.x)
        ay = adapt(self.value.a.y)
        bx = adapt(self.value.b.x)
        by = adapt(self.value.b.y)
        box = b"((%s, %s), (%s, %s))" % (ax.getquoted(), ay.getquoted(),
                                         bx.getquoted(), by.getquoted())
        return b"'%s'::box" % box


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

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return tuple(tuple(point) for point in self.points)
        return None

    @property
    def points(self):
        return tuple(self.value)

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return BoxRecord(*(PointRecord(*v) for v in eval(val)))

    @staticmethod
    def array_to_python(val, cur):
        if val is None:
            return val
        val = val.strip('{}').split(';')
        return [Box.to_python(v, cur) for v in val]

    @staticmethod
    def register_adapter():
        register_adapter(BoxRecord, _BoxAdapter)
        BOXTYPE = reg_type('BOXTYPE', BOX, Box.to_python)
        reg_type('BOXARRAYTYPE', BOXARRAY, Box.array_to_python)


BoxRecord = namedtuple('BoxRecord', ('a', 'b'))


class _CircleAdapter(_PointAdapter):

    def getquoted(self):
        cx = adapt(self.value.center.x)
        cy = adapt(self.value.center.y)
        r = adapt(self.value.radius)
        circ = b"((%s, %s), %s)" % (cx.getquoted(), cy.getquoted(),
                                    r.getquoted())
        return b"'%s'::circle" % circ


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

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return (tuple(self.value[0]), self.value[1])
        return None

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        circ = eval(val.strip('<>'))
        return CircleRecord(PointRecord(*circ[0]), circ[1])

    @staticmethod
    def register_adapter():
        register_adapter(CircleRecord, _CircleAdapter)
        CIRCLETYPE = reg_type('CIRCLETYPE', CIRCLE, Circle.to_python)
        reg_array_type('CIRCLEARRAYTYPE', CIRCLEARRAY, CIRCLETYPE)


CircleRecord = namedtuple('CircleRecord', ('center', 'radius'))


class _LineAdapter(_PointAdapter):

    def getquoted(self):
        a = adapt(self.value.a)
        b = adapt(self.value.b)
        c = adapt(self.value.c)
        return b"'{%s, %s, %s}'::line" % (a.getquoted(),
                                          b.getquoted(),
                                          c.getquoted())


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

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return tuple(self.value)
        return None

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return LineRecord(*eval(val))

    @staticmethod
    def register_adapter():
        register_adapter(LineRecord, _LineAdapter)
        LINETYPE = reg_type('LINETYPE', LINE, Line.to_python)
        reg_array_type('LINEARRAYTYPE', LINEARRAY, LINETYPE)


LineRecord = namedtuple('LineRecord', ('a', 'b', 'c'))


class _LSegAdapter(_PointAdapter):

    def getquoted(self):
        ax = adapt(self.value.a.x)
        ay = adapt(self.value.a.y)
        bx = adapt(self.value.b.x)
        by = adapt(self.value.b.y)
        box = b"((%s, %s), (%s, %s))" % (ax.getquoted(), ay.getquoted(),
                                         bx.getquoted(), by.getquoted())
        return b"'%s'::lseg" % box


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

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return tuple(tuple(point) for point in self.value)
        return None

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return LSegRecord(*eval(val))

    @staticmethod
    def register_adapter():
        register_adapter(LSegRecord, _LSegAdapter)
        LSEGTYPE = reg_type('LSEGTYPE', LSEG, LSeg.to_python)
        reg_array_type('LSEGARRAYTYPE', LSEGARRAY, LSEGTYPE)


LSegRecord = namedtuple('LSegRecord', ('a', 'b'))


class _PathAdapter(_PointAdapter):

    def getquoted(self):
        points = (b"(%s, %s)" % (adapt(x).getquoted(), adapt(y).getquoted())
                  for x, y in self.value[:-1])
        ctype = b"(%s)" if self.value.closed else b"[%s]"
        ctype = ctype % b", ".join(points)
        return b"'%s'::path" % ctype


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
        return Field.copy(self, *args, closed=self._closed, **kwargs)

    __copy__ = copy

    def close(self):
        self._closed = True

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return tuple(self.value)
        return None

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        closed = not val.startswith('[')
        return PathRecord(*eval(val), closed=closed)

    @staticmethod
    def register_adapter():
        register_adapter(PathRecord, _PathAdapter)
        PATHTYPE = reg_type('PATHTYPE', PATH, Path.to_python)
        reg_array_type('PATHARRAYTYPE', PATHARRAY, PATHTYPE)


class PathRecord(UserList):
    __slots__ = ('data',)

    def __init__(self, *paths, closed=False):
        p = list('_' for _ in paths)
        p.append('closed')
        nt = namedtuple('PathRecord', p, rename=True)
        f = list((PointRecord(_[0], _[1])
                 if not isinstance(_, PointRecord) else _
                 for _ in paths))
        f.append(closed)
        self.data = nt(*f)

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.data.__getattribute__(name)


class _PolygonAdapter(_PointAdapter):

    def getquoted(self):
        points = (b"(%s, %s)" % (adapt(point.x).getquoted(),
                                 adapt(point.y).getquoted())
                  for point in self.value)
        points = b"(%s)" % b", ".join(points)
        return b"'%s'::polygon" % points


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

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return tuple(tuple(point) for point in self.value)
        return None

    @staticmethod
    def to_python(val, cur):
        if val is None:
            return val
        return PolygonRecord(eval(val))

    @staticmethod
    def register_adapter():
        register_adapter(PolygonRecord, _PolygonAdapter)
        POLYGONTYPE = reg_type('POLYGONTYPE', POLYGON, Polygon.to_python)
        POLYGONARRAYTYPE = reg_array_type('POLYGONARRAYTYPE',
                                          POLYGONARRAY,
                                          POLYGONTYPE)


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
