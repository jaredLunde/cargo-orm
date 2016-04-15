"""

  `Cargo SQL Key-Value Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import psycopg2
import warnings

import decimal
import collections
import psycopg2.extras
import psycopg2.extensions

try:
    import ujson as json
except ImportError:
    import json

from cargo.etc.types import *
from cargo.expressions import *
from cargo.fields.field import Field
from cargo.logic import JsonLogic, JsonBLogic, HStoreLogic, KeyValueOps,\
                        SequenceOps


__all__ = ('Json', 'JsonB', 'HStore')



class _jsontype(object):
    def __iadd__(self, other):
        return self.__class__(self.__add__(other))

    def __isub__(self, other):
        return self.__class__(self.__sub__(other))

    def __imul__(self, other):
        return self.__class__(self.__mul__(other))

    def __ipow__(self, other):
        return self.__class__(self.__pow__(other))

    def __ixor__(self, other):
        return self.__class__(self.__xor__(other))

    def __ior__(self, other):
        return self.__class__(self.__or__(other))

    def __imatmul__(self, other):
        return self.__class__(self.__matmul__(other))

    def __ilshift__(self, other):
        return self.__class__(self.__lshift__(other))

    def __irshift__(self, other):
        return self.__class__(self.__rshift__(other))

    def __imod__(self, other):
        return self.__class__(self.__mod__(other))

    def __ifloordiv__(self, other):
        return self.__class__(self.__floordiv__(other))

    def __itruediv__(self, other):
        return self.__class__(self.__truediv__(other))

    def __iconcat__(self, other):
        return self.__class__(self.__concat__(other))

    def __iand__(self, other):
        return self.__class__(self.__and__(other))

    @staticmethod
    def to_db(value):
        adapt = psycopg2.extensions.adapt
        return psycopg2.extensions.AsIs(
            "%s::json" % adapt(json.dumps(value)).getquoted().decode())

    def __str__(self):
        return self.to_db(self).getquoted().decode('ascii', 'replace')


class jsondict(dict, _jsontype):
    pass


class jsonlist(list, _jsontype):
    pass


class jsonstr(str, _jsontype):
    pass


class jsonint(int, _jsontype):
    pass


class jsonfloat(float, _jsontype):
    pass


class jsondecimal(decimal.Decimal, _jsontype):
    pass


psycopg2.extensions.register_adapter(jsondict, jsondict.to_db)
psycopg2.extensions.register_adapter(jsonlist, jsonlist.to_db)
psycopg2.extensions.register_adapter(jsonstr, jsonstr.to_db)
psycopg2.extensions.register_adapter(jsonint, jsonint.to_db)
psycopg2.extensions.register_adapter(jsonfloat, jsonfloat.to_db)
psycopg2.extensions.register_adapter(jsondecimal, jsondecimal.to_db)

_jsontypes = (((collections.Mapping, collections.ItemsView, dict), jsondict),
              (str, jsonstr),
              (int, jsonint),
              (float, jsonfloat),
              (decimal.Decimal, jsondecimal),
              (collections.Iterable, jsonlist))


def _get_json(val, oid):
    for instance, typ in _jsontypes:
        if isinstance(val, instance):
            return typ(val)
    raise TypeError('Could not adapt type `%s` to json.' % type(val))


class Json(Field, KeyValueOps, SequenceOps, JsonLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |JSON|

        The value given to this field must be Json serializable. It is
        automatically encoded and decoded on insertion and retrieval.
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'default', 'validator', '_alias', 'table', 'cast')
    OID = JSON

    def __init__(self, cast=None, *args, **kwargs):
        """ `Json`
            :see::meth:Field.__init__
            @cast: type cast for specifying the type of data should be expected
                for the value property, e.g. |dict| or |list|
        """
        self.cast = cast
        super().__init__(*args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if self.cast:
                value = self.cast(value)
            if value is not None:
                value = _get_json(value, self.OID)
            self.value = value
        return self.value

    def from_json(self, value):
        """ Loads @value from Json and inserts it as the value of the field """
        return self.__call__(json.loads(value))

    def for_json(self):
        if self.value_is_not_null:
            return self.value
        return None

    @staticmethod
    def to_python(value, cur):
        try:
            return json.loads(value)
        except TypeError:
            return value


JSONTYPE = reg_type('JSONTYPE', (JSON, JSONB), Json.to_python)
JSONARRAYTYPE = reg_array_type('JSONARRAYTYPE', JSONARRAY, JSONTYPE)
JSONBARRAYTYPE = reg_array_type('JSONBARRAYTYPE', JSONBARRAY, JSONTYPE)


class JsonB(Json, JsonBLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |JSONB|

        The value given to this field must be able Json serializable. It is
        automatically encoded and decoded on insertion and retrieval.
    """
    __slots__ = Json.__slots__
    OID = JSONB

    def __init__(self, *args, **kwargs):
        """ `JsonB`
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)


class HStore(Field, KeyValueOps, HStoreLogic):
    __slots__ = Field.__slots__
    OID = HSTORE

    def __init__(self, *args, **kwargs):
        """ `HStore`
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self.value = dict(value) if value is not None else None
        return self.value

    @property
    def type_name(self):
        return 'hstore'

    def for_json(self):
        if self.value_is_not_null:
            return self.value
        return None

    @staticmethod
    def register_adapter():
        psycopg2.extensions.register_adapter(dict,
                                             psycopg2.extras.HstoreAdapter)

    @staticmethod
    def register_type(db):
        try:
            return db.register('hstore')
        except (ValueError, psycopg2.ProgrammingError):
            warnings.warn('Type `hstore` was not found in the database.')
