"""

  `Bloom SQL Postgres Translators`
  ``Translates Postgres types to Bloom SQL types``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.exceptions import TranslationError
from bloom.etc.types import *


__all__ = ('datatype_map', 'sqltype_map', 'udtype_map', 'category_map',
           'translate_to', 'translate_from')


datatype_map = {
    'array': 'Array',
    'bigint': 'BigInt',
    'bigserial': 'BigSerial',
    'boolean': 'Bool',
    'box': 'Box',
    'bytea': 'Binary',
    'character': 'Char',
    'character varying': 'Varchar',
    'cidr': 'Cidr',
    'circle': 'Circle',
    'daterange': 'DateRange',
    'double precision': 'Double',
    'hstore': 'HStore',
    'inet': 'Inet',
    'integer': 'Int',
    'int4range': 'IntRange',
    'int8range': 'BigIntRange',
    'json': 'Json',
    'jsonb': 'JsonB',
    'line': 'Line',
    'lseg': 'LSeg',
    'macaddr': 'MacAddress',
    'money': 'Decimal',
    'numeric': 'Numeric',
    'numrange': 'NumericRange',
    'path': 'Path',
    'point': 'Point',
    'polygon': 'Polygon',
    'real': 'Float',
    'smallint': 'SmallInt',
    'smallserial': 'SmallSerial',
    'serial': 'Serial',
    'text': 'Text',
    'time without time zone': 'Time',
    'time': 'Time',
    'timestamp without time zone': 'Timestamp',
    'timestamp': 'Timestamp',
    'tsrange': 'TimestampRange',
    'uuid': 'UUID'
}


udtype_map = {
    'int8': 'BigInt',
    'int4': 'Int',
    'int': 'Int',
    'int2': 'SmallInt',
    'bool': 'Bool',
    'serial8': 'BigSerial',
    'serial4': 'Serial',
    'serial2': 'SmallSerial',
    'float8': 'Double',
    'float4': 'Float',
    'decimal': 'Decimal',
    'timetz': 'Time',
    'timestamptz': 'Timestamp',
    'varchar': 'Varchar'
}


category_map = {
    'A': 'Array',
    'B': 'Bool',
    'D': 'Timestamp',
    'E': 'Enum',
    'I': 'Inet',
    'N': 'Numeric',
    'S': 'Text'
}


sqltype_map = {
    INT: 'integer',
    SLUG: 'text',  # Depends if maxlen defined
    IP: 'inet',
    DATE: 'date',
    TIMESTAMP: 'timestamp',
    ENUM: 'USER-DEFINED',
    FLOAT: 'real',  # Depends on digits
    DOUBLE: 'double precision',  # Depends on digits
    BINARY: 'bytea',  # TODO
    DECIMAL: 'decimal',
    BIGINT: 'bigint',
    SMALLINT: 'smallint',
    TEXT: 'text',
    BOOL: 'boolean',
    CHAR: 'char',
    ARRAY: 'ARRAY',
    SMALLSERIAL: 'smallserial',
    SERIAL: 'serial',
    BIGSERIAL: 'bigserial',
    PASSWORD: 'text',
    KEY: 'text',
    UUIDTYPE: 'uuid',
    VARCHAR: 'varchar',
    JSON: 'json',
    JSONB: 'jsonb',
    UIDTYPE: 'bigint',
    STRUID: 'bigint',
    USERNAME: 'varchar',
    EMAIL: 'varchar',
    TIME: 'time',
    NUMERIC: 'numeric',
    ENCRYPTED: 'text',
    # TODO: Geometry and other fields
    HSTORE: 'hstore',
    INTRANGE: 'int4range',
    BIGINTRANGE: 'int8range',
    NUMRANGE: 'numrange',
    TSRANGE: 'tsrange',
    DATERANGE: 'daterange',
    # http://www.postgresql.org/docs/8.2/static/functions-geometry.html
    CIDR: 'cidr',
    BOX: 'box',
    CIRCLE: 'circle',
    LINE: 'line',
    LSEG: 'lseg',
    PATH: 'path',
    POINT: 'point',
    POLYGON: 'polygon',
    MACADDR: 'macaddr',
    CURRENCY: 'money'
}


def translate_from(datatype=None, udtype=None, category=None):
    if datatype and datatype in datatype_map:
        return datatype_map[datatype]
    if udtype and udtype in udtype_map:
        return udtype_map[udtype]
    if category and category in category_map:
        return category_map[category]
    raise TranslationError('Could not find a field for datatype={}, ' +
                           'udtype={}, category={}'.format(datatype,
                                                           udtype,
                                                           category))


def translate_to(sqltype, opt=None):
    realtype = sqltype_map[sqltype]
    if opt:
        return '{}({})'.format(realtype, opt)
    else:
        return realtype
