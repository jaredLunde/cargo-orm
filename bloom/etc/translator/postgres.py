"""

  `Bloom SQL Postgres Translators`
  ``Translates Postgres types to Bloom SQL types``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.exceptions import TranslationError
from bloom.etc.types import *


__all__ = ('datatype_map', 'OID_map', 'udtype_map', 'category_map',
           'translate_to', 'translate_from')


datatype_map = {
    'array': 'Array',
    'bigint': 'BigInt',
    'bigserial': 'BigSerial',
    'bit': 'Bit',
    'bit varying': 'Varbit',
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
    'money': 'Money',
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
    'time with time zone': 'TimeTZ',
    'time without time zone': 'Time',
    'time': 'Time',
    'timestamp with time zone': 'TimestampTZ',
    'timestamp without time zone': 'Timestamp',
    'timestamp': 'Timestamp',
    'tsrange': 'TimestampRange',
    'tstzrange': 'TimestampTZRange',
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
    'timetz': 'TimeTZ',
    'timestamptz': 'TimestampTZ',
    'varchar': 'Varchar',
    'varbit': 'Varbit'
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


OID_map = {
    BIT: 'bit',
    VARBIT: 'varbit',
    INT: 'integer',
    SLUG: 'text',
    IP: 'inet',
    DATE: 'date',
    TIMESTAMP: 'timestamp',
    ENUM: 'USER-DEFINED',
    FLOAT: 'real',
    DOUBLE: 'double precision',
    BINARY: 'bytea',
    DECIMAL: 'decimal',
    BIGINT: 'bigint',
    SMALLINT: 'smallint',
    TEXT: 'text',
    BOOL: 'boolean',
    CHAR: 'char',
    ARRAY: 'ARRAY',
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
    HSTORE: 'hstore',
    INTRANGE: 'int4range',
    BIGINTRANGE: 'int8range',
    NUMRANGE: 'numrange',
    TSRANGE: 'tsrange',
    TSTZRANGE: 'tstzrange',
    DATERANGE: 'daterange',
    CIDR: 'cidr',
    BOX: 'box',
    CIRCLE: 'circle',
    LINE: 'line',
    LSEG: 'lseg',
    PATH: 'path',
    POINT: 'point',
    POLYGON: 'polygon',
    MACADDR: 'macaddr',
    CURRENCY: 'decimal',
    MONEY: 'money'
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


def translate_to(OID, opt=None):
    realtype = OID_map[OID]
    if opt:
        return '{}({})'.format(realtype, opt)
    else:
        return realtype
