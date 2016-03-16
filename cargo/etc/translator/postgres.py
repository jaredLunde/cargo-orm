"""

  `Cargo SQL Postgres Translators`
  ``Translates Postgres types to Cargo SQL types``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
from cargo.exceptions import TranslationError
from cargo.etc.types import *


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
    'C': 'Field',
    'D': 'Timestamp',
    'E': 'Enum',
    'G': 'Point',
    'I': 'Cidr',
    'N': 'Numeric',
    'R': 'IntRange',
    'S': 'Text',
    'T': 'Timestamp',
    'U': 'Field',
    'V': 'Bit',
    'X': 'Field'
}


OID_map = {
    BIT: 'bit',
    VARBIT: 'varbit',
    INT: 'integer',
    SLUG: 'text',
    INET: 'inet',
    DATE: 'date',
    TIMESTAMP: 'timestamp',
    TIMESTAMPTZ: 'timestamptz',
    ENUM: 'USER-DEFINED',
    FLOAT: 'real',
    DOUBLE: 'double precision',
    BINARY: 'bytea',
    DECIMAL: 'numeric',
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
    USERNAME: 'citext',
    EMAIL: 'text',
    TIME: 'time',
    TIMETZ: 'timetz',
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
    CURRENCY: 'numeric',
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


def translate_to(OID):
    return OID_map[OID]
