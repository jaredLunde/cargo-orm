#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Postgres Translators`
  ``Translates Postgres types to Bloom SQL types``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.exceptions import TranslationError
from bloom.etc import types


datatype_map = {
    'array': 'Array',
    'bigint': 'BigInt',
    'bigserial': 'BigSerial',
    'boolean': 'Bool',
    # TODO
    'bytea': 'Binary',
    'character': 'Char',
    'character varying': 'Varchar',
    'double precision': 'Float',
    # TODO
    'hstore': 'HStore',
    'inet': 'Inet',
    'integer': 'Int',
    'json': 'JSON',
    'jsonb': 'JSONb',
    'money': 'Decimal',
    'numeric': 'Numeric',
    'real': 'Float',
    'smallint': 'SmallInt',
    'smallserial': 'SmallSerial',
    'serial': 'Serial',
    'text': 'Text',
    'time without time zone': 'Time',
    'time': 'Time',
    'timestamp without time zone': 'Timestamp',
    'timestamp': 'Timestamp',
    'uuid': 'UUID',
    # TODO
    'xml': 'XML'
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
    'float8': 'Float',
    'float4': 'Float',
    'decimal': 'Decimal',
    'timetz': 'Time',
    'timestamptz': 'Timestamp'
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
    'INT': 'integer',
    'SLUG': 'text',  # Depends if maxlen defined
    'IP': 'inet',
    'DATE': 'date',
    'TIMESTAMP': 'timestamp',
    'ENUM': 'USER-DEFINED',
    'FLOAT': 'real',  # Depends on digits
    'BINARY': 'bytea',  # TODO
    'DECIMAL': 'numeric',
    'BIGINT': 'bigint',
    'SMALLINT': 'smallint',
    'TEXT': 'text',
    'BOOL': 'boolean',
    'CHAR': 'char',
    'ARRAY': 'ARRAY',
    'SERIAL': 'serial',
    'BIGSERIAL': 'bigserial',
    'PASSWORD': 'text',  # Depends if maxlen defined
    'KEY': 'text',
    'UUIDTYPE': 'uuid',
    'VARCHAR': 'varchar',
    'JSONTYPE': 'json',
    'JSONB': 'jsonb',
    'UIDTYPE': 'biginteger',
    'STRUID': 'biginteger',
    'USERNAME': 'varchar',
    'EMAIL': 'varchar',
    'TIME': 'time',
    'NUMERIC': 'numeric',
    'SMALLSERIAL': 'smallserial',
    # TODO: Geometry and other fields
    'HSTORE': 'hstore',
    'RANGE': 'int4range',  # Depends on type
    # http://www.postgresql.org/docs/8.2/static/functions-geometry.html
    'CIDR': 'cidr',
    'BOX': 'box',
    'CIRCLE': 'circle',
    'LINE': 'line',
    'LSEG': 'lseg',
    'PATH': 'path',
    'POINT': 'point',
    'POLYGON': 'polygon',
    'MACADDR': 'macaddr',
    'CURRENCY': 'money',
    'XML': 'xml',
    'ENCRYPTED': 'bytea'
}


def translate_from(datatype=None, udtype=None, category=None):
    if datatype and datatype in datatype_map:
        return datatype_map[datatype]
    if udtype and udtype in udtype_map:
        return udtype_map[udtype]
    if category and category in category_map:
        return category_map[category]
    raise TranslationError('Could not find a field for datatype={}, ' +
                           'udtype={}, category={}')


def translate_to(sqltype, opt=None):
    for name, realtype in sqltype_map.items():
        if sqltype == getattr(types, name):
            break
    if opt:
        return '{}({})'.format(realtype, opt)
    else:
        return realtype
