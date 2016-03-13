try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple

from psycopg2.extensions import new_type, new_array_type, register_type


#: UNKNOWN TYPES
NOTHING = '_'
UNKNOWN = 705

#: INTEGER TYPES
SMALLINT = 21
SMALLINTARRAY = 1005
INT = 23
INTARRAY = 1007
BIGINT = 20
BIGINTARRAY = 1016

#: CHARACTER TYPES
CHAR = 18
CHARARRAY = 1002
VARCHAR = 1043
VARCHARARRAY = 1015
TEXT = 25
TEXTARRAY = 1009

#: DATETIME TIMES
DATE = 1082
DATEARRAY = 1182
TIMESTAMP = 1114
TIMESTAMPARRAY = 1115
TIMESTAMPTZ = 1184
TIMESTAMPTZARRAY = 1185
TIME = 1083
TIMEARRAY = 1183
TIMETZ = 1266
TIMETZARRAY = 1270

#: NUMERIC TYPES
FLOAT = 700
FLOATARRAY = 1021
DECIMAL = 1700
DECIMALARRAY = 1231
NUMERIC = DECIMAL
NUMERICARRAY = DECIMALARRAY
DOUBLE = 701
DOUBLEARRAY = 1022
CURRENCY = 1700
CURRENCYARRAY = 1231
MONEY = 790
MONEYARRAY = 791

#: BIT TYPES
BIT = 1560
BITARRAY = 1561
VARBIT = 1562
VARBITARRAY = 1563

#: BYTE TYPES
BINARY = 17
BINARYARRAY = 1001

#: BOOLEAN TYPES
BOOL = 16
BOOLARRAY = 1000

#: IDENTIFIER TYPES
SMALLSERIAL = SMALLINT
SMALLSERIALARRAY = SMALLINTARRAY
SERIAL = INT
SERIALARRAY = INTARRAY
BIGSERIAL = BIGINT
BIGSERIALARRAY = BIGINTARRAY
UUIDTYPE = 2950
UUIDARRAY = 2951
UIDTYPE = BIGINT
UIDARRAY = BIGINTARRAY
STRUID = BIGINT
STRUIDARRAY = BIGINTARRAY

#: BASE ARRAY (TEXT ARRAY)
ARRAY = TEXTARRAY

#: ENUM TYPES ARE UNKNOWN
ENUM = -1234

#: SPECIAL TYPES
PASSWORD = TEXT
PASSWORDARRAY = TEXTARRAY
KEY = TEXT
KEYARRAY = TEXTARRAY
ENCRYPTED = TEXT
ENCRYPTEDARRAY = TEXTARRAY
SLUG = TEXT
SLUGARRAY = TEXTARRAY
USERNAME = TEXT
USERNAMEARRAY = TEXTARRAY
EMAIL = TEXT
EMAILARRAY = TEXTARRAY

#: NETWORKING TYPES
IP = 869
IPARRAY = 1041
INET = IP
INETARRAY = IPARRAY
CIDR = 650
CIDRARRAY = 651
MACADDR = 829
MACADDRARRAY = 1040

#: KEYVALUE TYPES
HSTORE = -1235
JSON = 114
JSONARRAY = 199
JSONB = 3802
JSONBARRAY = 3807

#: GEOMETRIC TYPES
BOX = 603
BOXARRAY = 1020
CIRCLE = 718
CIRCLEARRAY = 719
LINE = 628
LINEARRAY = 629
LSEG = 601
LSEGARRAY = 1018
PATH = 602
PATHARRAY = 1019
POINT = 600
POINTARRAY = 1017
POLYGON = 604
POLYGONARRAY = 1027

#: RANGE TYPES
INTRANGE = 3904
INTRANGEARRAY = 3905
BIGINTRANGE = 3926
BIGINTRANGEARRAY = 3927
NUMRANGE = 3906
NUMRANGEARRAY = 3907
TSRANGE = 3908
TSRANGEARRAY = 3909
TSTZRANGE = 3910
TSTZRANGEARRAY = 3911
DATERANGE = 3912
DATERANGEARRAY = 3913


OIDCategory = namedtuple('OIDCategory',
                         ('CHARS', 'INTS', 'NUMERIC', 'ARRAYS', 'BITS',
                          'KEYVALUE', 'DATES', 'GEOMETRIC', 'NETWORK', 'RANGE',
                          'BINARY', 'UUID'))
category = OIDCategory(
    CHARS={TEXT, CHAR, VARCHAR},
    INTS={SMALLINT, INT, BIGINT},
    NUMERIC={FLOAT, DOUBLE, DECIMAL, MONEY},
    ARRAYS={TEXTARRAY, CHARARRAY, VARCHARARRAY, SMALLINTARRAY, INTARRAY,
            BIGINTARRAY, FLOATARRAY, DOUBLEARRAY, DECIMALARRAY, NUMERICARRAY,
            MONEYARRAY, BITARRAY, VARBITARRAY, JSONARRAY, JSONBARRAY,
            TIMEARRAY, TIMETZARRAY, DATEARRAY, TIMESTAMPARRAY,
            TIMESTAMPTZARRAY, POINTARRAY, PATHARRAY, LINEARRAY, LSEGARRAY,
            BOXARRAY, CIRCLEARRAY, POLYGONARRAY, INETARRAY, MACADDRARRAY,
            CIDRARRAY, INTRANGEARRAY, BIGINTRANGEARRAY, NUMRANGEARRAY,
            TSRANGEARRAY, TSTZRANGEARRAY, DATERANGEARRAY, BINARYARRAY,
            UUIDARRAY},
    BITS={BIT, VARBIT},
    KEYVALUE={JSON, JSONB, HSTORE},
    DATES={TIME, TIMETZ, DATE, TIMESTAMP, TIMESTAMPTZ},
    GEOMETRIC={POINT, PATH, LINE, LSEG, BOX, CIRCLE, POLYGON},
    NETWORK={INET, MACADDR, CIDR},
    RANGE={INTRANGE, BIGINTRANGE, NUMRANGE, TSRANGE, TSTZRANGE, DATERANGE},
    BINARY={BINARY},
    UUID={UUIDTYPE}
)


def reg_type(name, oids, callback):
    try:
        oids[0]
    except TypeError:
        oids = (oids,)
    TYPE = new_type(oids, name, callback)
    register_type(TYPE)
    return TYPE


def reg_array_type(name, oids, basetype):
    try:
        oids[0]
    except TypeError:
        oids = (oids,)
    ARRAYTYPE = new_array_type(oids, name, basetype)
    register_type(ARRAYTYPE)
    return ARRAYTYPE
