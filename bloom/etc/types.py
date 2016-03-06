'''
16   -> Just bool
17   -> Just bytea
18   -> Just char
19   -> Just name
20   -> Just int8
21   -> Just int2
23   -> Just int4
24   -> Just regproc
25   -> Just text
26   -> Just oid
27   -> Just tid
28   -> Just xid
29   -> Just cid
142  -> Just xml
600  -> Just point
601  -> Just lseg
602  -> Just path
603  -> Just box
604  -> Just polygon
628  -> Just line
650  -> Just cidr
700  -> Just float4
701  -> Just float8
705  -> Just unknown
718  -> Just circle
790  -> Just money
829  -> Just macaddr
869  -> Just inet
1042 -> Just bpchar
1043 -> Just varchar
1082 -> Just date
1083 -> Just time
1114 -> Just timestamp
1184 -> Just timestamptz
1186 -> Just interval
1266 -> Just timetz
1560 -> Just bit
1562 -> Just varbit
1700 -> Just numeric
1790 -> Just refcursor
2249 -> Just record
2278 -> Just void
2287 -> Just array_record
2202 -> Just regprocedure
2203 -> Just regoper
2204 -> Just regoperator
2205 -> Just regclass
2206 -> Just regtype
2950 -> Just uuid
114  -> Just json
3802 -> Just jsonb
22   -> Just int2vector
30   -> Just oidvector
143  -> Just array_xml
199  -> Just array_json
629  -> Just array_line
651  -> Just array_cidr
719  -> Just array_circle
791  -> Just array_money
1000 -> Just array_bool
1001 -> Just array_bytea
1002 -> Just array_char
1003 -> Just array_name
1005 -> Just array_int2
1006 -> Just array_int2vector
1007 -> Just array_int4
1008 -> Just array_regproc
1009 -> Just array_text
1010 -> Just array_tid
1011 -> Just array_xid
1012 -> Just array_cid
1013 -> Just array_oidvector
1014 -> Just array_bpchar
1015 -> Just array_varchar
1016 -> Just array_int8
1017 -> Just array_point
1018 -> Just array_lseg
1019 -> Just array_path
1020 -> Just array_box
1021 -> Just array_float4
1022 -> Just array_float8
1027 -> Just array_polygon
1028 -> Just array_oid
1040 -> Just array_macaddr
1041 -> Just array_inet
1115 -> Just array_timestamp
1182 -> Just array_date
1183 -> Just array_time
1185 -> Just array_timestamptz
1187 -> Just array_interval
1231 -> Just array_numeric
1270 -> Just array_timetz
1561 -> Just array_bit
1563 -> Just array_varbit
2201 -> Just array_refcursor
2207 -> Just array_regprocedure
2208 -> Just array_regoper
2209 -> Just array_regoperator
2210 -> Just array_regclass
2211 -> Just array_regtype
2951 -> Just array_uuid
3807 -> Just array_jsonb
3904 -> Just int4range
3905 -> Just _int4range
3906 -> Just numrange
3907 -> Just _numrange
3908 -> Just tsrange
3909 -> Just _tsrange
3910 -> Just tstzrange
3911 -> Just _tstzrange
3912 -> Just daterange
3913 -> Just _daterange
3926 -> Just int8range
3927 -> Just _int8range
_ -> Nothing
'''

INT = 23
SLUG = 1
IP = 869
DATE = 1082
TIMESTAMP = 1114
TIMESTAMPTZ = 1184
ENUM = 5
FLOAT = 6
BINARY = 17
DECIMAL = 8
BIGINT = 9
SMALLINT = 10
TEXT = 25
BOOL = 12
CHAR = 13
ARRAY = 14
SERIAL = 15
BIGSERIAL = 16
PASSWORD = 25
KEY = 25
UUIDTYPE = 2950
CHAR = 18
VARCHAR = 21
JSONTYPE = 114
JSON = JSONTYPE
JSONB = 3802
UIDTYPE = 20
STRUID = 20
USERNAME = 1043
EMAIL = 1043
TIME = 1083
TIMETZ = 1266
NUMERIC = 29
SMALLSERIAL = 30
#: TODO: http://www.postgresql.org/docs/8.2/static/functions-geometry.html
HSTORE = 31  # DONE
CIDR = 650  # DONE
BOX = 603
CIRCLE = 718
LINE = 628
LSEG = 601
PATH = 602
POINT = 600
POLYGON = 604
MACADDR = 829  # DONE
CURRENCY = 42
ENCRYPTED = 44  # DONE
DOUBLE = 45  # DONE
INTRANGE = 46
BIGINTRANGE = 47
NUMRANGE = 48
TSRANGE = 49
DATERANGE = 3912
