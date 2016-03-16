SELECT idx.indisprimary AS primary,
       idx.indisunique AS unique,
       idx.indislive AS index,
       pgc.relname AS table,
       a.attname AS field_name,
       pt.typcategory AS category,
       a.attnum AS ordinal,
       pgn.nspname AS schema,
       a.attndims AS ARG_dimensions,
       ic.udt_name AS udtype,
       ic.data_type AS datatype,
       a.attnotnull AS ARG_not_null,
       ic.character_maximum_length AS ARG_maxlen,
       com.description AS comment,
       ic.numeric_precision AS ARG_digits,
       def.adsrc AS ARG_default

FROM pg_attribute AS a

JOIN pg_class AS pgc
  ON pgc.oid = a.attrelid
JOIN pg_type AS pt
  ON pt.oid = a.atttypid
JOIN pg_namespace AS pgn
  ON pgn.oid = pgc.relnamespace
JOIN INFORMATION_SCHEMA.COLUMNS ic
  ON ic.table_schema = pgn.nspname
 AND ic.table_name = pgc.relname
 AND ic.column_name = a.attname
LEFT JOIN pg_index AS idx
       ON pgc.oid = idx.indrelid
      AND idx.indkey[0] = a.attnum
LEFT JOIN pg_description AS com
       ON pgc.oid = com.objoid
      AND a.attnum = com.objsubid
LEFT JOIN pg_attrdef AS def
       ON a.attrelid = def.adrelid
      AND a.attnum = def.adnum

WHERE a.attnum > 0 AND pgc.oid = a.attrelid
  AND pg_table_is_visible(pgc.oid)
  AND NOT a.attisdropped
  AND pgc.relname = %(table)s
  AND pgn.nspname = %(schema)s

ORDER BY a.attnum;
