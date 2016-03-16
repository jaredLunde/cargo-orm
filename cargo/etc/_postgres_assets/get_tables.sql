SELECT table_schema AS schema,
       table_name AS table,
       com.description AS comment

FROM information_schema.tables

JOIN pg_class pgc
  ON pgc.relname = table_name
LEFT JOIN pg_description as com
      ON com.objoid = pgc.oid
     AND com.objsubid = 0

WHERE table_type = 'BASE TABLE'
  AND NOT table_name LIKE 'pg_%%'
  AND table_schema <> 'information_schema'
  AND table_schema = %(schema)s

ORDER BY pgc.oid ASC;
