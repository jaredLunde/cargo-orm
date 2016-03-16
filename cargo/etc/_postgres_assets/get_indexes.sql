SELECT ns.nspname AS schema,
       idx.indrelid::regclass AS table,
       idx.indisunique AS ARG_unique,
       idx.indisprimary AS ARG_primary,
       am.amname AS type,
	   i.relname AS index_name,
       ARRAY(
         SELECT pg_get_indexdef(idx.indexrelid, k + 1, true)
         FROM generate_subscripts(idx.indkey, 1) as k
         ORDER BY k
       ) AS fields

FROM  pg_index as idx

JOIN pg_class AS i
  ON i.oid = idx.indexrelid
JOIN pg_am AS am
  ON i.relam = am.oid
JOIN pg_namespace AS ns
  ON ns.oid = i.relnamespace
 AND ns.nspname = ANY(current_schemas(false))

WHERE nspname = %(schema)s AND indrelid = %(table)s::regclass;
