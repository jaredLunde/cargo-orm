SELECT tc.table_name AS table,
       kcu.column_name AS field,
       ccu.table_name AS reference_table,
       ccu.column_name AS reference_field

FROM information_schema.table_constraints tc

LEFT JOIN information_schema.key_column_usage kcu
       ON tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema = kcu.constraint_schema
      AND tc.constraint_name = kcu.constraint_name

LEFT JOIN information_schema.referential_constraints rc
       ON tc.constraint_catalog = rc.constraint_catalog
      AND tc.constraint_schema = rc.constraint_schema
      AND tc.constraint_name = rc.constraint_name

LEFT JOIN information_schema.constraint_column_usage ccu
       ON rc.unique_constraint_catalog = ccu.constraint_catalog
      AND rc.unique_constraint_schema = ccu.constraint_schema
      AND rc.unique_constraint_name = ccu.constraint_name

WHERE lower(tc.constraint_type) in ('foreign key')
  AND tc.table_schema = %(schema)s
  AND tc.table_name = %(table)s;
