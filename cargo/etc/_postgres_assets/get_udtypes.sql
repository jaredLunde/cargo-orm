SELECT pg_type.typname AS name,
       pg_enum.enumlabel AS label,
	   pg_enum.enumsortorder AS ordinal,
	   pg_type.typcategory AS cast
FROM pg_type
LEFT JOIN pg_enum
       ON pg_enum.enumtypid = pg_type.oid
WHERE pg_type.typname = %(type)s;
