find ./ -type f -exec sed -i -e 's/vital\.sql/bloom/g' {} \;;;
find ./ -type f -exec sed -i -e 's/\/VitalSQL/\/bloom-orm/g' {} \;;;
find ./ -type f -exec sed -i -e 's/VitalSQL/Bloom ORM/g' {} \;;;
