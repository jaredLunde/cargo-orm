find ./ -type f -exec sed -i -e 's/vital\.sql/bloom/g' {} \;;
find ./ -type f -exec sed -i -e 's/\/VitalSQL/\/bloom-orm/g' {} \;;
find ./ -type f -exec sed -i -e 's/VitalSQL/Bloom ORM/g' {} \;;
find ./ -type f -exec sed -i -e 's/Vital/Bloom/g' {} \;;
find ./ -type f -exec sed -i -e 's/from vital import config/from kola import config/g' {} \;;
find ./ -type f -exec sed -i -e 's/create_vital_pool/create_kola_pool/g' {} \;;
find ./ -type f -exec sed -i -e 's/create_vital_client/create_kola_client/g' {} \;;
