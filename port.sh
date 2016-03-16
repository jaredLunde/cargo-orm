find ./ -type f -exec sed -i -e 's/vital\.sql/bloom/g' {} \;;
find ./ -type f -exec sed -i -e 's/\/VitalSQL/\/bloom-orm/g' {} \;;
find ./ -type f -exec sed -i -e 's/VitalSQL/Bloom ORM/g' {} \;;
find ./ -type f -exec sed -i -e 's/Vital/Bloom/g' {} \;;
find ./ -type f -exec sed -i -e 's/from vital import config/from kola import config/g' {} \;;
find ./ -type f -exec sed -i -e 's/create_vital_pool/create_kola_pool/g' {} \;;
find ./ -type f -exec sed -i -e 's/create_vital_client/create_kola_client/g' {} \;;
find ./ -type f -exec sed -i -e 's/from vital import/from kola import/g' {} \;;
find ./ -type f -exec sed -i -e 's/unit_tests\.sql/unit_tests/g' {} \;;

find ./ -type f -exec sed -i -e 's/unit_tests\.sql/unit_tests/g' {} \;;


# -*- coding: utf-8 -*-
# -*==coding: utf-8 -*-

find ./ -type f -exec sed -i -e 's/#!\/usr\/bin\/python3 \-S//g' {} \;;
find ./ -type f -exec sed -i -e 's/# \-\*==coding\: utf\-8 \-\*\-//g' {} \;;
find ./ -type f -exec sed -i -e 's/# \-\*\- coding\: utf\-8 \-\*\-//g' {} \;;
find ./ -type f -exec sed -i -e '/./,$!d' {} \;;


find ./ -type f -exec sed -i -e 's/Bloom/Cargo/g' {} \;;
find ./ -type f -exec sed -i -e 's/bloom/cargo/g' {} \;;
find ./ -type f -exec sed -i -e 's/\/bloom-orm/\/cargo-orm/g' {} \;;
