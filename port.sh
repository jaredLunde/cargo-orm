find ./ -type f -exec sed -i -e 's/vital\.sponge/vital\.kola/g' {} \;;
find ./ -type f -exec sed -i -e 's/vital\.debug\.debug2/vital\.debug/g' {} \;;
find ./ -type f -exec sed -i -e 's/sponge/kola/g' {} \;;
find ./ -type f -exec sed -i -e 's/Sponge/Kola/g' {} \;;
