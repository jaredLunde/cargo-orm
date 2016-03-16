# Cargo ORM
A fast, friendly, Postgres ORM written in Python.


## Getting started
`pip install cargo-orm`

### or

```shell
git clone https://github.com/jaredlunde/cargo-orm
cd cargo-orm
python setup.py install
```

### Hello Model
```python
from cargo import create_db, Model, db


class Hello(Model):
    uid = UID()  # Primary key
    text = Text(default='Hello Word')    
```


©2016 Jared Lunde
