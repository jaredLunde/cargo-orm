# Bloom ORM
A fast, friendly, Postgre ORM written in Python.


## Getting started
`pip install bloom-orm`

### or 

```shell
git clone https://github.com/jaredlunde/bloom-orm
cd bloom-orm
python setup.py install
```

### Hello Model
```python
from bloom import create_db, Model, db


class Hello(Model):
    uid = UID()  # Primary key
    text = Text(default='Hello Word')    
```


©2016 Jared Lunde
