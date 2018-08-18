```python
def get_classmethod(alias, name):
    @classmethod
    def fn(cls, *a, **kw):
        return getattr(cls(), alias)(*a, **kw)
    return fn


class MetaModel(type):
    def __new__(cls, name, bases, dct):
        next_dct = {}
        for k, v in dct.items():
            ins_method_hint = '_do_'
            if k.startswith(ins_method_hint):
                _, cls_method = k.split(ins_method_hint)
                next_dct[cls_method] = get_classmethod(k, cls_method)
                next_dct[k] = v
            else:
                next_dct[k] = v
        return super().__new__(cls, name, bases, next_dct)


class Model(metaclass=MetaModel):
    def __init__(self, *args):
        super().__init__()
        self.instance_var = 'test'
    def _do_select(self, *a):
        print(self.instance_var)
        return ','.join(map(str, a))
```
