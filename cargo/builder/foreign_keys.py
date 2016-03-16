"""

  `Cargo SQL Builder Foreign Keys`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
from vital.debug import prepr


__all__ = ('ForeignKeyMeta',)


class ForeignKeyMeta(object):

    def __init__(self, field_name, table, ref_table=None, ref_field=None,
                 schema=None):
        self.field_name = field_name
        self.table = table
        self.ref_table = ref_table
        self.ref_field = ref_field
        self.schema = schema

    @prepr('field_name', 'table', 'ref_table', 'ref_field')
    def __repr__(self): return
