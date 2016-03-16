"""

  `Cargo ORM Database Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Database',)


class Database(BaseCreator):

    def __init__(self, orm, name, owner=None, template=None, tablespace=None,
                 encoding=None, lc_collate=None, lc_ctype=None, connlimit=-1):
        """ `Create a Database`
            :see::func:cargo.builders.create_database
        """
        super().__init__(orm, name)
        self._owner = None
        if owner:
            self.owner(owner)

        self._template = None
        if template:
            self.template(template)

        self._tablespace = None
        if tablespace:
            self.tablespace(tablespace)

        self._encoding = None
        if encoding:
            self.encoding(encoding)

        self._lc_collate = None
        if lc_collate:
            self.lc_collate(lc_collate)

        self._lc_ctype = None
        if lc_ctype:
            self.lc_ctype(lc_ctype)

        self._connlimit = None
        if connlimit and connlimit > 0:
            self.connlimit(connlimit)

    def owner(self, owner):
        self._owner = Clause('OWNER', self._cast_safe(owner))
        return self

    def template(self, tpl):
        self._template = Clause('TEMPLATE', self._cast_safe(tpl))
        return self

    def encoding(self, enc):
        self._encoding = Clause('ENCODING', enc)
        return self

    def tablespace(self, tablespace):
        self._tablespace = Clause('TABLESPACE', self._cast_safe(tablespace))
        return self

    def lc_collate(self, col):
        self._lc_collate = Clause('LC_COLLATE', col)
        return self

    def lc_ctype(self, ctype):
        self._lc_ctype = Clause('LC_CTYPE', ctype)
        return self

    def connlimit(self, limit):
        self._connlimit = Clause("CONNECTION LIMIT", limit)
        return self

    @property
    def query(self):
        '''
        CREATE DATABASE name
        [ [ WITH ] [ OWNER [=] user_name ]
               [ TEMPLATE [=] template ]
               [ ENCODING [=] encoding ]
               [ LC_COLLATE [=] lc_collate ]
               [ LC_CTYPE [=] lc_ctype ]
               [ TABLESPACE [=] tablespace_name ]
               [ CONNECTION LIMIT [=] connlimit ] ]
        '''
        self.orm.reset()
        self._add(Clause("CREATE DATABASE", self.name),
                  self._owner,
                  self._template,
                  self._encoding,
                  self._lc_collate,
                  self._lc_ctype,
                  self._tablespace,
                  self._connlimit)
        return Raw(self.orm)
