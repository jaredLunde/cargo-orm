"""

  `Cargo ORM Rule Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Rule',)


class Rule(BaseCreator):

    def __init__(self, orm, name, event, table, command, condition=None,
                 also=False, instead=False, nothing=False, replace=False):
        """ `Create a Rule`
            :see::func:cargo.builders.create_rule
        """
        super().__init__(orm, name)
        self._event = self._cast_safe(event)
        self._table = self._cast_safe(table)
        self._command = self._cast_safe(command)

        self._where = None
        if condition:
            self.where(condition)

        self._replace = None
        if replace:
            self.replace()

        self._also = None
        if also:
            self.also()

        self._instead = None
        if instead:
            self.instead()

        self._nothing = None
        if nothing:
            self.nothing()

    def also(self):
        self._also = Clause('ALSO')
        return self

    def instead(self):
        self._instead = Clause('INSTEAD')
        return self

    def nothing(self):
        self._nothing = Clause('NOTHING')
        return self

    def where(self, condition):
        self._where = Clause('WHERE', condition)

    condition = where

    def replace(self):
        self._replace = Clause('OR REPLACE')

    @property
    def _common_name(self):
        return " ON ".join((self.name, self._table))

    @property
    def query(self):
        '''
        CREATE [ OR REPLACE ] RULE name AS ON event
        TO table_name [ WHERE condition ]
        DO [ ALSO | INSTEAD ] { NOTHING | command | ( command ; command ... ) }

        e.g.
        CREATE RULE "_RETURN" AS
            ON SELECT TO t1
            DO INSTEAD
                SELECT * FROM t2;
        '''
        self.orm.reset()
        self._add(Clause('CREATE', self._replace or _empty, self.name),
                  Clause('AS ON', self._event),
                  Clause('TO', self._table, self._where or _empty),
                  Clause('DO',
                         self._instead or _empty,
                         self._also or _empty,
                         self._nothing or _empty,
                         self._command))
        return Raw(self.orm)
