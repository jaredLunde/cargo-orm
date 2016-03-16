"""

  `Cargo ORM Domain Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Domain',)


class Domain(BaseCreator):

    def __init__(self, orm, name, data_type, collate=None, default=None,
                 constraint=None, not_null=False, check=None):
        """ `Create a Domain`
            :see::func:cargo.builders.create_domain
        """
        super().__init__(orm, name)
        self.data_type = self._cast_safe(data_type)

        self._collate = None
        if collate:
            self.collate(collate)

        self._default = None
        if default:
            self.default(default)

        self._constraint = None
        if constraint:
            self.constraint(constraint)

        self._not_null = None
        if not_null:
            self.not_null()

        self._check = None
        if check:
            self.check(check)

    def collate(self, collate):
        self._collate = Clause('COLLATE', collate)
        return self

    def default(self, default):
        self._default = Clause('DEFAULT', default)
        return self

    def constraint(self, constraint):
        self._constraint = Clause('CONSTRAINT', constraint)
        return self

    def not_null(self):
        self._not_null = Clause('NOT NULL')
        return self

    def check(self, expression):
        self._check = Clause('CHECK', self._cast_safe(expression))
        return self

    @property
    def query(self):
        '''
        CREATE DOMAIN name [ AS ] data_type
            [ COLLATE collation ]
            [ DEFAULT expression ]
            [ constraint [ ... ] ]

        where constraint is:

        [ CONSTRAINT constraint_name ]
        { NOT NULL | NULL | CHECK (expression) }
        '''
        self.orm.reset()
        self._add(Clause('CREATE DOMAIN',
                         self.name,
                         self.data_type,
                         join_with=" AS "),
                  self._collate,
                  self._default,
                  self._constraint,
                  self._not_null,
                  self._check)
        return Raw(self.orm)
