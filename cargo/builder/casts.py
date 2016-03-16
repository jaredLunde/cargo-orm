"""

  `Cargo ORM Cast Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Cast',)


class Cast(BaseCreator):

    def __init__(self, orm, source_type, target_type, function=None,
                 as_assignment=False, as_implicit=False, inout=False):
        """ `Create a Cast`
            :see::func:cargo.builders.create_cast
        """
        super().__init__(orm, name=None)
        self._source_type = self._cast_safe(source_type)
        self._target_type = self._cast_safe(target_type)
        self._function = None
        self.function(function)

        self._as_assignment = None
        if as_assignment:
            self.as_assignment()

        self._as_implicit = None
        if as_implicit:
            self.as_implicit()

        self._inout = None
        if inout:
            self.inout()

    def function(self, function=None):
        if function:
            self._function = Clause('WITH FUNCTION', self._cast_safe(function))
        else:
            self._function = Clause('WITHOUT FUNCTION')

    def as_assignment(self):
        self._as_assignment = Clause('AS ASSIGNMENT')

    def as_implicit(self):
        self._as_implicit = Clause(" AS IMPLICIT")

    def inout(self):
        self._inout = Clause('WITH INOUT')

    @property
    def _common_name(self):
        return ' AS '.join((self._source_type, self._target_type))

    @property
    def query(self):
        '''
        CREATE CAST (source_type AS target_type)
        WITH FUNCTION function_name (argument_type [, ...])
        [ AS ASSIGNMENT | AS IMPLICIT ]

        CREATE CAST (source_type AS target_type)
            WITHOUT FUNCTION
            [ AS ASSIGNMENT | AS IMPLICIT ]

        CREATE CAST (source_type AS target_type)
            WITH INOUT
            [ AS ASSIGNMENT | AS IMPLICIT ]
        '''
        self.orm.reset()
        self._add(Clause('CREATE CAST',
                         self._source_type,
                         self._target_type,
                         wrap=True,
                         join_with=" AS "),
                  self._function,
                  self._inout,
                  self._as_assignment,
                  self._as_implicit)
        return Raw(self.orm)
