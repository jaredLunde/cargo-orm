"""

  `Cargo ORM Operator Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Operator',)


class Operator(BaseCreator):

    def __init__(self, orm, name, func, hashes=False, merges=False, **opts):
        """ `Create an Operator`
            :see::func:cargo.builders.create_operator
        """
        super().__init__(orm, name)
        self.func = safe('PROCEDURE').eq(self._cast_safe(func))

        self._hashes = None
        if hashes:
            self.hashes()

        self._merges = None
        if merges:
            self.merges()

        self._opts = []
        if opts:
            self.opts(**opts)

    def hashes(self):
        self._hashes = Clause('HASHES')
        return self

    def merges(self):
        self._merges = Clause('MERGES')
        return self

    def opts(self, **opts):
        self._opts = []
        for name, val in opts.items():
            cls = safe(name.upper()).eq(self._cast_safe(val))
            self._opts.append(cls)
        return self

    @property
    def query(self):
        '''
        CREATE OPERATOR name (
            PROCEDURE = function_name
            [, LEFTARG = left_type ] [, RIGHTARG = right_type ]
            [, COMMUTATOR = com_op ] [, NEGATOR = neg_op ]
            [, RESTRICT = res_proc ] [, JOIN = join_proc ]
            [, HASHES ] [, MERGES ]
        )
        '''
        self.orm.reset()
        _opts = self._opts.copy()
        _opts.insert(0, self.func)
        if self._hashes:
            _opts.append(self._hashes)
        if self._merges:
            _opts.append(self._merges)
        self._add(Clause('CREATE OPERATOR', self.name),
                  ValuesClause('', *_opts))
        return Raw(self.orm)
