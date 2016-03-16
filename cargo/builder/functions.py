"""

  `Cargo ORM Function Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Function',)


class Function(BaseCreator):

    def __init__(self, orm, function, expression, *opt, returns=None,
                 replace=False, language=None, dry=False, **opts):
        """ `Create a Function`
            :see::func:cargo.builders.create_function
        """
        super().__init__(orm, name=None)
        self._function = self._cast_safe(function)
        self._expression = self._cast_safe(expression)
        opts.update({o: True for o in opt})

        self._options = None
        if opts:
            self.options(**opts)

        self._replace = None
        if replace:
            self.replace()

        self._returns = None
        if returns:
            self.returns(returns)

        self._lang = None
        if language:
            self.lang(language)

    def options(self, **opt):
        opt_ = []
        for k, v in opt.items():
            k = k.replace("_", " ").strip()
            if v is True:
                cls = safe(k)
            elif k.lower() == 'as':
                if not isinstance(v, (tuple, list)):
                    v = [v]
                cls = CommaClause(k, *v)
            else:
                cls = Clause(k, self._cast_safe(v))
            opt_.append(cls)
        self._options = Clause("", *opt_)
        return self

    def lang(self, lang):
        self._lang = Clause('LANGUAGE', self._cast_safe(lang))
        return self

    def replace(self):
        self._replace = True
        return self

    def returns(self, cast):
        self._returns = Clause('RETURNS', self._cast_safe(cast))
        return self

    def returns_table(self, *cols):
        """ @*cols: (#tuple) |(col_name data_type)| """
        cols = (safe(" ".join(map(str, col))) for col in cols)
        self._returns = ValuesClause('RETURNS TABLE', *cols)

    @property
    def _common_name(self):
        return self._function

    @property
    def query(self):
        '''
        CREATE [ OR REPLACE ] FUNCTION
            name ( [ [ argmode ] [ argname ] argtype [ { DEFAULT | = }
                default_expr ] [, ...] ] )
            [ RETURNS rettype
              | RETURNS TABLE ( column_name column_type [, ...] ) ]
          { LANGUAGE lang_name
            | WINDOW
            | IMMUTABLE | STABLE | VOLATILE | [ NOT ] LEAKPROOF
            | CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
            | [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER
            | COST execution_cost
            | ROWS result_rows
            | SET configuration_parameter { TO value | = value | FROM CURRENT }
            | AS 'definition'
            | AS 'obj_file', 'link_symbol'
          }
        '''
        self.orm.reset()
        cc = 'CREATE {}FUNCTION'.format('OR REPLACE ' if self._replace else "")
        self._add(Clause(cc, self._function),
                  self._returns,
                  Clause('AS', self._expression),
                  self._lang,
                  self._options)
        return Raw(self.orm)
