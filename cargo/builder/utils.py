"""

  `Cargo SQL Builder Utils`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import os
import inspect
from collections import OrderedDict

from cargo import fields
from cargo.clients import db
from cargo.expressions import safe
from cargo.etc.translator import postgres


__all__ = ('_get_sql_file', '_get_all_args', '_find_sql_field', 'BaseCreator')


path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _find_sql_field(datatype, udtype, category):
    """ See http://www.postgresql.org/docs/9.5/static/datatype.html """
    return postgres.translate_from(datatype, udtype, category)


def _get_sql_file(name, searchpath=''):
    f = None
    searchpath = os.path.join(path, 'etc/_postgres_assets', searchpath,
                              name + '.sql')
    sql_file = None
    with open(searchpath, 'r') as f:
        sql_file = f.read()
    return sql_file


def _get_base_classes(obj, ignore_builtins=False):
        """ @obj: a python class object
            -> #list of :class:Class base classes of @obj
        """
        return inspect.getmro(obj)[:-1]


def _get_args_dict(obj, arglist=None):
    arglist = arglist or []
    add_arg = arglist.append
    try:
        for k, v in inspect.signature(obj).parameters.items():
            if v.kind == v.VAR_POSITIONAL:
                add_arg(("*{}".format(k), "_NODEFAULT"))
            elif v.kind == v.VAR_KEYWORD:
                add_arg(("**{}".format(k), "_NODEFAULT"))
            elif k == "self":
                pass
            elif v.default is v.empty:
                add_arg(("{}".format(k), "_NODEFAULT"))
            else:
                add_arg(("{}".format(k), v.default))
    except ValueError:
        #: @obj not supported by signature
        pass
    return arglist


def _get_all_args(obj, is_method=False):
        """ @obj: any callable python object
            @is_method: #bool whether or not the callable is a class method

            -> :class:OrderedDict |{arg_name: default_value}| of all arguments
                and keyword arguments accepted by :prop:obj
        """
        arglist = [(k, v)
                   for cls in _get_base_classes(obj)
                   for k, v in _get_args_dict(cls)]
        arglist.extend(_get_args_dict(obj))
        return OrderedDict(arglist)


class BaseCreator(object):

    def __init__(self, orm, name):
        orm = orm if orm is not None else db
        self.orm = orm.copy()
        try:
            self._name = name
        except AttributeError:
            pass

    def __repr__(self):
        try:
            return "<{}:`{}`>".format(self.__class__.__name__,
                                      self.query.mogrified)
        except AttributeError:
            try:
                return "<{}:`{}`:{}>".format(self.__class__.__name__,
                                             self.query,
                                             self.query.params)
            except AttributeError:
                return "<{}:`{}`>".format(self.__class__.__name__,
                                          self.query)

    def _add(self, *clauses):
        for clause in clauses:
            if clause:
                self.orm.state.add(clause)

    def _cast_safe(self, val):
        if isinstance(val, str):
            return safe(val)
        elif isinstance(val, BaseCreator):
            return val.query
        else:
            return val

    @property
    def _common_name(self):
        return self.name

    @property
    def name(self):
        return self._cast_safe(self._name)

    @property
    def string(self):
        return self.query.string

    @property
    def params(self):
        return self.query.params

    def execute(self):
        return self.orm.execute(self.query.string, self.query.params)

    create = execute
