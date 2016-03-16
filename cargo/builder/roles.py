"""

  `Cargo ORM User Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Role', 'User')


class Role(BaseCreator):
    TYPE = 'ROLE'

    def __init__(self, orm, name, *options, in_role=None, in_group=None,
                 role=None, admin=None, user=None, password=None,
                 password_encryption=True, valid_until=None, connlimit=None):
        """ `Create a Role`
            :see::func:cargo.bulider.create_role
        """
        super().__init__(orm, name)
        self.options = []
        self.set_option(*options)
        in_role_ = in_role if isinstance(in_role, (tuple, list)) else [in_role]
        if in_role:
            self.in_role(*in_role_)
        in_group_ = in_group if not in_group or \
            isinstance(in_group, (tuple, list)) else\
            [in_group]
        if in_group:
            self.in_group(*in_group_)
        self.password(password, password_encryption)
        self.valid_until(valid_until)
        self.connlimit(connlimit)

    def set_option(self, *opt):
        self.options.extend(safe(o.upper()) for o in opt)

    def in_role(self, *val):
        self._in_role = (safe(v) for v in val)
        if val:
            self.options.append(Clause('IN ROLE',
                                       *self._in_role, join_with=", "))
        return self

    def in_group(self, *val):
        self._in_group = (safe(v) for v in val)
        if val:
            self.options.append(Clause('IN GROUP',
                                       *self._in_group,
                                       join_with=", "))
        return self

    def password(self, val, encrypted=True):
        self._password = val
        self._password_encryption = encrypted
        if val:
            txt = 'ENCRYPTED ' if encrypted else 'UNENCRYPTED '
            self.options.append(Clause(
                '{}PASSWORD'.format(txt),
                val))
        return self

    def valid_until(self, val):
        self._valid_until = val
        if val:
            self.options.append(Clause('VALID UNTIL', val))
        return self

    def connlimit(self, val):
        self._connlimit = val
        if val:
            self.options.append(Clause('CONNLIMIT', val))
        return self

    @property
    def query(self):
        ''' CREATE USER name [ [ WITH ] option [ ... ] ] '''
        self.orm.state.add(Clause('CREATE ' + self.TYPE, self.name))
        if self.options:
            self.orm.state.add(Clause('WITH', *self.options))
        query = Raw(self.orm)
        self.orm.reset()
        return query


class User(Role):
    TYPE = 'USER'

    def __init__(self, *args, **kwargs):
        """ `Create a User Role`
            :see::class:BaseCreator
        """
        super().__init__(*args, **kwargs)
