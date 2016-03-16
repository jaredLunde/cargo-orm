"""

  `Cargo ORM Tablespace Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Tablespace',)


class Tablespace(BaseCreator):

    def __init__(self, orm, name, location, owner=None, **options):
        """ `Create a Tablespace`
            :see::func:cargo.builder.create_tablespace
        """
        super().__init__(orm, name)
        self._location = None
        self.location(location)

        self._owner = None
        if owner:
            self.owner(owner)

        self._options = []
        if options:
            self.options(options)

    def location(self, directory):
        self._location = Clause('LOCATION', directory)
        return self

    def owner(self, owner):
        self._owner = Clause('OWNER', self._cast_safe(owner))
        return self

    def options(self, **opt):
        self._options = []
        for name, val in opt.items():
            self._options.append(safe(name).eq(val))
        return self

    @property
    def query(self):
        '''
        CREATE TABLESPACE tablespace_name [ OWNER user_name ]
        LOCATION 'directory'
        '''
        self.orm.reset()
        options = None
        if self._options:
            options = ValuesClause('WITH', *self._options)
        self._add(Clause('CREATE TABLESPACE', self.name),
                  self._owner,
                  self._location,
                  options)
        return Raw(self.orm)
