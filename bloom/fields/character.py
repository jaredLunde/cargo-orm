"""

  `Bloom SQL Character Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.etc.types import *
from bloom.etc.translator.postgres import OID_map
from bloom.expressions import *
from bloom.fields.field import Field
from bloom.validators import CharValidator


__all__ = ('Char', 'Varchar', 'Text')


class Char(Field, StringLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |CHAR|
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'minlen',
                 'maxlen', 'table')
    OID = CHAR

    def __init__(self, maxlen, minlen=0,  validator=CharValidator,  **kwargs):
        """ `Char`
            Fixed-length, blank-padded character field

            :see::meth:Field.__init__
            @maxlen: (#int) blank-padded length of string value - this is the
                exact length of the field in the SQL table
            @minlen: (#int) minimum length of string value
        """
        super().__init__(validator=validator, **kwargs)
        self.maxlen = maxlen
        self.minlen = minlen

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self.value = str(value) if value is not None else None
        return self.value

    @property
    def type_name(self):
        return '%s(%s)' % (OID_map[self.OID], self.maxlen)

    def copy(self, *args, **kwargs):
        cls = self._copy(maxlen=self.maxlen, minlen=self.minlen, *args,
                         **kwargs)
        return cls

    __copy__ = copy


class Varchar(Char):
    """ =======================================================================
        Field object for the PostgreSQL field type |VARCHAR|
    """
    __slots__ = Char.__slots__
    OID = VARCHAR

    def __init__(self, maxlen=-1, minlen=0, **kwargs):
        """ `Varchar`
            Variable-length character field with 10485760 byte maximum limit.

            :see::meth:Field.__init__
            @maxlen: (#int) maximum length of string value
            @minlen: (#int) minimum length of string value
        """
        super().__init__(minlen=minlen, maxlen=maxlen, **kwargs)

    @property
    def type_name(self):
        if self.maxlen > 0:
            return 'varchar(%s)' % self.maxlen
        else:
            return 'varchar'


class Text(Char):
    """ =======================================================================
        Field object for the PostgreSQL field type |TEXT|
    """
    __slots__ = Char.__slots__
    OID = TEXT

    def __init__(self, maxlen=-1, minlen=0, **kwargs):
        """ `Text`
            Variable unlimited-length character field.

            :see::meth:Field.__init__
            @maxlen: (#int) maximum length of string value
            @minlen: (#int) minimum length of string value
        """
        super().__init__(minlen=minlen, maxlen=maxlen, **kwargs)

    @property
    def type_name(self):
        return 'text'
