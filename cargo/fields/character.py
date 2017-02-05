"""

  `Cargo SQL Character Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import psycopg2
from cargo.etc.types import *
from cargo.etc.translator.postgres import OID_map
from cargo.expressions import *
from cargo.fields.field import Field
from cargo.validators import CharValidator


__all__ = ('Char', 'Varchar', 'Text', 'CiText')


class Char(Field, StringLogic):
    """ ======================================================================
        Field object for the PostgreSQL field type |CHAR|
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'minlen',
                 'maxlen', 'table')
    OID = CHAR

    def __init__(self, maxlen, minlen=0,  validator=CharValidator,  **kwargs):
        """`Char`
            ==================================================================
            Fixed-length, blank-padded character field
            @maxlen: (#int) blank-padded length of string value - this is the
                exact length of the field in the SQL table
            @minlen: (#int) minimum length of string value
            ==================================================================
            :see::meth:Field.__init__
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

    def copy(self):
        return Field.copy(self, maxlen=self.maxlen, minlen=self.minlen)

    __copy__ = copy


class Varchar(Char):
    """ ======================================================================
        Field object for the PostgreSQL field type |VARCHAR|
    """
    __slots__ = Char.__slots__
    OID = VARCHAR

    def __init__(self, maxlen=-1, minlen=0, **kwargs):
        """`Varchar`
            Variable-length character field with 10485760 byte maximum limit.
            ==================================================================
            @maxlen: (#int) maximum length of string value
            @minlen: (#int) minimum length of string value
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(minlen=minlen, maxlen=maxlen, **kwargs)

    @property
    def type_name(self):
        if self.maxlen > 0:
            return 'varchar(%s)' % self.maxlen
        else:
            return 'varchar'


class Text(Char):
    """ ======================================================================
        Field object for the PostgreSQL field type |TEXT|
    """
    __slots__ = Char.__slots__
    OID = TEXT

    def __init__(self, maxlen=-1, minlen=0, **kwargs):
        """`Text`
            Variable unlimited-length character field.
            ==================================================================
            @maxlen: (#int) maximum length of string value
            @minlen: (#int) minimum length of string value
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(minlen=minlen, maxlen=maxlen, **kwargs)

    @property
    def type_name(self):
        return 'text'


class CiText(Text):
    """ ======================================================================
        Field object for PostgreSQL CITEXT types.
        ======================================================================
    """
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'validator', '_alias', 'default', 'minlen', 'maxlen',
                 'table', '_type_oid', '_type_array_oid')
    OID = CITEXT

    def __init__(self, **kwargs):
        """`CiText`
            ==================================================================
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(**kwargs)
        self._type_oid = None
        self._type_array_oid = None

    @property
    def type_name(self):
        return 'citext'

    def _register_oid(self, db):
        if self._type_oid is None and self._type_array_oid is None:
            OIDs = db.get_type_OID('citext')
            self._type_oid, self._type_array_oid = OIDs

    def register_type(self, db):
        try:
            self._register_oid(db)
            OID, ARRAY_OID = db.get_type_OID('citext')
            reg_array_type('CITEXTARRAYTYPE',
                           self._type_array_oid,
                           psycopg2.STRING)
        except ValueError:
            warnings.warn('Type `citext` was not found in the database.')

    def copy(self, **kwargs):
        cls = Field.copy(self,
                         maxlen=self.maxlen,
                         minlen=self.minlen,
                         **kwargs)
        cls._type_oid = self._type_oid
        cls._type_array_oid = self._type_array_oid
        return cls

    __copy__ = copy
