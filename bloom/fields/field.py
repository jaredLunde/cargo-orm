"""

  `Bloom SQL Field`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import copy

from psycopg2.extensions import *

from vital.cache import cached_property
from vital.debug import prepr

from bloom.etc.types import *
from bloom.exceptions import *
from bloom.expressions import *
from bloom.validators import *


__all__ = ('Field',)


class Field(BaseLogic):
    """ =======================================================================
        This is the base field object. You can create new custom fields
        like so:
        ..
            class MyCoolField(Field):

                def validate(self):
                    if not some_custom_validation():
                        return False
                    return self._validate()
        ..
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'default', '_validator', '_alias', 'table')
    OID = UNKNOWN
    empty = _empty

    def __init__(self, value=empty, not_null=None, primary=None,
                 unique=None, index=None, default=None, validator=None):
        """ ``SQL Field``

            @value: value to populate the field with
            @not_null: (#bool) True if the field cannot be |Null|
            @primary: (#bool) True if this field is the primary key in your
                table
            @unique: (#bool) True if this field is a unique index in your table
            @index: (#bool) True if this field is a plain index in your table,
                that is, not unique or primary
            @default: default value to set the field to
            @validator: (:class:Validator) validator plugin,
                :meth:Validator.validate must return True if the field
                validates, and False if it does not. It must also include an
                |error| attribute which stores the content of the error message
                if the validation fails.
        """
        self.field_name = None
        self.primary = primary
        self.unique = unique
        self.index = index
        self.not_null = not_null
        self.table = None
        try:
            self.default = default
        except AttributeError:
            """ Ignore error for classes where default is a property not an
                attribute """
            pass
        self.value = self.empty
        self._alias = None
        self._validator = validator
        self.__call__(value)

    @prepr('name', 'value', _no_keys=True)
    def __repr__(self): return

    def __call__(self, value=empty):
        """ Sets the value of the field to @value and returns @value

            -> @value
        """
        if value is not self.empty:
            self._set_value(value)
        return self.value

    def __getstate__(self):
        return dict(
            (slot, getattr(self, slot))
            for slot in self.__slots__
            if hasattr(self, slot))

    def __setstate__(self, state):
        for slot, value in state.items():
            setattr(self, slot, value)

    def _copy(self, *args, **kwargs):
        cls = self.__class__(*args, **kwargs)
        cls.field_name = self.field_name
        cls.primary = self.primary
        cls.unique = self.unique
        cls.index = self.index
        cls.not_null = self.not_null
        if self.value is not None and self.value is not self.empty:
            cls.value = copy.copy(self.value)
        cls.default = self.default
        cls._validator = self._validator
        cls.table = self.table
        cls._alias = self._alias
        return cls

    copy = _copy
    __copy__ = _copy

    @property
    def name(self):
        if self.table:
            return "{}.{}".format(self.table, self.field_name)
        return self.field_name

    def set_alias(self, table=None, name=None):
        """ Used for :class:aliased ==when this field is wrapped with
            :class:aliased, @table.@name, @name, or @table will be used
            instead of the field name. If only a table is given, the alias
            will be your_table.field_name, if a table and a name are provided,
            both are overriden. If only @name is provided, only the name is
            used.
        """
        if table is not None and name is not None:
            self._alias = '{}.{}'.format(table, name)
        elif name:
            self._alias = name
        elif table:
            self._alias = "{}.{}".format(table, self.field_name)
        else:
            self._alias = None

    def alias(self, val, use_field_name=False):
        """ Creates an :class:aliased for the field name

            -> :class:aliased object

            ===================================================================
            ``Usage Example``
            ..
                model.field.alias('better_field')
            ..
            |field AS better_field|
        """
        return aliased("{} AS {}".format(
            self.name if not use_field_name else self.field_name, val))

    def _set_value(self, value):
        """ Sets the value of the field to @value and returns @value

            -> @value
        """
        self.value = value
        return self.value

    def _should_insert(self):
        if not (self.validate() or self.default is not None):
            raise self.validator.raises(self.validator.error,
                                        self.field_name,
                                        code=self.validator.code)
        return self.value is not self.empty

    def _should_update(self):
        if not self.validate():
            raise self.validator.raises(self.validator.error,
                                        self.field_name,
                                        code=self.validator.code)
        return self.value is not self.empty

    def clear(self):
        """ Sets the value of the field to :prop:empty """
        self._set_value(self.empty)

    @cached_property
    def validator(self):
        return self._validator(self)

    def _validate(self):
        if self._validator is not None:
            return self.validator.validate()
        return True

    def validate(self):
        return self._validate()
