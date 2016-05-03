"""

  `Cargo SQL Field`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import copy
try:
    import ujson as json
except:
    import json
from vital.debug import preprX

from cargo.etc.types import *
from cargo.etc.translator.postgres import OID_map
from cargo.exceptions import *
from cargo.expressions import BaseLogic, Expression, _empty, safe
from cargo.validators import NullValidator


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
    __slots__ = ('field_name', 'primary', 'unique', 'index', 'not_null',
                 'value', 'default', 'validator', '_alias', 'table')
    OID = UNKNOWN
    empty = _empty

    def __init__(self, value=empty, not_null=None, primary=None,
                 unique=None, index=None, default=None,
                 validator=NullValidator, name=None, table=None):
        """``SQL Field``
            @value: value to populate the field with
            @not_null: (#bool) True if the field cannot be |Null|
            @primary: (#bool) True if this field is the primary key in your
                table
            @unique: (#bool) True if this field is a unique index in your table
            @index: (#bool or #str) |True| if this field is an index in your
                table, you can also pass a #str specific index type
                e.g. |Text(index='btree')| or |Array(Text(), index='gin')|
            @default: default value to set the field to
            @validator: (:class:Validator) validator plugin,
                :meth:Validator.validate must return True if the field
                validates, and False if it does not. It must also include an
                |error| attribute which stores the content of the error message
                if the validation fails. Passing |None| will disable validation
                for this field.
            @name: (#str) the name of the field in the table
            @table: (#str) the name of the table
        """
        self.table = table
        self.field_name = name
        self.primary = primary
        self.unique = unique
        self.index = index
        self.not_null = not_null
        try:
            self.default = default
        except AttributeError:
            """ Ignore error for classes where default is a property not an
                attribute """
            pass
        self._alias = None
        try:
            self.validator = validator(self)
        except TypeError:
            self.validator = None
        self.value = self.empty
        self.__call__(value)

    __repr__ = preprX('name', 'value', keyless=True)

    def __getstate__(self):
        state = {}
        if hasattr(self, '__dict__'):
            state = self.__dict__
        if hasattr(self, '__slots__'):
            state.update({slot: getattr(self, slot)
                          for slot in self.__slots__})
        return state

    def __setstate__(self, state):
        for slot, value in state.items():
            if not isinstance(value, self.empty.__class__):
                setattr(self, slot, value)
            else:
                setattr(self, slot, _empty)

    def __call__(self, value=empty):
        """ Sets the value of the field to @value and returns @value

            -> @value
        """
        if value is not self.empty:
            self.value = value
        return self.value

    def __str__(self):
        try:
            return self.value.__str__()
        except AttributeError:
            return self.__repr__()

    def __int__(self):
        return self.value.__int__()

    def __float__(self):
        return self.value.__float__()

    def __bytes__(self):
        return self.value.__bytes__()

    def __len__(self):
        return self.value.__len__()

    def __bool__(self):
        return True

    def set_name(self, field_name):
        """ @field_name: (#str) name of the field """
        self.field_name = field_name

    def set_table(self, table_name):
        """ @table_name: (#str) name of the table the field belongs to """
        self.table = table_name

    @property
    def value_is_null(self):
        return self.value is self.empty or self.value is None

    @property
    def value_is_not_null(self):
        return self.value is not None and self.value is not self.empty

    @property
    def name(self):
        """ -> (#str) full name of the field with the name of the table
                included. i.e. the field |bar| in a table |foo| returns
                'foo.bar'
        """
        if self.table:
            return "{}.{}".format(self.table, self.field_name)
        return self.field_name

    @property
    def type_name(self):
        return OID_map[self.OID]

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

    def alias(self, val=None, **kwargs):
        """ Creates an :class:aliased for the field name
            @val: (#str) alias name, if |None| :prop:_alias will be used,
                if that is |None| a |ValueError| will be raised
            -> :class:aliased object

            ===================================================================
            ``Usage Example``
            ..
                model.field.alias('better_field')
            ..
            |field AS better_field|
        """
        if val is None:
            val = self._alias
        if val is None:
            raise ValueError('Alias for `%s` cannot be `None`' % self.name)
        return Expression(self, "AS", safe('"%s"' % val), **kwargs)

    def for_json(self):
        """ Prepares a value for being dumped to JSON encoding. This method
            is called when the |to_json| method of a :class:Model is called.
            This can also be thought of as a sort of 'for_python' method, too
            in the sense that it is the entity as it exists outside of the
            field class.
        """
        if self.value_is_null:
            return None
        return str(self)

    def to_json(self):
        """ Dumps the field to JSON encoding.
            -> (#str) JSON-encoded string
        """
        return json.dumps(self.for_json())

    def _should_insert(self):
        if not (self.validate() or self.default is not None):
            raise self.validator.raises(self.validator.error,
                                        self,
                                        code=self.validator.code)
        return self.value is not self.empty

    def _should_update(self):
        if not self.validate():
            raise self.validator.raises(self.validator.error,
                                        self,
                                        code=self.validator.code)
        return self.value is not self.empty

    def validate(self):
        if self.validator is not None:
            return self.validator.validate()
        return True

    def copy(self, *args, **kwargs):
        vc = None
        if self.validator is not None:
            vc = self.validator.__class__
        cls = self.__class__(*args,
                             primary=self.primary,
                             unique=self.unique,
                             index=self.index,
                             not_null=self.not_null,
                             default=self.default,
                             validator=vc,
                             name=self.field_name,
                             table=self.table,
                             **kwargs)
        if self.value_is_not_null:
            try:
                cls.value = self.value.copy()
            except AttributeError:
                cls.value = copy.copy(self.value)
        cls._alias = self._alias
        return cls

    __copy__ = copy

    def clear(self):
        """ Sets the value of the field to :prop:empty """
        self.value = self.empty
        return self

    def reset(self):
        """ Sets the value of the field to :prop:empty and the alias of the
            field to |None
        """
        self.clear()
        self._alias = None
        return self
