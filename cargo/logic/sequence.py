"""
  `Sequence-type Logic and Operations`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.expressions import *


__all__ = ('ArrayLogic', 'EnumLogic')


class ArrayLogic(BaseLogic):
    __slots__ = tuple()
    CONTAINS_OP = '@>'
    CONTAINED_OP = '<@'
    OVERLAPS_OP = '&&'

    def contains(self, array, **kwargs):
        """ Creates a |@>| SQL expression
            @array: (#list) or #tuple object

            -> SQL :class:Expression object
            ===================================================================

            ``Usage Example``
            ..
                condition = model.array_field.contains([1, 2])
                model.where(condition)
            ..
            |array_field @> [1,2]|
        """
        return Expression(self, self.CONTAINS_OP, list(array), **kwargs)

    def contained_by(self, array, **kwargs):
        """ Creates a |<@| SQL expression
            @array: (#list) or #tuple object

            -> SQL :class:Expression object
            ===================================================================

            ``Usage Example``
            ..
                condition = model.array_field.is_contained_by([1, 2])
                model.where(condition)
            ..
            |array_field <@ [1,2]|
        """
        return Expression(self, self.CONTAINED_OP, list(array), **kwargs)

    def overlaps(self, array, **kwargs):
        """ Creates a |&&| (overlaps) SQL expression
            @array: (#list) or #tuple object

            -> SQL :class:Expression object
            ===================================================================

            ``Usage Example``
            ..
                condition = model.array_field.overlaps([1, 2])
                model.where(condition)
            ..
            |array_field && [1,2]|
        """
        return Expression(self, self.OVERLAPS_OP, list(array), **kwargs)

    def all(self, target, **kwargs):
        """ Creates an |ALL| SQL expression
            @target: (#list) or #tuple object

            -> SQL :class:Expression object
            ===================================================================

            ``Usage Example``
            ..
                condition = model.array_field.all([1, 2])
                model.where(condition)
            ..
            |[1,2] = ALL(array_field)|
        """
        return Expression(target, "=", Function("ALL", self), **kwargs)

    def any(self, target, **kwargs):
        """ Creates an |ANY| SQL expression
            @target: (#list) or #tuple object

            -> SQL :class:Expression object
            ===================================================================

            ``Usage Example``
            ..
                condition = model.array_field.any(1)
                model.where(condition)
            ..
            |1 = ANY(array_field)|
        """
        return Expression(target, "=", Function("ANY", self), **kwargs)

    def some(self, target, **kwargs):
        """ Creates a |SOME| SQL expression
            @target: (#list) or #tuple object

            -> SQL :class:Expression object
            ===================================================================

            ``Usage Example``
            ..
                condition = model.array_field.some([1, 2])
                model.where(condition)
            ..
            |[1,2] = SOME(array_field)|
        """
        return Expression(list(target), "=", Function("SOME", self), **kwargs)

    def length(self, dimension=1, **kwargs):
        """ :see::meth:F.array_length """
        return F.array_length(self, dimension, **kwargs)

    def append_el(self, element, **kwargs):
        """ :see::meth:F.array_append """
        return F.array_append(self, element, **kwargs)

    def prepend_el(self, element, **kwargs):
        """ :see::meth:F.array_prepend """
        return F.array_prepend(self, element, **kwargs)

    def remove_el(self, element, **kwargs):
        """ :see::meth:F.array_remove """
        return F.array_remove(self, element, **kwargs)

    def replace_el(self, element, new_element, **kwargs):
        """ :see::meth:F.array_replace """
        return F.array_replace(self, element, new_element, **kwargs)

    def position(self, element, **kwargs):
        """ :see::meth:F.array_position """
        return F.array_position(self, element, **kwargs)

    def positions(self, element, **kwargs):
        """ :see::meth:F.array_positions """
        return F.array_positions(self, element, **kwargs)

    def ndims(self, **kwargs):
        """ :see::meth:F.array_ndims """
        return F.ndims(self, **kwargs)

    def dims(self, **kwargs):
        """ :see::meth:F.array_dims """
        return F.dims(self, **kwargs)

    def concat(self, other, **kwargs):
        """ :see::meth:F.array_concat """
        return F.array_cat(self, other, **kwargs)

    def unnest(self, **kwargs):
        """ :see::meth:F.unnest """
        return F.unnest(self, **kwargs)


class EnumLogic(NumericLogic, StringLogic):
    __slots__ = tuple()
    '''
    enum_first(anyenum)	Returns the first value of the input enum type	enum_first(null::rainbow)	red
    enum_last(anyenum)	Returns the last value of the input enum type	enum_last(null::rainbow)	purple
    enum_range(anyenum)	Returns all values of the input enum type in an ordered array	enum_range(null::rainbow)	{red,orange,yellow,green,blue,purple}
    enum_range(anyenum, anyenum)	Returns the range between the two given enum values, as an ordered array. The values must be from the same enum type. If the first parameter is null, the result will start with the first value of the enum type. If the second parameter is null, the result will end with the last value of the enum type.	enum_range('orange'::rainbow, 'green'::rainbow)	{orange,yellow,green}
    enum_range(NULL, 'green'::rainbow)	{red,orange,yellow,green}
    enum_range('orange'::rainbow, NULL)	{orange,yellow,green,blue,purple}
    '''
