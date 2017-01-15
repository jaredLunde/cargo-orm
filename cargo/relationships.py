"""

  `Cargo ORM Relationships`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
import copy
import importlib
from functools import lru_cache

from pydoc import locate, ErrorDuringImport

from vital.cache import cached_property
from vital.debug import preprX, get_obj_name

from cargo.fields import *
from cargo.etc.types import *
from cargo.expressions import Clause, safe
from cargo.exceptions import RelationshipImportError, PullError


__all__ = (
  'BaseRelationship',
  'ForeignKey',
  'Relationship',
  'Reference'
)


@lru_cache(1000)
def _import_from(owner_module, string):
    obj = locate(string)
    if obj is None:
        full_string = "{}.{}".format(owner_module, string)
        obj = locate(full_string)
        if obj is None:
            *name, attr = full_string.split(".")
            name = ".".join(name)
            try:
                mod = importlib.import_module(name)
                obj = getattr(mod, attr)
            except (AttributeError, ImportError):
                *name, attr_sup = name.split(".")
                name = ".".join(name)
                mod = importlib.import_module(name)
                obj = getattr(getattr(mod, attr_sup), attr)
    return obj


class _ForeignObject(object):
    __slots__ = tuple()

    def pull(self, *args, dry=False, naked=False, **kwargs):
        model = self.ref.model.where(self.ref.field == self.value)
        if naked:
            model.naked()
        if dry:
            model.dry()
        return model.get(*args, **kwargs)


class BaseRelationship(object):

    def _raise_forge_error(self, path, additional=None):
        raise RelationshipImportError("Could not import object at {}. {}"
                                      .format(path, additional or ""))

    def _find(self, string):
        try:
            obj = _import_from(self._owner.__module__, string)
        except:
            self._raise_forge_error(string)
        return obj


class Reference(object):

    def __init__(self, model, field_name, constraints=None, schema=None):
        """`Reference`
            ==================================================================
            This object is added to :class:ForeignKey fields as
            the |ref| property. It provides accessed to the model
            and field objects which the foreign key is a reference to.
        """
        self._model = model
        self._forged = False
        self._schema = schema
        self.field_name = field_name
        self.constraints = constraints or []

    __repr__ = preprX('_model', 'field_name')

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.model.__getattribute__(name)

    def __setattr__(self, name, value):
        try:
            super().__setattr__(name, value)
        except AttributeError:
            self.model.__setattr__(name, value)

    def __setitem__(self, name, value):
        self.model[name] = value

    def __delitem__(self, name):
        del self.model[name]

    def add_constraint(self, name, val=None):
        """ Adds foreign key constraints to the reference. This is used
            primarily with :class:cargo.builder.Builder

            @name: (#str) the clause, e.g. |name='MATCH PARTIAL'|
            @val: (#str) the clause value,
                e.g. |name='on update', val='cascade'|

            ..
                # OPTIONS:
                # [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ]
                # [ ON DELETE action ] [ ON UPDATE action ]
                field.ref.add_constraint('on delete', 'cascade')
                field.ref.add_constraint('on update', 'cascade')
            ..
        """
        if not isinstance(name, Clause):
            clause = Clause(name, safe(val) if val is not None else _empty)
        else:
            clause = name
        self.constraints.append(clause)
        return self

    def on_update(self, val):
        return self.add_constraint('ON UPDATE', val)

    def on_delete(self, val):
        return self.add_constraint('ON DELETE', val)

    @cached_property
    def model(self):
        """ The referenced :class:Model """
        self._forged = True
        return self._model(schema=self._schema)

    @cached_property
    def field(self):
        """ The refenced :class:Field """
        field = getattr(self.model, self.field_name)
        return field

    def copy(self):
        cls = self.__class__(self.model, self.field_name, self.constraints)
        try:
            cls.model = self.model.copy()
        except AttributeError:
            pass
        cls._forged = self._forged
        return cls

    __copy__ = copy


class ForeignKeyState(object):
    """ State object for pickling and unpickling """
    __slots__ = ('args', 'kwargs', 'relation', 'ref')

    def __init__(self, args, kwargs, relation, ref):
        self.args = args
        self.kwargs = kwargs
        self.relation = relation
        self.ref = ref


class ForeignKey(BaseRelationship, _ForeignObject):
    """ ===============================================================
        ``Usage Example``

        Adds a Foreign Key in Images that references the user id in Users
        ..
            from cargo import *

            class Users(Model):
                uid = UID()

            class Images(Model):
                uid = UID()
                owner = ForeignKey('Users.uid', index=True,  not_null=True,
                                   relation='images')
        ..


        The Foreign Key field contains a reference to its parent. When
        the 'relation' keyword argument is provided, the Foreign Key field
        will create a :class:Relationship reference back to itself from
        its parent using the provided argument value as the attribute name.
        ..
            images = Images()
            users = Users()
            isinstance(images.owner.ref.field, users.uid)
            # True
            isinstance(images.owner.ref.model, Users)
            # True
            isinstance(users.images['owner'], images.owner.__class__)
            # True
        ..

    """

    def __init__(self, ref, *args, relation=None, **kwargs):
        """`Foreign Keys`
            ==================================================================
            @ref: (#str) python path to the :class:Field which
                this foreign key is a reference to e.g.
                |coolapp.model.User.uid|
            @relation: (#str) attribute name of :class:Relationship to set
                in the model which @ref is referencing
            @*args and @**kwargs will get passed to the the :class:Field
        """
        self._owner = None
        self._owner_attr = None
        self._ref = ref
        self._relation = relation
        self._args = args
        self._kwargs = kwargs

    __repr__ = preprX('_ref')

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__ = dict

    def _find_ref(self, string):
        """ Finds an object based on @string """
        string = string.split(".")
        field_name = string[-1]
        string = ".".join(string[:-1])
        try:
            obj = getattr(self._find(string), field_name)
        except AttributeError:
            self._raise_forge_error(
                string,
                'Field `{}` not found in `{}`'.format(field_name, string))
        if isinstance(obj, Field):
            return obj
        elif isinstance(obj, ForeignKey):
            return obj.get_field()
        else:
            self._raise_forge_error(string,
                                    'The object found was not a Field.')

    @cached_property
    def ref(self):
        return self._find_ref(self._ref)

    @cached_property
    def ref_model(self):
        return self._find('.'.join(self._ref.split(".")[:-1]))

    def get_field(self):
        """ Gets the ForeignKeyField object, which inherits the traits of
            the owner field. ForeignKeyField provides an attribute
            named |ref| containing the :class:Reference to which
            the foreign key refers. :class:Reference provides both the
            model and the field referenced.
        """
        _class = copy.copy(self.ref.__class__)
        _args, _kwargs = self._args, self._kwargs
        _owner, _owner_attr = self._owner, self._owner_attr
        _relation = self._relation
        _ref = self._ref
        _ref_model, _ref_attr = self.ref_model, _ref.split(".")[-1]
        _slots = list(_class.__slots__)
        _slots.append('ref')
        _slots.append('_state')

        class ForeignKeyField(_class, _ForeignObject):
            __slots__ = _slots
            __doc__ = _class.__doc__

            def __init__(self, *args, **kwargs):
                primary = False
                if 'primary' in _kwargs:
                    primary = _kwargs['primary']
                    del _kwargs['primary']
                super().__init__(*_args, primary=primary, **_kwargs)
                self.default = _kwargs.get('default')
                self.table = _owner.table
                self.field_name = _owner_attr
                self.ref = Reference(_ref_model, _ref_attr,
                                     schema=_owner.schema)
                self._state = ForeignKeyState(_args, _kwargs, _relation, _ref)

            __repr__ = _class.__repr__

            def clear(self):
                super().clear()
                if self.ref._forged:
                    self.ref.model.reset()
                    self.ref.model.reset_fields()

            def copy(self):
                cls = _class.copy(self)
                try:
                    cls.ref = self.ref.copy()
                except AttributeError:
                    cls.ref = Reference(_ref_model, _ref_attr)
                cls._state = self._state
                return cls

        return ForeignKeyField()

    def _create_relation(self):
        """ Creates the :class:Relationship object in the parent model
            at the attribute specified in :prop:_relation
        """
        _owner = '%s.%s' % (self._owner.__class__.__module__,
                            self._owner.__class__.__name__)
        objname = "%s.%s" % (_owner, self._owner_attr)
        setattr(self.ref_model, self._relation, Relationship(objname))

    def forge(self, owner, attribute):
        """ Called when the @owner :class:Model is initialized. Makes
            this relationship 'official' and usable.

            @owner: (:class:Model) owner model which is forging this
                relationship
            @attribute: (#str) name of the attribute in the owner where the
                relationship resides
        """
        self._owner = owner
        self._owner_attr = attribute
        self._forged = True
        field = self.get_field()
        owner._add_field(field)
        if self._relation:
            self._create_relation()
        return field

    def copy(self):
        return self.__class__(self._ref, relation=self._relation)


class Relationship(BaseRelationship):
    """ ======================================================================
        ``Usage Example``

        Forge a |JOIN| relationship between two models.
        ..
            from cargo import *


            class Users(Model):
                uid = UID()
                images = Relationship('coolapp.model.Images.owner')


            class Images(Model):
                uid = UID()
                owner = ForeignKey('Users.uid')
        ..
        |FROM users JOIN images ON images.owner = users.uid|
        |FROM images JOIN users ON users.uid = images.owner|

        This is the same as:
        ..
            from cargo import *

            class Users(Model):
                uid = UID()

            class Images(Model):
                uid = UID()
                owner = ForeignKey('coolapp.model.Users.uid',
                                   relation="images")
        ..

        ======================================================================
        ``Pull data from a relationship``
        ..
            user = Users(uid=1761)
            user.pull()  # Pulls all relationship information
            print(user.images['owner'])
        ..
        |1761|

        This is the same as:
        ..
            user = Users(uid=1761)
            user.images.pull()  # Pulls all relationship information
                                # for images
            print(user.images['owner'])
        ..
        |1761|
    """
    def __init__(self, foreign_key):
        """`Relationships`
            ==================================================================
            This class must be used with :class:ForeignKey. It inherits the
            the :class:Model which the foreign key belongs to and has access
            to all methods and properties of the model. The foreign key can
            be accessed through :prop:foreign_key.

            @foreign_key: (#str) full python path to the foreign key which
                possesses the |JOIN| information to the relationship e.g.
                |coolapp.models.Images.owner|
        """
        self._owner = None
        self._owner_attr = None
        self._foreign_key = foreign_key
        self._forged = False

    __repr__ = preprX('_foreign_key', '_model_cls')

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self._model.__getattribute__(name)

    def __setattr__(self, name, value):
        try:
            super().__setattr__(name, value)
        except AttributeError:
            self._model.__setattr__(name, value)

    def __getitem__(self, name):
        return self._model[name]

    def __setitem__(self, name, value):
        self._model[name] = value

    def __delitem__(self, name):
        del self._model[name]

    def __getstate__(self):
        """ For pickling """
        d = self.__dict__.copy()
        return d

    def __setstate__(self, dict):
        """ For pickling """
        self.__dict__ = dict

    def _find_foreign_key(self, string):
        """ Finds an object based on @string """
        obj = self._find(".".join(string.split(".")[:-1]))
        try:
            obj = getattr(obj(), string.split(".")[-1])
        except AttributeError:
            self._raise_forge_error(string)
        if isinstance(obj, _ForeignObject):
            return obj
        else:
            self._raise_forge_error(
                string, 'The object found was not a ForeignKey.')

    def filter(self, *args, **kwargs):
        self._model.filter(*args, **kwargs)
        return self

    def one(self):
        self._model.one()
        return self

    def pull(self, *args, offset=0, limit=0, order_field=None, reverse=None,
             dry=False, **kwargs):
        """ Pulls data from the relationship model based on the data in
            the :prop:join_field

            @offset: (#int) cursor start position
            @limit: (#int) total number of results to fetch
            @reverse: (#bool) True if returning in descending order
            @order_field: (:class:cargo.Field) object to order the
                query by
            @*args and @**kwargs get passed to the :meth:Model.select query
        """
        if self.join_field.value_is_null and not self.state.has('WHERE'):
            raise PullError(('Required field `{}` was empty and no explicit ' +
                             'WHERE clause was specified.')
                            .format(self.join_field.name))
        model = self._model
        if offset:
            model.offset(offset)
        if limit:
            model.limit(limit)
        if order_field is not None:
            model.order_by(order_field.asc() if not reverse else
                           order_field.desc())
        model.where(self.foreign_key.eq(self.join_field.value))
        if dry:
            model.dry()
        results = model.select(*args, **kwargs)
        return results

    def pull_one(self, *args, offset=0, order_field=None, reverse=None,
                 dry=False, **kwargs):
        """ Pulls a single row from the relationship model based on the data in
            the :prop:join_field

            @offset: (#int) cursor start position
            @reverse: (#bool) True if returning in descending order
            @order_field: (:class:cargo.Field) object to order the
                query by
            @*args and @**kwargs get passed to the :meth:Model.select query
        """
        self._model.one()
        return self.pull(*args,
                         offset=offset,
                         order_field=order_field,
                         reverse=reverse,
                         dry=dry,
                         **kwargs)

    @cached_property
    def foreign_key(self):
        """ -> :class:ForeignKey found from :prop:_foreign_key """
        return self._find_foreign_key(self._foreign_key)

    @cached_property
    def join_field(self):
        """ -> :class:Field referenced by :prop:foreign_key """
        return getattr(self._owner, self.foreign_key.ref.field.field_name)

    @cached_property
    def _model_cls(self):
        """ Uninitialized :class:Model which :prop:foreign_key belongs to """
        return self._find('.'.join(self._foreign_key.split(".")[:-1]))

    @cached_property
    def _model(self):
        """ Initialized :class:Model which :prop:foreign_key belongs to """
        return self._model_cls(schema=self._owner.schema).copy()

    def forge(self, owner, attribute):
        """ Called when the @owner :class:Model is initialized. Makes
            this relationship 'official' and usable.

            @owner: (:class:Model) owner model which is forging this
                relationship
            @attribute: (#str) name of the attribute in the owner where the
                relationship resides
        """
        cls = self.__class__(self._foreign_key)
        cls._owner = owner
        cls._owner_attr = attribute
        cls._forged = True
        cls._owner._relationships.append(cls)
        cls._owner.__setattr__(attribute, cls)
        return cls

    def copy(self, owner=None):
        owner = owner or self._owner
        if self._forged and owner:
            cls = self.__class__(self._foreign_key)
            cls.forge(owner, self._owner_attr)
            return cls
        else:
            return self.__class__(self._foreign_key)
