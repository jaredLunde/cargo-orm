"""

  `Cargo Encrypted Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import string
import decimal
import arrow
import collections
from netaddr import *

from psycopg2.extensions import *

from vital.debug import preprX
from vital.tools.encoding import uniorbytes
from vital.security import aes_b64_encrypt, aes_b64_decrypt, randstr

from cargo.etc.types import *
from cargo.etc.translator.postgres import OID_map
from cargo.expressions import *

from cargo.fields.field import Field
from cargo.fields.binary import cargobytes
from cargo.fields.character import Text


__all__ = ('Encrypted', 'EncryptionFactory', 'AESFactory', 'AESBytesFactory')


class EncryptionFactory(object):
    """ Use a custom encryption scheme by providing |encrypt| and |decrypt|
        methdos to a class. The methods must accept both |value| and |secret|
        keyword arguments.
    """
    __slots__ = tuple()
    OID = TEXT

    @staticmethod
    def decrypt(val, secret):
        pass

    @staticmethod
    def encrypt(val, secret):
        pass


class AESFactory(EncryptionFactory):
    """ An encryption factory which uses the AES algorithm. """
    __slots__ = tuple()

    @staticmethod
    def decrypt(val, secret):
        return aes_b64_decrypt(str(val), secret)

    @staticmethod
    def encrypt(val, secret):
        return aes_b64_encrypt(str(val), secret)


class AESBytesFactory(EncryptionFactory):
    """ An encryption factory which uses the AES algorithm. """
    __slots__ = tuple()
    OID = BINARY

    @staticmethod
    def decrypt(val, secret):
        return aes_b64_decrypt(val, secret)

    @staticmethod
    def encrypt(val, secret):
        return cargobytes(
            uniorbytes(aes_b64_encrypt(val, secret), bytes))


class _EncryptedValue(object):
    def __getstate__(self):
        return {}

    def set_field(self, field):
        self.field = field

    def __iadd__(self, other):
        _self = self.__class__(self.__add__(other))
        _self.set_field(self.field)
        return _self

    def __isub__(self, other):
        _self = self.__class__(self.__sub__(other))
        _self.set_field(self.field)
        return _self

    def __imul__(self, other):
        _self = self.__class__(self.__mul__(other))
        _self.set_field(self.field)
        return _self

    def __ipow__(self, other):
        _self = self.__class__(self.__pow__(other))
        _self.set_field(self.field)
        return _self

    def __ixor__(self, other):
        _self = self.__class__(self.__xor__(other))
        _self.set_field(self.field)
        return _self

    def __ior__(self, other):
        _self = self.__class__(self.__or__(other))
        _self.set_field(self.field)
        return _self

    def __imatmul__(self, other):
        _self = self.__class__(self.__matmul__(other))
        _self.set_field(self.field)
        return _self

    def __ilshift__(self, other):
        _self = self.__class__(self.__lshift__(other))
        _self.set_field(self.field)
        return _self

    def __irshift__(self, other):
        _self = self.__class__(self.__rshift__(other))
        _self.set_field(self.field)
        return _self

    def __imod__(self, other):
        _self = self.__class__(self.__mod__(other))
        _self.set_field(self.field)
        return _self

    def __ifloordiv__(self, other):
        _self = self.__class__(self.__floordiv__(other))
        _self.set_field(self.field)
        return _self

    def __itruediv__(self, other):
        _self = self.__class__(self.__truediv__(other))
        _self.set_field(self.field)
        return _self

    def __iconcat__(self, other):
        return self.__class__(self.__concat__(other))
        _self.set_field(self.field)
        return _self

    def __iand__(self, other):
        _self = self.__class__(self.__and__(other))
        _self.set_field(self.field)
        return _self

    @property
    def encrypted(self):
        return self.field.encrypt(self)


class _EncryptedAdapter(object):

    def __init__(self, value):
        self.value = value

    def prepare(self, conn):
        self.conn = conn

    def getquoted(self):
        inherit = category.KEYVALUE
        try:
            if self.value.field.type.OID in inherit:
                adapter = adapt(self.value.field.type(self.value.encrypted))
            else:
                adapter = adapt(self.value.encrypted)
        except AttributeError:
            adapter = adapt(self.value.encrypted)
        adapter.prepare(self.conn)
        return adapter.getquoted()


class encstr(str, _EncryptedValue):
    pass


class encint(int, _EncryptedValue):
    pass


class encfloat(float, _EncryptedValue):
    pass


class encdecimal(decimal.Decimal, _EncryptedValue):
    pass


class encbytes(bytes, _EncryptedValue):
    pass


class encdict(dict, _EncryptedValue):
    pass


class enclist(list, _EncryptedValue):
    pass


class enctuple(tuple, _EncryptedValue):
    pass


class encset(set, _EncryptedValue):
    pass


class encip(IPAddress, _EncryptedValue):
    pass


class encipnet(IPNetwork, _EncryptedValue):
    pass


class enceui(EUI, _EncryptedValue):
    pass


class _EncArrow(arrow.Arrow, _EncryptedValue):
    def __getstate__(self):
        return self.__dict__


def encarrow(a):
    return _EncArrow.fromdatetime(a.datetime)


_enctypes = (((collections.Mapping, collections.ItemsView, dict), encdict),
             (str, encstr),
             (bytes, encbytes),
             (int, encint),
             (float, encfloat),
             (set, encset),
             (tuple, enctuple),
             (decimal.Decimal, encdecimal),
             (IPAddress, encip),
             (IPNetwork, encipnet),
             (EUI, enceui),
             (collections.Iterable, enclist),
             (arrow.Arrow, encarrow))


def _get_enc_value(value, field):
    for instance, typ in _enctypes:
        if isinstance(value, instance):
            ev = typ(value)
            ev.set_field(field)
            return ev
    raise TypeError('Cannot encrypt type `%s`.' % type(value))


class Encrypted(Field):
    """ ==================================================================
        This is a special field which encrypts its value with AES-256
        and a unique initialization vector by default. For that reason,
        these fields are useless as indexes by default. They can become
        indexable by using an :class:EncryptionFactory which does not
        utilize a unique initialization vector. However, by doing so you
        are making your encrypted field inherently insecure and may make
        the encryption aspect of the field useless in high-security
        requirement cases.

        The encryption takes place at application level via this field
        rather than with PGCrypto. The reason being that keys in your
        queries can be exploited in pg_stat_activity and the system logs
        via crypto statements that fail with an error. The decrypted
        value of this field is stored in :prop:value and will be exposed
        when the field is called. The reason for this is to maintain
        consistency with other :class:Field types.

        This field also inherits the attributes of the field in @type. So
        :class:Array fields maintain their ability to |append|, for
        instance.

        The encrypted value is stored in :prop:encrypted

        The default storage type for this type is |text|, not |bytea|
        as one might expect. Special fields generally obey their class's
        type when possible, i.e. with :class:Binary fields.

        =========================
        ``Types and Limitations``
        * :class:Binary types: will be stored as |bytea| in Postgres
        * :class:Integer and :class:Numeric types: will be stored as |text|
            in Postgres
        * :mod:keyvalue types: will remain |json|, |jsonb| and |hstore| types
            and JSONb types. All field keys/values will be converted to
            strings on both client and database layers.
        * :class:Text types: will remain |text|
        * :class:Inet types: will be stored as |text|
        * :mod:datetimes types: will be stored as |text|
        * :mod:identifier, :mod:geometry, :mod:bits, :mod:boolean,
            :class:Enum and :mod:range, types are unsupported, as you
            should expect.
        * :class:Array types: encrypted fields reside INSIDE array fields
            and not the other way around. e.g. |Array(Encrypted(Text()))|

        =========================
        ``Usage``
        ..
            import os
            from cargo.fields import *

            permissions = Encrypted(os.environ.get('CARGO_ENCRYPTION_SECRET'),
                                    type=Text(not_null=True))
        ..

        ``Example with :class:BigInt field``
        ..
            # Plain
            e = Encrypted(Encrypted.generate_secret(),
                          type=BigInt(),
                          factory=AESFactory)
            # Array
            array_e = Array(Encrypted(Encrypted.generate_secret(),
                                      type=BigInt(),
                                      factory=AESFactory))
        ..
    """
    OID = ENCRYPTED
    prefix = '$cipher$'
    __slots__ = ('type', '_secret', 'factory')

    def __init__(self, secret, type=Text(), value=Field.empty,
                 validation=None, factory=None, name=None, table=None):
        """ `Encrypted`
            ==================================================================
            @secret: (#str|#callable) secret key to encrypt the field with. A
                cryptographically secure key can be generated by calling
                :meth:generate_secret. It is recommended that this value
                passed via an environment variable to the field via
                :attr:os.environ. If @secret is #callable, it will be called
                in order to get the value - this is useful for variable
                secret keys.
            @type: (:class:Field) Initialized :class:Field type to cast and
                validate the encrypted field with. The decrypted value is
                what will be used for validation. All parameters such as
                |index| and |default| should be defined in here, as this
                Field class is merely a wrapper.
            @factory: (:class:EncryptionFactory) an object with |encrypt|
                and |decrypt| methods. This is the algorithm which will
                be used to encrypt and decrypt. The methods must accept both
                |value| and |secret| keyword arguments.
        """
        self._secret = secret
        if factory is None:
            factory = AESFactory
            type_ = type
            if type_.OID == ARRAY:
                type_ = type.type
            if type_.OID == BINARY:
                factory = AESBytesFactory
        self.factory = factory
        self.type = type.copy()
        if name:
            self.type.field_name = name
        if table:
            self.type.table = name
        self._alias = None
        self.__call__(value)

    __repr__ = preprX('type', keyless=True)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self.type.__call__(self.decrypt(value))
            if self._labeled(self.type.value):
                self.type.__call__(self.decrypt(self.type.value))
        return self.value

    def __str__(self):
        return self.type.__str__()

    def __int__(self):
        return self.type.__int__()

    def __float__(self):
        return self.type.__float__()

    def __bytes__(self):
        return self.type.__bytes__()

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.type.__getattribute__(name)

    def __setattr__(self, name, value):
        try:
            super().__setattr__(name, value)
        except AttributeError:
            self.type.__setattr__(name, value)

    def __getitem__(self, name):
        return self.type.__getitem__(name)

    def __setitem__(self, name, value):
        return self.type.__setitem__(name, value)

    def __delitem__(self, name):
        self.type.__delitem__(name)

    @property
    def type_name(self):
        if self.type.OID not in category.KEYVALUE:
            return OID_map[self.factory.OID]
        return self.type.type_name

    @property
    def value(self):
        try:
            return _get_enc_value(self.type.value, self)
        except TypeError:
            return self.type.value

    @value.setter
    def value(self, value):
        self.type.value = value

    @property
    def field_name(self):
        return self.type.field_name

    @field_name.setter
    def field_name(self, value):
        self.type.field_name = value

    @property
    def table(self):
        return self.type.table

    @table.setter
    def table(self, value):
        self.type.table = value

    @property
    def primary(self):
        return self.type.primary

    @primary.setter
    def primary(self, value):
        self.type.primary = value

    @property
    def unique(self):
        return self.type.unique

    @unique.setter
    def unique(self, value):
        self.type.unique = value

    @property
    def index(self):
        return self.type.index

    @index.setter
    def index(self, value):
        self.type.index = value

    @property
    def not_null(self):
        return self.type.not_null

    @not_null.setter
    def not_null(self, value):
        self.type.not_null = value

    @property
    def _alias(self):
        return self.type._alias

    @_alias.setter
    def _alias(self, value):
        self.type._alias = value

    @staticmethod
    def generate_secret(size=32,
                        keyspace=string.ascii_letters+string.digits+'.+#/'):
        if size not in {16, 24, 32}:
            raise ValueError('AES secret key size must be of size ' +
                             '16, 24 or 32.')
        return randstr(size, keyspace)

    def encrypt(self, val):
        """ Encrypts @val with the local :prop:secret """
        if isinstance(val, (list, tuple)):
            out = []
            add_out = out.append
            for v in val:
                add_out(self.encrypt(v))
            return out if not isinstance(val, tuple) else list(out)
        elif hasattr(val, '__iter__') and hasattr(val, 'items')\
                and callable(val.items):
            out = {}
            for k, v in val.items():
                out[self.encrypt(k)] = self.encrypt(v)
            return out
        else:
            return self._label(self.factory.encrypt(val, self.secret))

    def _decrypt_list(self, val):
        decrypt = self.decrypt
        val = list(val)
        for x, v in enumerate(val.copy()):
            val[x] = decrypt(v)
        return val

    def _decrypt_dict(self, val):
        decrypt = self.decrypt
        for k, v in val.copy().items():
            del val[k]
            val[decrypt(k)] = decrypt(v)
        return val

    def decrypt(self, val):
        """ Encrypts @val with the local :prop:secret """
        if isinstance(val, (list, tuple)):
            return self._decrypt_list(val)
        elif hasattr(val, '__iter__') and hasattr(val, 'items')\
                and callable(val.items):
            return self._decrypt_dict(val)
        else:
            if self._labeled(val):
                val = self._unlabel(val)
                val = self.factory.decrypt(val, self.secret)
            return val

    @property
    def secret(self):
        """ -> (#str) secret key for encryption """
        if callable(self._secret):
            return self._secret()
        else:
            return self._secret

    @property
    def encrypted(self):
        """ This will inevitably return a new value each time it is called
            when using a :class:EncryptionFactory with an initialization
            vector.
            -> (#str) the encrypted :prop:value with :prop:prefix
        """
        return self.encrypt(self.value)

    def _label(self, val):
        """ Adds |$cipher$| prefix to @val """
        if val is not None and not self._labeled(val):
            prefix = self.prefix
            if isinstance(val, bytes):
                prefix = prefix.encode()
            return prefix + val
        else:
            return val

    def _unlabel(self, val):
        """ Removes |__cargocrypt__:| prefix from @val """
        if val is not None and self._labeled(val):
            prefix = self.prefix
            if isinstance(val, bytes):
                prefix = prefix.encode()
                return val.replace(prefix, b'', 1)
            return val.replace(prefix, '', 1)
        else:
            return val

    def _labeled(self, val):
        """ -> (#bool) |True| if @val is labeled with :prop:prefix  """
        prefix = self.prefix
        try:
            if isinstance(val, bytes):
                prefix = prefix.encode()
            return val.startswith(prefix)
        except (AttributeError, TypeError):
            return False

    @property
    def validation(self):
        return self.type.validation

    @property
    def validation_error(self):
        return self.type.validation_error

    def validate(self):
        return self.type.validate()

    def register_adapter(self):
        register_adapter(encint, _EncryptedAdapter)
        register_adapter(encstr, _EncryptedAdapter)
        register_adapter(encfloat, _EncryptedAdapter)
        register_adapter(encdecimal, _EncryptedAdapter)
        register_adapter(encbytes, _EncryptedAdapter)
        register_adapter(enclist, _EncryptedAdapter)
        register_adapter(encdict, _EncryptedAdapter)
        register_adapter(enctuple, _EncryptedAdapter)
        register_adapter(encset, _EncryptedAdapter)
        register_adapter(_EncArrow, _EncryptedAdapter)
        register_adapter(encip, _EncryptedAdapter)
        register_adapter(encipnet, _EncryptedAdapter)
        register_adapter(enceui, _EncryptedAdapter)
        self.type.register_adapter()

    def copy(self, *args, **kwargs):
        return self.__class__(self._secret,
                              *args,
                              type=self.type.copy(),
                              factory=self.factory,
                              **kwargs)

    def clear(self):
        self.type.clear()

    __copy__ = copy
