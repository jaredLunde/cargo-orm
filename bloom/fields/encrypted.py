"""

  `Bloom Encrypted Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import string
import collections

from psycopg2.extensions import *

from vital.debug import prepr
from vital.security import aes_b64_encrypt, aes_b64_decrypt,\
                           aes_encrypt, aes_decrypt, randstr

from bloom.etc.types import *
from bloom.expressions import *

from bloom.fields.field import Field
from bloom.fields.character import Text


__all__ = ('Encrypted', 'EncryptionFactory', 'AESFactory', 'AESBytesFactory')


class EncryptionFactory(object):
    """ Use a custom encryption scheme by providing |encrypt| and |decrypt|
        methdos to a class. The methods must accept both |value| and |secret|
        keyword arguments.
    """
    __slots__ = tuple()

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
        return aes_b64_decrypt(val, secret)

    @staticmethod
    def encrypt(val, secret):
        return aes_b64_encrypt(val, secret)


class AESBytesFactory(EncryptionFactory):
    """ An encryption factory which uses the AES algorithm. """
    __slots__ = tuple()

    @staticmethod
    def decrypt(val, secret):
        return aes_decrypt(val, secret)

    @staticmethod
    def encrypt(val, secret):
        return aes_encrypt(val, secret)


class _EncryptedValue(object):
    def set_field(self, field):
        self.field = field

    @property
    def encrypted(self):
        return self.field.encrypted

    @staticmethod
    def to_db(val):
        inherit = {JSONTYPE, JSONB, BINARY}
        if val.field.type.sqltype in inherit or \
           (val.field.type.sqltype == ARRAY and
                val.field.type.type.sqltype in inherit):
            return val.field._in_type(val.encrypted)
        return adapt(val.encrypted)


class encstr(str, _EncryptedValue):
    pass


class encint(int, _EncryptedValue):
    pass


class encfloat(float, _EncryptedValue):
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


register_adapter(encstr, encstr.to_db)
register_adapter(encint, encint.to_db)
register_adapter(encfloat, encfloat.to_db)
register_adapter(encbytes, encbytes.to_db)
register_adapter(enclist, enclist.to_db)
register_adapter(enctuple, enctuple.to_db)
register_adapter(encset, encset.to_db)

_enctypes = (((collections.Mapping, collections.ItemsView, dict), encdict),
             (str, encstr),
             (bytes, encbytes),
             (int, encint),
             (float, encfloat),
             (set, encset),
             (tuple, enctuple),
             (collections.Iterable, enclist))


def _get_enc_value(value, field):
    for instance, typ in _enctypes:
        if isinstance(value, instance):
            ev = typ(value)
            ev.set_field(field)
            return ev
    raise TypeError('Cannot encrypt type `%s`.' % type(val))


class Encrypted(Field):
    """ This is a special field which encrypts its value with AES 256
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
        * :class:Array types: will be stored as |text[]| arrays in Postgres
            regardless of their defined cast. The cast will be obeyed on the
            client side, however.
        * :class:Binary types: will be stored as |bytea| in Postgres
        * :class:Integer and :class:Numeric types: will be stored as |text|
            in Postgres
        * :class:JSON and JSONb types: will remain JSON and JSONb types. All
            field values will be converted to strings, however, on both
            client and database levels.
        * :class:Text types: will remain text
        * :class:Inet types: will be stored as |text|
        * :mod:identifier, :mod:datetimes, :mod:geometric, :mod:boolean,
            :mod:xml, and :mod:range types are unsupported

        =========================
        ``Usage``
        ..
            import os
            from bloom.fields import *

            permissions = Encrypted(os.environ.get('BLOOM_ENCRYPTION_SECRET'),
                                    type=Array(type=Text, dimensions=1),
                                    not_null=True)
        ..

        ``Example with :class:Binary field``
        ..
            e = Encrypted(os.urandom(32),
                          type=Binary(),
                          factory=AESBinaryFactory)
        ..
    """
    sqltype = ENCRYPTED
    prefix = '$cipher$'
    __slots__ = ('type', '_secret', 'factory')

    def __init__(self, secret, type=Text(), value=Field.empty,
                 validation=None, factory=AESFactory, **kwargs):
        """ `Encrypted`
            :see::meth:Field.__init__
            @secret: (#str|#callable) secret key to encrypt the field with. A
                cryptographically secure key can be generated by calling
                :meth:generate_secret. It is recommended that this value
                passed via an environment variable to the field via
                :attr:os.environ. If @secret is #callable, it will be called
                in order to get the value - this is useful for variable
                secret keys.
            @type: (:class:Field) Initialized :class:Field type to cast and
                validate the encrypted field with. The dencypted value is
                what will be used for validation.
            @factory: (:class:EncryptionFactory) an object with |encrypt|
                and |decrypt| methods. This is the algorithm which will
                be used to encrypt and decrypt. The methods must accept both
                |value| and |secret| keyword arguments.
        """
        self._secret = secret
        if (type.sqltype == BINARY and factory == AESFactory) or\
           (type.sqltype == ARRAY and type.type.sqltype == BINARY):
            factory = AESBytesFactory
        self.factory = factory
        self.type = type.copy()
        self._in_type = type.copy()
        if type.sqltype == ARRAY:
            if type.type.sqltype == BINARY:
                self._in_type.cast = bytes
            else:
                self._in_type.cast = str
        self.type.validation = validation or self.type.validation
        self.type.primary = kwargs.get('primary', self.type.primary)
        self.type.unique = kwargs.get('unique', self.type.unique)
        self.type.index = kwargs.get('index', self.type.index)
        self.type.not_null = kwargs.get('not_null', self.type.not_null)
        try:
            self.type.default = kwargs.get('default', self.type.default)
        except AttributeError:
            """ Ignore error for classes where default is a property not an
                attribute """
            pass
        self._alias = None
        self.__call__(value)

    @prepr('type', _no_keys=True)
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self.type._set_value(
                _get_enc_value(self.type.__call__(self.decrypt(value)), self))
        return self.value

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.type.__getattribute__(name)

    @property
    def value(self):
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
        for x, v in enumerate(val.copy()):
            val[x] = decrypt(v)
        return self.type.__call__(val)

    def _decrypt_dict(self, val):
        decrypt = self.decrypt
        for k, v in val.copy().items():
            del val[k]
            val[decrypt(k)] = decrypt(v)
        return self.type.__call__(val)

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
        """ Adds |__bloomcrypt__:| prefix to @val """
        if val is not None and not self._labeled(val):
            prefix = self.prefix
            if isinstance(val, bytes):
                prefix = prefix.encode()
            return prefix + val
        else:
            return val

    def _unlabel(self, val):
        """ Removes |__bloomcrypt__:| prefix from @val """
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
        try:
            prefix = self.prefix
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

    def copy(self, *args, **kwargs):
        cls = self.__class__(self._secret, *args, type=self.type.copy(),
                             **kwargs)
        cls.field_name = self.field_name
        cls.primary = self.primary
        cls.unique = self.unique
        cls.index = self.index
        cls.not_null = self.not_null
        cls.default = self.default
        cls.table = self.table
        cls._alias = self._alias
        return cls

    __copy__ = copy
