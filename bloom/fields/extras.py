#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Special Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import re
import copy
import string
from multiprocessing import cpu_count

from slugify import slugify

import argon2
from argon2 import PasswordHasher
from passlib.context import CryptContext

from vital.debug import prepr
from vital.security import randkey, aes_b64_encrypt, aes_b64_decrypt,\
                           aes_encrypt, aes_decrypt, randstr
from vital.tools import strings as string_tools

from bloom.etc import passwords, usernames
from bloom.etc.types import *
from bloom.expressions import *

from bloom.fields.field import Field
from bloom.fields.binary import Binary
from bloom.fields.character import Text
from bloom.fields.sequence import Array

# TODO: Currency field

__all__ = ('Username', 'Email', 'Password', 'Slug', 'SlugFactory',
           'Key', 'AuthKey', 'Encrypted', 'EncryptionFactory', 'AESFactory',
           'AESBytesFactory')


class SlugFactory(object):
    __slots__ = ('slugify_opt',)

    def __init__(self, **slugify_opt):
        """ :see::func:slugify """
        self.slugify_opt = slugify_opt

    def __call__(self, value):
        return slugify(value, **self.slugify_opt)


class Slug(Text):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |TEXT|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', '_factory', 'table')
    sqltype = SLUG

    def __init__(self, value=Field.empty, minlen=0, maxlen=-1,
                 slug_factory=None, **kwargs):
        """ `Slug`
            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
            @slug_factory: (:class:SlugFactory)
        """
        self._factory = slug_factory
        super().__init__(value=value, minlen=minlen, maxlen=maxlen, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is None:
                value = None
            else:
                value = self.slugify(str(value))
            self._set_value(value)
        return self.value

    def slugify(self, value):
        if self._factory is not None:
            return self._factory(value)
        else:
            return slugify(
                value, max_length=self.maxlen, word_boundary=False)

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minlen = self.minlen
        cls.maxlen = self.maxlen
        cls._factory = self._factory
        return cls

    __copy__ = copy


_pwd_contexts = {}


def get_pwd_context(schemes, rounds, salt_size):
    context_key = "{}.{}.{}".format(
        schemes, rounds, salt_size)
    if not _pwd_contexts.get(context_key):
        context = CryptContext(
            # crypt schemes to allow
            schemes=schemes,
            default='bcrypt',
            # vary rounds parameter randomly when creating new hashes...
            all__vary_rounds=0.12,
            # set the number of rounds that should be used...
            # (appropriate values may vary for different schemes,
            # and the amount of time you wish it to take)
            all__default_rounds=rounds if rounds > 4 else 5,
            bcrypt__default_rounds=31 if rounds > 31 else rounds,
            bcrypt__ident='2b',
            bcrypt_sha256__default_rounds=31 if rounds > 31 else rounds,
            pbkdf2_sha512__salt_size=salt_size,
            pbkdf2_sha256__salt_size=salt_size)
        _pwd_contexts[context_key] = context
    return _pwd_contexts[context_key]


class Password(Field, StringLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for PostgreSQL CHAR/TEXT types.

        This object also validates that a password fits given requirements
        such as 'minlen' and hashes plain text with supplied hash schemes
        using :class:CryptContext.

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            password = Password('coolpasswordbrah')
            print(password)
        ..
        |$pbkdf2-sha512$19083$VsoZY6y1NmYsZWxtDQEAoBQCoJRSaq01BiAEQMg5JwQg5P...|

        ========================================================================
        ..
            password.verify('coolpasswordbro')
        ..
        |False|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_value', 'validation_error', '_alias',
        'default', 'minlen', 'maxlen', 'scheme', 'schemes', 'salt_size',
        'strict', 'table')
    sqltype = PASSWORD

    def __init__(self, value=Field.empty, minlen=8, maxlen=-1, scheme="argon2",
                 schemes=None, salt_size=16, rounds=15, strict=True,
                 **kwargs):
        """ `Password`

            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
            @scheme: (#str) :class:CryptContext scheme to use
            @schemes: (#list) of :class:CryptContext schemes, not required if
                scheme is |argon2|
            @salt_size: (#int) length of chars to salt password with
            @rounds: (#int) number of rounds to hash the value for
            @strict: (#bool) True if you wish to blacklist the most commonly
                used passwords

            TODO: Explicitly state if something is a hash coming in
        """
        super().__init__(**kwargs)
        self.minlen, self.maxlen = minlen, maxlen
        self.rounds = rounds
        self.scheme = scheme
        self.schemes = schemes or [
            "pbkdf2_sha512", "bcrypt_sha256", "bcrypt", "pbkdf2_sha256"]
        self.salt_size = salt_size
        self.strict = strict
        value_is_hash = self.is_hash(value)
        self._set_value(
            self.encrypt(value) if value and not value_is_hash else None)
        self.validation_value = str(value) \
            if value and not value_is_hash else None

    @prepr('name', 'scheme', _no_keys=True)
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            is_hash = self.is_hash(value)
            if not is_hash and value is not None:
                self.validation_value = str(value)
                self._set_value(self.encrypt(str(value)))
            else:
                self._set_value(value)
        return self.value

    def is_hash(self, value):
        """ Tests whether or not a given @value is already hashed or
            is plain-text
        """
        if not value or not isinstance(value, (str, bytes)):
            return False
        try:
            _null, scheme, *_ = value.split('$')
            return scheme.strip() in {
                s.replace('_', '-')
                for s in self.schemes + ['argon2i', 'argon2d']
            }
        except:
            return False

    def argon2_encrypt(self, value, time_cost=0, memory_cost=1024,
                       parallelism=0,  hash_len=45, salt_len=0,
                       encoding='utf-8'):
        """ Hashes @value using the argon2 algorithm.

            @value: value to hash
            @time_cost: (#int) Defines the amount of computation realized and
                therefore the execution time, given in number of iterations.
            @memory_cost: (#int) Defines the memory usage, given in kibibytes.
            @parallelism: (#int) Defines the number of parallel threads
                (changes the resulting hash value).
            @hash_len: (#int) Length of the hash in bytes.
            @salt_len: (#int) Length of random salt to be generated for each
                password.
            @encoding: (#str) The Argon2 C library expects bytes. So if hash()
                or verify() are passed a unicode string, it will be encoded
                using this encoding.

            -> (#str) argon2 hash
        """
        a2 = PasswordHasher(
            time_cost=(time_cost or self.rounds), memory_cost=memory_cost,
            parallelism=(parallelism or cpu_count() * 2), hash_len=hash_len,
            salt_len=(salt_len or self.salt_size), encoding=encoding)
        return a2.hash(value)

    def encrypt(self, value, scheme=None):
        """ Hashes @value with @scheme or the default scheme """
        scheme = scheme or self.scheme
        if scheme == 'argon2':
            return self.argon2_encrypt(value)
        pwd_context = get_pwd_context(
            self.schemes, self.rounds, self.salt_size)
        digest = pwd_context.encrypt(value, scheme=scheme)
        if scheme != 'bcrypt':
            return digest
        else:
            return "$bcrypt{}".format(digest)

    @staticmethod
    def generate(size=256):
        """ Generates a secure and random password of @size in bits of entropy
        """
        return AuthKey.generate(size=size)
    gen = generate

    @property
    def requirements(self):
        return "Password must be at least {} characters long.".format(
            self.minlen)

    @property
    def mask(self):
        """ Returns a 'masked' version of the password in
            :prop:validation_value
        """
        if self.validation_value:
            return "".join("x" for x in range(len(self.validation_value)))

    def argon2_verify(self, value, hash=None, time_cost=0, memory_cost=1024,
                      parallelism=0, hash_len=45, salt_len=0,
                      encoding='utf-8'):
        """ Verifies @value against @hash using the argon2 algorithm.

            @value: value to hash
            @time_cost: (#int) Defines the amount of computation realized and
                therefore the execution time, given in number of iterations.
            @memory_cost: (#int) Defines the memory usage, given in kibibytes.
            @parallelism: (#int) Defines the number of parallel threads
                (changes the resulting hash value).
            @hash_len: (#int) Length of the hash in bytes.
            @salt_len: (#int) Length of random salt to be generated for each
                password.
            @encoding: (#str) The Argon2 C library expects bytes. So if hash()
                or verify() are passed a unicode string, it will be encoded
                using this encoding.

            -> (#bool) True if the given @value is correct
        """
        hash = hash or self.value
        if not hash or not value:
            return False
        pwd_context = PasswordHasher(
            time_cost=(time_cost or self.rounds), memory_cost=memory_cost,
            parallelism=(parallelism or cpu_count() * 2), hash_len=hash_len,
            salt_len=(salt_len or self.salt_size), encoding=encoding)
        try:
            return pwd_context.verify(hash, value)
        except argon2.exceptions.VerificationError as e:
            return False

    def verify(self, value, hash=None):
        """ Verifies that @value matches the password hash
            -> #bool True if the given @value is correct
        """
        hash = hash or self.value
        if not hash or not value:
            return False
        scheme = hash.split("$")[1]
        if 'argon2' in scheme:
            return self.argon2_verify(value, hash)
        pwd_context = get_pwd_context(
            self.schemes, self.rounds, self.salt_size)
        try:
            return pwd_context.verify(value, hash.replace("$bcrypt$", "$"))
        except ValueError:
            return False

    def validate(self):
        """ Validates the field
            -> #bool True if the :prop:validation_value is valid
        """
        universal_validation = self._validate()
        if not universal_validation:
            return False
        if self.strict and self.validation_value in passwords.blacklist:
            self.validation_error = "The password entered is blacklisted ({})"\
                .format(repr(self.validation_value))
            return False
        return self.is_hash(self.value)

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minlen = self.minlen
        cls.maxlen = self.maxlen
        cls.scheme = self.scheme
        cls.schemes = copy.copy(self.schemes)
        cls.salt_size = self.salt_size
        cls.strict = self.strict
        cls.validation_value = self.validation_value
        return cls

    __copy__ = copy


class Key(Field, StringLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for PostgreSQL CHAR/TEXT types.

        Generates random keys of @size bits of entropy.

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            akey = Key()
            akey.generate(256)
        ..
        |v+mwqKTshyGNcoChT1HFHnSr.umyotPhAXMZ/pxEJWrFH|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'size', 'keyspace',
        '_genargs', 'table', 'rng')
    sqltype = KEY

    def __init__(self, value=Field.empty, size=256,
                 keyspace=string.ascii_letters+string.digits+'/.#+',
                 rng=None, **kwargs):
        """ `Key`
            :see::meth:Field.__init__
            @size: (#int) size in bits to generate
            @keyspace: (#str) iterable chars to include in the key
            @rng: the random number generator implementation to use.
                :class:random.SystemRandom by default
        """
        super().__init__(value=value, **kwargs)
        self.size = size
        self.keyspace = keyspace
        self.rng = rng
        self._genargs = (self.size, self.keyspace, self.rng)

    @prepr('size', 'name', 'value', _no_keys=True)
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(str(value) if value is not None else value)
        return self.value

    @classmethod
    def generate(self, size=256,
                 keyspace=string.ascii_letters+string.digits+'/.#+',
                 rng=None):
        """ Generates a high-entropy random key. First, a @size pseudo-random
            bit integer is generated using /dev/urandom, which is transformed
            into a string of the same approximate bit length using
            :class:vital.security.strkey

            @size: (#int) size in bits to generate
            @keyspace: (#str) iterable chars to include in the key
            @rng: the random number generator implementation to use.
                :class:random.SystemRandom by default
            -> #str high-entropy key
        """
        keyspace = keyspace if keyspace or not hasattr(self, 'keyspace') else \
            self.keyspace
        size = size if size or not hasattr(self, 'size') else self.size
        return randkey(size, keyspace, rng=rng)

    def new(self, *args, **kwargs):
        """ Creates a new key and sets the value of the field to the key """
        if not args and not kwargs:
            self._set_value(self.generate(*self._genargs))
        else:
            self._set_value(self.generate(*args, **kwargs))

    def copy(self, *args, **kwargs):
        cls = self.__class__(*args, **kwargs)
        cls.field_name = self.field_name
        cls.primary = self.primary
        cls.unique = self.unique
        cls.index = self.index
        cls.notNull = self.notNull
        if self.value is not None and self.value is not Field.empty:
            cls.value = copy.copy(self.value)
        cls.validation = self.validation
        cls.validation_error = self.validation_error
        cls._alias = self._alias
        cls.table = self.table
        cls.size = self.size
        cls.keyspace = self.keyspace
        cls.rng = self.rng
        cls._genargs = copy.copy(self._genargs)
        return cls

    __copy__ = copy


AuthKey = Key


class Email(Text):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for PostgreSQL CHAR/TEXT types.

        Validates that a given value looks like an email address before
        inserting it into the DB.

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            email = Email("not_a@real_email")
            email.validate_chars()
        ..
        |False|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', 'table')
    sqltype = EMAIL

    def __init__(self, value=Field.empty, minlen=6, maxlen=320, **kwargs):
        """ `Email`
            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
        """
        super().__init__(value=value, minlen=minlen, maxlen=maxlen, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(str(value).lower() if value is not None else value)
        return self.value

    def validate(self):
        universal_validation = self._validate()
        if not universal_validation:
            return False
        if (self.value or self.notNull) and not self.validate_chars():
            self.validation_error = "{} is not a valid email address".format(
                repr(self.value))
            return False
        return True

    def validate_chars(self):
        return string_tools.is_email(self.value)


class Username(Text):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for PostgreSQL CHAR/TEXT types.

        Validates that a given value looks like a username and is not
        in :attr:bloom.etc.usernames.reserved_usernames before inserting
        it into the DB.

        - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        ``Usage Example``
        ..
            username = Username("admin")
            username.validate()
        ..
        |False|

        ..
            print(username.validation_error)
        ..
        |'This is a reserved username.'|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', '_re', 'reserved_usernames', 'table')
    sqltype = USERNAME

    def __init__(self, value=Field.empty, minlen=1, maxlen=25,
                 reserved_usernames=None, re_pattern=None, **kwargs):
        """ `Username`
            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
            @reserved_usernames: (#list) of usernames to prevent from being
                created
            @re_pattern: (sre_compile) compiled regex pattern to validate
                usernames with. Defautls to :var:string_tools.username_re
        """
        super().__init__(value=value, minlen=minlen, maxlen=maxlen, **kwargs)
        self._re = re_pattern or string_tools.username_re
        if reserved_usernames is not None:
            self.reserved_usernames = set(u.lower()
                                          for u in reserved_usernames)
        else:
            self.reserved_usernames = set(usernames.reserved_usernames)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(str(value) if value is not None else value)
        return self.value

    def add_reserved_username(self, *usernames):
        for username in usernames:
            self.reserved_usernames.add(username.lower())

    def validate(self):
        if self.value and \
           self.value.strip().lower() in self.reserved_usernames:
            self.validation_error = "This is a reserved username."
            return False
        universal_validation = self._validate()
        if not universal_validation:
            return False
        if (self.value or self.notNull) and not self.validate_chars():
            self.validation_error = "{} is not a valid username".format(
                repr(self.value))
            return False
        return True

    def validate_chars(self):
        return self._re.match(self.value)

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minlen = self.minlen
        cls.maxlen = self.maxlen
        cls._re = self._re
        cls.reserved_usernames = self.reserved_usernames.copy()
        return cls

    __copy__ = copy

    def __deepcopy__(self, memo):
        cls = self.__class__()
        for k in list(self.__slots__) + list(dir(self)):
            attr = getattr(self, k)
            if k not in {'copy', '_re'} and not k.startswith('__')\
               and not callable(attr):
                try:
                    setattr(cls, k, copy.deepcopy(getattr(self, k)))
                except AttributeError:
                    pass
                except TypeError:
                    setattr(cls, k, copy.copy(getattr(self, k)))
            elif k == '_re':
                setattr(cls, k, getattr(self, k))
        return cls


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
    """ An encryption factory which uses the AES-256 algorithm. """
    __slots__ = tuple()

    @staticmethod
    def decrypt(val, secret):
        return aes_b64_decrypt(val, secret)

    @staticmethod
    def encrypt(val, secret):
        return aes_b64_encrypt(val, secret)


class AESBytesFactory(EncryptionFactory):
    """ An encryption factory which uses the AES-256 algorithm. """
    __slots__ = tuple()

    @staticmethod
    def decrypt(val, secret):
        return aes_decrypt(val, secret)

    @staticmethod
    def encrypt(val, secret):
        return aes_encrypt(val, secret)


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

        ..
            import os
            from bloom.fields import *

            permissions = Encrypted(os.environ.get(
                                       'BLOOM_ENCRYPTION_SECRET',
                                        Encrypted.generate_secret(256)),
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
    _prefix = '$cipher$'
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'default', '_alias', 'table', 'type', '_secret', 'factory')

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
        if (type.sqltype == BINARY and factory == AESFactory) or \
           (type.sqltype == ARRAY and type.type.sqltype == BINARY):
            factory = AESBytesFactory
        self.factory = factory
        self.type = type.copy()
        self._in_type = type.copy()
        if hasattr(self.type, 'cast'):
            self._in_type.cast = str
        self.type.validation = validation or self.type.validation
        self.field_name = None
        self.primary = kwargs.get('primary')
        self.unique = kwargs.get('unique')
        self.index = kwargs.get('index')
        self.notNull = kwargs.get('not_null')
        self.table = None
        try:
            self.default = kwargs.get('default')
        except AttributeError:
            """ Ignore error for classes where default is a property not an
                attribute """
            pass
        self.value = Field.empty
        self._alias = None
        self.__call__(value)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(self.type.__call__(self.decrypt(value)))
        return self.value

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError:
            return self.type.__getattribute__(name)

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
        unlabel = self._unlabel
        decrypt = self.decrypt
        for x, v in enumerate(val.copy()):
            val[x] = decrypt(v)
        return self.type.__call__(val)

    def _decrypt_dict(self, val):
        unlabel = self._unlabel
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
            -> (#str) the encrypted :prop:value with :prop:_prefix
        """
        return self.encrypt(self.value)

    def _label(self, val):
        """ Adds |__bloomcrypt__:| prefix to @val """
        if val is not None and not self._labeled(val):
            prefix = self._prefix
            if isinstance(val, bytes):
                prefix = prefix.encode()
            return prefix + val
        else:
            return val

    def _unlabel(self, val):
        """ Removes |__bloomcrypt__:| prefix from @val """
        if val is not None and self._labeled(val):
            prefix = self._prefix
            if isinstance(val, bytes):
                prefix = prefix.encode()
                return val.replace(prefix, b'', 1)
            return val.replace(prefix, '', 1)
        else:
            return val

    def _labeled(self, val):
        """ -> (#bool) |True| if @val is labeled with :prop:_prefix  """
        try:
            prefix = self._prefix
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

    def _cast_real(self, val):
        inherit = {JSONTYPE, JSONB, BINARY}
        if self.type.sqltype in inherit or \
           (self.type.sqltype == ARRAY and self.type.type.sqltype in inherit):
            self._in_type(val)
            return self._in_type.real_value
        return val

    @property
    def real_value(self):
        if self.value is not self.empty and self.value is not None:
            return self._cast_real(self.encrypted)
        elif self.default is not None:
            val = self.encrypt(self.default)
            return self._cast_real(val)
        else:
            return self.default

    def validate(self):
        self.type(self.value)
        validate = self.type.validate()
        return validate

    def copy(self, *args, **kwargs):
        cls = self.__class__(self._secret, *args, type=self.type, **kwargs)
        cls.field_name = self.field_name
        cls.primary = self.primary
        cls.unique = self.unique
        cls.index = self.index
        cls.notNull = self.notNull
        if self.value is not None and self.value is not self.empty:
            cls.value = copy.copy(self.value)
        cls.default = self.default
        cls.table = self.table
        cls._alias = self._alias
        return cls

    __copy__ = copy
