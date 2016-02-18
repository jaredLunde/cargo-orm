#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital SQL Special Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

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
from vital.security import randkey
from vital.tools import strings as string_tools

from vital.sql.etc import passwords, usernames
from vital.sql.etc.types import *
from vital.sql.expressions import *

from vital.sql.fields.field import Field
from vital.sql.fields.character import Char

# TODO: Currency field

__all__ = ('Username', 'Email', 'Password', 'Slug', 'SlugFactory',
           'Key', 'AuthKey')


class SlugFactory(object):
    __slots__ = ('slugify_opt',)

    def __init__(self, **slugify_opt):
        """ :see::func:slugify """
        self.slugify_opt = slugify_opt

    def __call__(self, value):
        return slugify(value, **self.slugify_opt)


class Slug(Char):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |TEXT|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minlen',
        'maxlen', '_factory', 'table')
    sqltype = SLUG

    def __init__(self, value=None, minlen=0, maxlen=-1, slug_factory=None,
                 **kwargs):
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

    def __init__(self, value=None, minlen=8, maxlen=-1, scheme="argon2",
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
            @default_scheme: (#str) default scheme to use in
                :class:CryptContext
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

    @prepr('name', 'scheme', 'rounds', 'minlen', 'maxlen', 'mask')
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
        'validation', 'validation_error', '_alias', 'size', 'chars',
        '_genargs', 'table', 'rng')
    sqltype = KEY

    def __init__(self, value=None, size=256,
                 chars=string.ascii_letters+string.digits+'/.#+',
                 rng=None, **kwargs):
        """ `Key`
            :see::meth:Field.__init__
            @size: (#int) size in bits to generate
            @chars: (#str) iterable chars to include in the key
            @rng: the random number generator implementation to use.
                :class:random.SystemRandom by default
        """
        super().__init__(value=value, **kwargs)
        self.size = size
        self.chars = chars
        self.rng = rng
        self._genargs = (self.size, self.chars, self.rng)

    @prepr('name', ('size', 'purple'), 'value')
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(str(value) if value is not None else value)
        return self.value

    @property
    def default(self):
        args = getattr(self, '_genargs') if hasattr(self, '_genargs') else []
        return self.generate(*args)

    @classmethod
    def generate(self, size=256,
                 chars=string.ascii_letters+string.digits+'/.#+',
                 rng=None):
        """ Generates a high-entropy random key. First, a @size pseudo-random
            bit integer is generated using /dev/urandom, which is transformed
            into a string of the same approximate bit length using
            :class:vital.security.strkey

            @size: (#int) size in bits to generate
            @chars: (#str) iterable chars to include in the key
            @rng: the random number generator implementation to use.
                :class:random.SystemRandom by default
            -> #str high-entropy key
        """
        chars = chars if chars or not hasattr(self, 'chars') else self.chars
        size = size if size or not hasattr(self, 'size') else self.size
        return randkey(size, chars, rng=rng)

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
        cls.chars = self.chars
        cls.rng = self.rng
        cls._genargs = copy.copy(self._genargs)
        return cls

    __copy__ = copy


AuthKey = Key


class Email(Char):
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

    def __init__(self, value=None, minlen=6, maxlen=320, **kwargs):
        """ `Email`
            :see::meth:Field.__init__
            @minlen: (#int) minimum length of string value
            @maxlen: (#int) minimum length of string value
        """
        super().__init__(value=value, minlen=minlen, maxlen=maxlen, **kwargs)

    @prepr('name', 'value')
    def __repr__(self): return

    def __str__(self):
        return str(self.value)

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


class Username(Char):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for PostgreSQL CHAR/TEXT types.

        Validates that a given value looks like a username and is not
        in :attr:vital.sql.etc.usernames.reserved_usernames before inserting
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

    def __init__(self, value=None, minlen=1, maxlen=25,
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

    @prepr('name', 'value')
    def __repr__(self): return

    def __str__(self):
        return str(self.value)

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
