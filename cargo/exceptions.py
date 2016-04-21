"""

  `Exceptions Raised by Cargo SQL`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import psycopg2 as _psycopg2
from collections import namedtuple as _namedtuple


_ErrorCodes = _namedtuple('_ErrorCodes', 'EXECUTE COMMIT')
ERROR_CODES = _ErrorCodes(EXECUTE=10001, COMMIT=10002)


#: Catchable exceptions raised by pscyopg2
Psycopg2QueryErrors= (_psycopg2.ProgrammingError,
                      _psycopg2.IntegrityError,
                      _psycopg2.DataError,
                      _psycopg2.InternalError)


class QueryError(Exception):
    """ Raised when there was an error executing a :class:cargo.Query """
    def __init__(self, message, code=None, root=None):
        self.message = message
        self.code = code
        self.root = root


class BuildError(Exception):
    """ Raised when tables fail to build with :class:cargo.builder.Build """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class ORMIndexError(Exception):
    """ Raised when there was an error saving a record """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class RelationshipImportError(Exception):
    """ Raised when a relationship could not be forged """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class PullError(Exception):
    """ Raised when a relationship could not be pulled """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class TranslationError(Exception):
    """ Raised when a native sql type could not be translated automatically
        to a vital sql :class:Field type
    """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class SchemaError(Exception):
    """ Raised when errors related to the database schema happen. """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class FieldError(Exception):
    """ Raised when errors related to :class:Field(s) happen. """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class ValidationError(Exception):
    """ Raised when there was an error validating one of your
        :class:cargo.Field objects with
        :class:cargo.validators.Validate
    """
    def __init__(self, message, field=None, code=None):
        self.message = message
        self.code = code
        self.field = field


class ValidationValueError(ValidationError):
    pass


class ValidationTypeError(ValidationError):
    pass


class IncorrectPasswordError(ValueError):
    """ Raised when a password given to the :class:Password field is incorrect
    """
    def __init__(self, message, code=None):
        self.message = message
        self.code = code
