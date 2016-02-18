#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Exceptions Raised by Vital SQL`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

"""


__all__ = (
  'QueryError',
  'SchemaError',
  'ORMIndexError',
  'RelationshipImportError',
  'PullError',
  'TranslationError',
  'ValidationError'
)


# TODO: Create error codes to pass to the exceptions


class QueryError(Exception):
    """ Raised when there was an error executing a :class:vital.sql.Query """
    def __init__(self, message):
        self.message = message


class ORMIndexError(Exception):
    """ Raised when there was an error saving a record """
    def __init__(self, message):
        self.message = message


class RelationshipImportError(Exception):
    """ Raised when a relationship could not be forged """
    def __init__(self, message):
        self.message = message


class PullError(Exception):
    """ Raised when a relationship could not be pulled """
    def __init__(self, message):
        self.message = message


class TranslationError(Exception):
    """ Raised when a native sql type could not be translated automatically
        to a vital sql :class:Field type
    """
    def __init__(self, message):
        self.message = message


class SchemaError(Exception):
    """ Raised when errors related to the database schema happen. """
    def __init__(self, message):
        self.message = message


class ValidationError(Exception):
    """ Raised when there was an error validating one of your
        :class:vital.sql.Field objects with
        :class:vital.sql.validators.Validate
    """
    def __init__(self, message, field=None):
        self.message = message
        self.field = field
