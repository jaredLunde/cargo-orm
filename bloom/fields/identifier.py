#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Identifier Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import string

from vital.security import strkey

from bloom.etc.types import *
from bloom.expressions import *

from bloom.fields.field import Field
from bloom.fields.integer import SmallInt


__all__ = ('UUID', 'UID', 'SmallSerial', 'Serial', 'BigSerial', 'StrUID')


class UUID(Field, StringLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |UUID|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'default', 'validation', 'validation_error', '_alias', 'table')
    sqltype = UUIDTYPE

    def __init__(self, value=Field.empty, **kwargs):
        """ `UUID`
            :see::meth:Field.__init__
        """
        super().__init__(value=value, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(value)
        return self.value


class SmallSerial(SmallInt):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT2| with an
        |AUTO_INCREMENT|-like interface. It is always assumed that this field
        is the |PRIMARY| key.

        The data type bigserial is not a true type, but merely a notational
        convenience for setting up unique identifier columns
        (similar to the |AUTO_INCREMENT| property supported by some other
        databases).

        Its value will not be known until the model record is inserted into
        the DB.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = SMALLSERIAL

    def __init__(self, value=Field.empty, minval=1, maxval=32767,
                 primary=True, **kwargs):
        """ `SmallSerial`
            :see::meth:Field.__init__
            @maxval: (#int) maximum integer value
        """
        super().__init__(
            value=value, minval=minval, maxval=maxval,
            primary=primary, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(int(value) if value is not None else None)
        return self.value


class Serial(SmallSerial):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT4| with an
        |AUTO_INCREMENT|-like interface. It is always assumed that this field
        is the |PRIMARY| key.

        The data type serial is not a true tpye, but merely a notational
        convenience for setting up unique identifier columns
        (similar to the |AUTO_INCREMENT| property supported by some other
        databases).

        Its value will not be known until the model record is inserted into
        the DB. You must set the default value of this field to a sequence in
        your table.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = SERIAL

    def __init__(self, value=Field.empty, minval=1, maxval=2147483647,
                 primary=True, **kwargs):
        """ `Serial`
            :see::meth:Field.__init__
            @maxval: (#int) maximum integer value
        """
        super().__init__(
            value=value, minval=minval, maxval=maxval, primary=primary,
            **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(int(value) if value is not None else None)
        return self.value


class BigSerial(Serial):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT8| with an
        |AUTO_INCREMENT|-like interface. It is always assumed that this field
        is the |PRIMARY| key.

        The data type bigserial is not a true type, but merely a notational
        convenience for setting up unique identifier columns
        (similar to the |AUTO_INCREMENT| property supported by some other
        databases).

        Its value will not be known until the model record is inserted into
        the DB.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = BIGSERIAL

    def __init__(self, value=Field.empty, minval=1, maxval=9223372036854775807,
                 primary=True, **kwargs):
        """ `BigSerial`
            :see::meth:Field.__init__
            @maxval: (#int) maximum integer value
        """
        super().__init__(
            value=value, minval=minval, maxval=maxval,
            primary=primary, **kwargs)


class UID(BigSerial):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT8|. It is always
        assumed that this field is the |PRIMARY| key.

        The data type UID is not a true type, but merely a notational
        convenience for setting up global unique identifier columns.

        Its value will not be known until the model record is inserted into
        the DB. You must set the default value of this field to a sequence
        in your table.

        It is used with a PostgreSQL function such as an |id_generator()|::
        ..
        create schema shard_1;
        create sequence shard_1.global_id_sequence;CREATE OR REPLACE FUNCTION
            shard_1.id_generator(OUT result bigint) AS $$;
        DECLARE
            our_epoch bigint := 1314220021721;
            seq_id bigint;
            now_millis bigint;
            -- the id of this DB shard, must be set for each
            -- schema shard you have - you could pass this as a parameter too
            shard_id int := 1;
        BEGIN
            SELECT nextval('shard_1.global_id_sequence') % 1024 INTO seq_id;

        SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO
          now_millis;
            result := (now_millis - our_epoch) << 23;
            result := result | (shard_id << 10);
            result := result | (seq_id);
        END;
        $$ LANGUAGE PLPGSQL;
        ..

        Where the default value for the field would be |shard_1.id_generator()|

        See [here for more information](
          http://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/)
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = UIDTYPE

    def __init__(self, value=Field.empty, minval=1, maxval=9223372036854775807,
                 primary=True, **kwargs):
        """ `UID`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
        """
        super().__init__(
            value=value, minval=minval, maxval=maxval,
            primary=primary, **kwargs)


class StrUID(UID):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INT8|. It is always
        assumed that this field is the |PRIMARY| key.

        The data type StrUID is not a true type, but merely a notational
        convenience for setting up global unique identifier columns.

        Its value will not be known until the model record is inserted into
        the DB.

        StrUID differs from :class:UID because aside from its place in the DB,
        the field value appears an #str representation of the #int ID.
        ..
            struid = StrUID(1234)
            print(struid)
        ..
        |'d00'|

        ..
            print(struid.value)
        ..
        |1234|

        ..
            struid = StrUID('d00')
            print(struid.value)
        ..
        |1234|

        ..
            print("/post/{}".format(struid))
        ..
        |'/post/d00'|

        ======================================================================
        ``It is used with a PostgreSQL function such as an |id_generator()|``
        ..
            create schema shard_1;
            create sequence shard_1.global_id_sequence;CREATE OR REPLACE
                FUNCTION shard_1.id_generator(OUT result bigint) AS $$;
            DECLARE
                our_epoch bigint := 1314220021721;
                seq_id bigint;
                now_millis bigint;
                -- the id of this DB shard, must be set for each
                -- schema shard you have - you could pass this as a parameter
                -- too
                shard_id int := 1;
            BEGIN
                SELECT nextval('shard_1.global_id_sequence') % 1024
                    INTO seq_id;

            SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO
              now_millis;
                result := (now_millis - our_epoch) << 23;
                result := result | (shard_id << 10);
                result := result | (seq_id);
            END;
            $$ LANGUAGE PLPGSQL;

            select shard_1.id_generator();
        ..

        Where the default value for the field would be
            |shard_1.id_generator();|

        See [here for more information](
          http://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/)
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'chaffify', 'table')
    sqltype = STRUID

    def __init__(self, value=Field.empty, minval=1, maxval=9223372036854775807,
                 chaffify=1024, primary=True, **kwargs):
        """ `StrUID`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
            @chaffify: (#int) multiple to avoid 1=b, 2=c, ... obfuscates
                the ordering.
                !! Do NOT change this number once it's been set.
                    The results will be unrecoverable without the chaffify #.
                !!
        """
        self.chaffify = chaffify
        super().__init__(
            value=value, minval=minval, maxval=maxval, primary=primary,
            **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(
                int(value) if str(value).isdigit() else
                self._strkey(value))
        return self.str_value

    def __str__(self):
        return self.str_value

    def __int__(self):
        return self.value

    def __len__(self):
        if self.value is not None:
            return len(self.str_value)
        return 0

    def _strkey(self, value):
        if value is None or value is Field.empty:
            return None
        return strkey(
            value, chaffify=self.chaffify, keyspace=string.ascii_letters)

    @property
    def str_value(self):
        return self._strkey(self.value)

    '''@property
    def real_value(self):
        return self.value'''

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        cls.chaffify = self.chaffify
        return cls

    __copy__ = copy
