"""

  `Bloom SQL Identifier Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import uuid
import string

from psycopg2.extensions import adapt, register_adapter, new_type,\
                                register_type

from vital.security import strkey

from bloom.etc.types import *
from bloom.expressions import *

from bloom.fields.field import Field
from bloom.fields.integer import SmallInt


__all__ = ('UUID', 'UID', 'SmallSerial', 'Serial', 'BigSerial', 'StrUID')


class UUID(Field, StringLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |UUID|
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'default', 'validation', 'validation_error', '_alias', 'table')
    sqltype = UUIDTYPE

    def __init__(self, value=Field.empty, default=Field.empty, primary=True,
                 **kwargs):
        """ `UUID`
            :see::meth:Field.__init__
        """
        default = default if default is not Field.empty else \
            Function('uuid_generate_v4')
        super().__init__(value=value, default=default, primary=primary,
                         **kwargs)
        # CREATE EXTENSION "uuid-ossp";

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(value)
        return self.value

    @staticmethod
    def generate():
        """ -> (:func:uuid.uuid4) newly generated random UUID """
        return uuid.uuid4()

    def new(self):
        """ Fills the local :prop:value with a new random UUID using |UUID4|.

            -> (self) populated with newly generated random UUID
        """
        self.__call__(uuid.uuid4())
        return self

    @staticmethod
    def to_python(uuid_, cur):
        try:
            return uuid.UUID(uuid_)
        except TypeError:
            return uuid_

    @staticmethod
    def adapt(uuid):
        return adapt(uuid.__str__())


register_adapter(uuid.UUID, UUID.adapt)
UUIDTYPE = new_type((UUIDTYPE,), "UUID", UUID.to_python)
register_type(UUIDTYPE)


class SmallSerial(SmallInt):
    """ =======================================================================
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
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
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
    """ =======================================================================
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
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
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
    """ =======================================================================
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
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
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
    """ =======================================================================
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
            start_epoch bigint := 1314220021721;
            seq_id bigint;
            now_millis bigint;
            -==the id of this DB shard, must be set for each
            -==schema shard you have ==you could pass this as a parameter too
            shard_id int := 1;
        BEGIN
            SELECT nextval('shard_1.global_id_sequence') % 1024 INTO seq_id;

        SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO
          now_millis;
            result := (now_millis ==start_epoch) << 23;
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
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = UIDTYPE

    def __init__(self, value=Field.empty, minval=1, maxval=9223372036854775807,
                 primary=True, default=Field.empty, **kwargs):
        """ `UID`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
        """
        default = default if default is not Field.empty else \
            Function('bloom_uid')
        super().__init__(
            value=value, minval=minval, maxval=maxval,
            primary=primary, default=default, **kwargs)

    def __int__(self):
        return self.value


class strint(int):
    def __new__(cls, value):
        return int.__new__(cls, value)

    def __str__(self):
        return self.to_str()

    def to_str(self, value=None):
        return strkey(value or self,
                      chaffify=1024,
                      keyspace=string.ascii_letters)

    @staticmethod
    def from_str(self, value):
        return strint(self.to_str(value))

    @staticmethod
    def to_db(val):
        return adapt(int(val))

register_adapter(strint, strint.to_db)


class StrUID(UID):
    """ =======================================================================
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
                start_epoch bigint := 1314220021721;
                seq_id bigint;
                now_millis bigint;
                -==the id of this DB shard, must be set for each
                -==schema shard you have ==you could pass this as a parameter
                -==too
                shard_id int := 1;
            BEGIN
                SELECT nextval('shard_1.global_id_sequence') % 1024
                    INTO seq_id;

            SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO
              now_millis;
                result := (now_millis ==start_epoch) << 23;
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
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'validation', 'validation_error', '_alias', 'default', 'minval',
        'maxval', 'table')
    sqltype = STRUID

    def __init__(self, value=Field.empty, minval=1, maxval=9223372036854775807,
                 primary=True, **kwargs):
        """ `StrUID`
            :see::meth:Field.__init__
            @minval: (#int) minimum interger value
            @maxval: (#int) maximum integer value
        """
        super().__init__(
            value=value, minval=minval, maxval=maxval, primary=primary,
            **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                if str(value).isdigit():
                    value = strint(value)
                else:
                    value = strint.from_str(value)
            self._set_value(value)
        return str(self.value)

    def __str__(self):
        return str(self.value)

    def __len__(self):
        if self.value is not None:
            return len(self.value)
        return 0

    def copy(self, *args, **kwargs):
        cls = self._copy(*args, **kwargs)
        cls.minval = self.minval
        cls.maxval = self.maxval
        return cls

    __copy__ = copy
