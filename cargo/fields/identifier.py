"""

  `Cargo SQL Identifier Fields`
   By default, all of these fields are primary keys.
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import uuid
import string

import psycopg2
from psycopg2.extensions import adapt, register_adapter, new_type,\
                                register_type, AsIs

from vital.security import strkey

from cargo.etc.types import *
from cargo.expressions import *

from cargo.fields.field import Field
from cargo.fields.integer import SmallInt


__all__ = ('UUID', 'UID', 'SmallSerial', 'Serial', 'BigSerial', 'StrUID')


class UUID(Field, StringLogic):
    """ ======================================================================
        Field object for the PostgreSQL field type |UUID|
    """
    __slots__ = Field.__slots__
    OID = UUIDTYPE

    def __init__(self, *args, default=Field.empty, primary=True, **kwargs):
        """`UUID`
            ==================================================================
            :see::meth:Field.__init__
        """
        default = default if default is not Field.empty else \
            Function('uuid_generate_v4')
        super().__init__(*args, default=default, primary=primary, **kwargs)

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

    @property
    def type_name(self):
        return 'uuid'

    @staticmethod
    def to_python(uuid_, cur):
        try:
            return uuid.UUID(uuid_)
        except TypeError:
            return uuid_

    @staticmethod
    def register_adapter():
        register_adapter(uuid.UUID, UUID.adapt)
        UUIDTYPE_ = reg_type('UUIDTYPE', UUIDTYPE, UUID.to_python)
        UUIDARRAYTYPE = reg_array_type('UUIDARRAYTYPE', UUIDARRAY, UUIDTYPE_)

    @staticmethod
    def register_type(db):
        try:
            return db.register('uuid')
        except (ValueError, psycopg2.ProgrammingError):
            warnings.warn('Type `uuid` was not found in the database.')

    @staticmethod
    def adapt(uuid):
        return AsIs("'%s'::uuid" % str(uuid))


class SmallSerial(SmallInt):
    """ ======================================================================
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
    __slots__ = SmallInt.__slots__
    OID = SMALLSERIAL

    def __init__(self, minval=1, maxval=32767, *args, primary=True, **kwargs):
        """`SmallSerial`
            ==================================================================
            @maxval: (#int) maximum integer value
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(minval, maxval, *args, primary=primary, **kwargs)


class Serial(SmallSerial):
    """ ======================================================================
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
    __slots__ = SmallInt.__slots__
    OID = SERIAL

    def __init__(self, minval=1, maxval=2147483647, *args,
                 primary=True, **kwargs):
        """ `Serial`
            ==================================================================
            @maxval: (#int) maximum integer value
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(minval, maxval, *args, primary=primary, **kwargs)


class BigSerial(Serial):
    """ ======================================================================
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
    __slots__ = SmallInt.__slots__
    OID = BIGSERIAL

    def __init__(self, minval=1, maxval=9223372036854775807, *args,
                 primary=True, **kwargs):
        """ `BigSerial`
            ==================================================================
            @maxval: (#int) maximum integer value
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(minval, maxval, *args, primary=primary, **kwargs)


class UID(Field, NumericLogic):
    """ ======================================================================
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
    __slots__ = Field.__slots__
    OID = UIDTYPE

    def __init__(self, *args, primary=True, default=Field.empty, **kwargs):
        """ `UID`
            ==================================================================
            :see::meth:Field.__init__
        """
        default = default if default is not Field.empty else \
            Function('cargo_uid')
        super().__init__(*args, primary=primary, default=default, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self.value = int(value) if value is not None else None
        return self.value

    def __int__(self):
        return int(self.value)

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return str(self.value)
        return None

_ascii_letters = ''.join(letter
                         for letter in string.ascii_letters
                         if letter not in {'I', 'l'})


class strint(int):
    def __new__(cls, value):
        return int.__new__(cls, value)

    def __iadd__(self, other):
        return self.__class__(self.__add__(other))

    def __isub__(self, other):
        return self.__class__(self.__sub__(other))

    def __imul__(self, other):
        return self.__class__(self.__mul__(other))

    def __ipow__(self, other):
        return self.__class__(self.__pow__(other))

    def __ixor__(self, other):
        return self.__class__(self.__xor__(other))

    def __ior__(self, other):
        return self.__class__(self.__or__(other))

    def __imatmul__(self, other):
        return self.__class__(self.__matmul__(other))

    def __ilshift__(self, other):
        return self.__class__(self.__lshift__(other))

    def __irshift__(self, other):
        return self.__class__(self.__rshift__(other))

    def __imod__(self, other):
        return self.__class__(self.__mod__(other))

    def __ifloordiv__(self, other):
        return self.__class__(self.__floordiv__(other))

    def __itruediv__(self, other):
        return self.__class__(self.__truediv__(other))

    def __iconcat__(self, other):
        return self.__class__(self.__concat__(other))

    def __iand__(self, other):
        return self.__class__(self.__and__(other))

    def __str__(self):
        return self.to_str()

    def to_str(self, value=None):
        return strkey(value or self,
                      chaffify=1,
                      keyspace=_ascii_letters)

    @staticmethod
    def from_str(value):
        if isinstance(value, str) and not value.isdigit():
            return strint(strkey(value or self,
                                 chaffify=1,
                                 keyspace=_ascii_letters))
        return strint(value)

    @staticmethod
    def to_db(val):
        return adapt(int(val))


class StrUID(UID):
    """ ======================================================================
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
    __slots__ = Field.__slots__
    OID = STRUID

    def __init__(self, *args, **kwargs):
        """ `StrUID`
            ==================================================================
            :see::meth:Field.__init__
        """
        super().__init__(*args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value is not None:
                if str(value).isdigit():
                    value = strint(value)
                else:
                    value = strint.from_str(value)
            self.value = value
        return str(self.value)

    def __str__(self):
        return str(self.value)

    def __len__(self):
        if self.value is not None:
            return len(self.value)
        return 0

    def for_json(self):
        """:see::meth:Field.for_json"""
        if self.value_is_not_null:
            return self.__str__()
        return None

    @staticmethod
    def register_adapter():
        register_adapter(strint, strint.to_db)
