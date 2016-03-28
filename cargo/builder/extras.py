"""

  `Cargo SQL Extra Type Builders`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import re
import time

import psycopg2

from vital.debug import logg

from cargo import Clause, Raw, safe
from cargo.etc import types
from cargo.exceptions import *
from cargo.builder.extensions import *
from cargo.builder.functions import *
from cargo.builder.create_shortcuts import create_sequence, create_schema
from cargo.builder.utils import *


__all__ = (
    'UIDFunction',
    'UUIDExtension',
    'HStoreExtension',
    'CITextExtension')


_uid_tpl = """
$$
DECLARE
    -- !! custom epoch, this will work for 41 years after the epoch
    start_epoch bigint := {epoch};
    -- Declares big integer variables we will use to calculate the uuid
    seq_id bigint;
    now_millis bigint;
    -- The shard ID, must be unique for each shard
    shard_id integer := {shard_id};
BEGIN
    -- Selects the next value of the sequence we created earlier
    -- and performs a modulus operator on it so that it fits in to
    -- 10 bits (this is where the 1024 comes from).
    SELECT mod(nextval('{schema}.{sequence_name}'), 1024) INTO seq_id;

SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
    -- left shifts 41 bits (64 - 41)
    result := (now_millis - start_epoch) << 23;
    -- left shifts 13 bits (64 - 41 - 13)
    result := result | (shard_id << 10);
    -- fills remaining 10 bits (from the modulus operation above)
    result := result | (seq_id);
END;
$$
"""


class UIDFunction(Function):
    """ ========================================================================
        Creates a 64-bit universally unique id. This is accomplished by
        calculating a 41-bit representation of the milliseconds from a given
        start epoch, using a 13-bit representation of the ID number of the
        shard, and a 10-bit representation of a global ID sequence for a given
        schema.
        ------------------------------------------------------------------------
        The UID can be sorted by its insertion order, that is, rows submitted
        sequentially will remain sequential in terms of their UID.
        ------------------------------------------------------------------------
        64-bit UID is delegated like so:
        * 41-bit time in milliseconds since epoch (gives 41 years of IDs)
        * 13-bit logical shard ID
        * 10-bit representation of an auto-incrementing sequence, modulus 1024.
            This means we can generate 1024 IDs, per shard, per millisecond.
        ========================================================================
        A modulus operation works like this:

        1. Say the 'nextval' is 5061 and the modulo is 1024
            |5061 % 1024|
        2. Divide 'nextval' by the modulo (1024 in this case)
            |5061 / 1024 = 4.942|
        3. Multiply the INTEGER value of step 2 by the modulo (1024)
            |INT 4.942 = 4|
            |4 * 1024 = 4096|
        4. Subtract step 3 from 'nextval' to get the difference,
            this is the result of the operation
            |5061 - 4096 = 965|

        |Result: 5061 % 1024 = 965|

        The maximum number in 10 bits is 1024.  Therefore, by taking the modulo
        of the sequence ids, we always get a number which is between 1 and
        1024. If we wanted the number to fit in an 8 bit representation we
        could set the modulo to 256.
        ========================================================================
        At scale, simply find/replace out {schema} for each shard you have.
        To use, set the |uid| field default value to |{schema}.cargo_uid()|
        where X is the shard ID. The |uid| field must be of type BIGINT (int8)
        in order to work properly as an integer field. It could still be placed
        in a text field, etc.

        Credit to:
        http://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/
    """
    extras_name = 'cargo_uid'

    def __init__(self, orm, shard_id=-1, name='cargo_uid', schema=None,
                 epoch=None, seq_cache=1024,  seq_name='cargo_uid_seq',
                 replace=False):
        """ `UID Function`
            @schema: (#str)
            @shard_id: (#int) logical shard id number, if there isn't one
                provided, it will first try to be guessed by finding digits
                at the end of the schema name, and if no digits are found
                the default will be shard id will be 0.
            @name: (#str) name of the function
            @epoch: (#int) millisecond time epoch. This UID function will work
                for 41 years its epoch. The default value is the current epoch
                in milliseconds.
            @seq_name: (#str) name of the sequence to create for this function.
                See also :class:cargo.builder.create_sequence
            @seq_cache: (#int) size of the sequence cache
        """
        self._epoch = epoch or int(time.time() * 1000)
        self.__name = name
        self._schema = schema or orm.schema or orm.db.schema or 'public'
        self._seq_cache = seq_cache
        self._seq_name = seq_name
        self._shard_id = shard_id
        super().__init__(orm,
                         function=(str(self.name) + '(OUT result bigint)'),
                         expression=self.get_expression(),
                         language='PLPGSQL',
                         replace=replace,
                         returns='bigint')

    @property
    def _name(self):
        return self._schema + '.' + self.__name

    _shard_re = re.compile(r"""[^\d]*(\d+)$""")

    @property
    def shard_id(self):
        if self._shard_id > -1:
            return self._shard_id
        m = self._shard_re.search(self._schema)
        if m:
            return int(m.group(1))
        else:
            return 0

    def set_shard_id(self, shard_id):
        self._shard_id = shard_id

    def epoch(self, epoch):
        """ @epoch: (#int) time epoch. This UID function will work for 41
            years its epoch. The default value is the current epoch
            seconds.
        """
        self._epoch = epoch

    def schema(self, schema):
        self._schema = schema
        self._function = self._cast_safe(self.name)

    def get_expression(self):
        return self._cast_safe(
            _uid_tpl.format(schema=self._schema,
                            epoch=int(self._epoch),
                            shard_id=int(self.shard_id),
                            sequence_name=self._seq_name))

    @property
    def query(self):
        self.orm.reset()
        #: Creates the schema if it does not exist
        schema = create_schema(self.orm, name=self._schema, not_exists=False,
                               dry=True)
        #: Creates a sequence
        seq_name = self._schema + '.' + self._seq_name
        # DROP SEQUENCE IF EXISTS seq_name;
        sequence = create_sequence(self.orm,
                                   name=seq_name,
                                   cache=self._seq_cache,
                                   dry=True)
        expression = self.get_expression()
        #: Creates the function
        cc = 'CREATE {}FUNCTION'.format('OR REPLACE ' if self._replace else "")
        self._add(Clause(cc, self._function),
                  self._returns,
                  Clause('AS', expression),
                  self._lang,
                  self._options)
        return (schema.query, sequence.query, Raw(self.orm))

    def execute(self):
        for query in self.query:
            try:
                self.orm.execute(query.query, query.params)
            except QueryError as e:
                logg(e.message).notice()

    @staticmethod
    def _get_comment():
        return "\n".join(
            l.strip() for l in UIDFunction.__doc__.splitlines()).strip()


class UUIDExtension(Extension):
    extras_name = 'uuid_ossp'

    def __init__(self, orm):
        super().__init__(orm, safe('"uuid-ossp"'), schema='public')

    @staticmethod
    def _get_comment():
        return """A universally unique identifier type."""


class HStoreExtension(Extension):
    extras_name = 'hstore'

    def __init__(self, orm):
        super().__init__(orm, safe('"hstore"'), schema='public')

    @staticmethod
    def _get_comment():
        return """A keyvalue storage type."""

    def execute(self):
        try:
            self.orm.execute(self.query.query, self.query.params)
            self.orm.db.register('hstore')
        except QueryError as e:
            logg(e.message).notice()


class CITextExtension(Extension):
    extras_name = 'citext'

    def __init__(self, orm):
        super().__init__(orm, safe('citext'), schema='public')

    @staticmethod
    def _get_comment():
        return """A case-insensitive text storage type."""

    def execute(self):
        try:
            self.orm.execute(self.query.query, self.query.params)
            OID, ARRAY_OID = self.orm.db.get_type_OID('citext')
            types.reg_array_type('CITEXT_ARRAY_TYPE', ARRAY_OID,
                                 psycopg2.STRING)
        except QueryError as e:
            logg(e.message).notice()
