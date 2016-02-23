--------------------------------------------------------------------------------
--
--  `Universally Unique ID Generator`
--   credit: http://rob.conery.io/2014/05/29/a-better-id-generator-for-postgresql/
--
--   Creates a 64-bit universally unique id. This is accomplished by
--   calculating a 41-bit representation of the milliseconds from a given epoch,
--   using a 13-bit representation of the ID number of the shard, and a
--   10-bit representation of a global ID sequence for a given schema.
--
--   The UID can be sorted by its insertion order, that is, rows submitted
--   sequentially will remain sequential in terms of their UID.
--
--------------------------------------------------------------------------------
--  64-bit UID is delegated like so:
--  =================================
--  41-bit time in milliseconds since epoch (gives 41 years of IDs)
--  13-bit logical shard ID
--  10-bit representation of an auto-incrementing sequence, modulus 1024.
--      This means we can generate 1024 IDs, per shard, per millisecond
--
--  A modulus operation works like this:
--  ====================================
--  1. Say the 'nextval' is 5061 and the modulo is 1024
--       i.e. 5061 % 1024
--  2. Divide 'nextval' by the modulo (1024 in this case)
--       5061 / 1024 = 4.942
--  3. Multiply the INTEGER value of step 2 by the modulo (1024)
--       INT 4.942 = 4
--       4 * 1024 = 4096
--  4. Subtract step 3 from 'nextval' to get the difference,
--     this is the result of the operation
--       5061 - 4096 = 965
--
--  Result: 5061 % 1024 = 965
--
--  The maximum number in 10 bits is 1024.  Therefore, by taking the modulo
--  of the sequence ids, we always get a number which is between 1 and 1024.
--  If we wanted the number to fit in an 8 bit representation we could
--  set the modulo to 255.
--
--------------------------------------------------------
--
--  At scale, simply find/replace out {schema} for each shard you have.
--
--  To use, set the `uid` field default value to `{schema}.create_uid()`
--  where X is the shard ID. The `uid` field must be of type BIGINT (int8)
--  in order to work properly as an integer field. It could still be placed
--  in a text field, etc.
--
--------------------------------------------------------------------------------
-- Creates the schema for this function if it doesn't already exist
CREATE SCHEMA {schema};

-- Creates a sequence called 'global_id_sequence'
-- used for the entire shared
CREATE SEQUENCE {schema}.global_id_sequence CACHE 10000;

-- Creates a function named 'create_uid'
CREATE OR REPLACE FUNCTION {schema}.create_uid(OUT result bigint) AS $$
DECLARE
    -- !! custom epoch, this will work for 41 years after the epoch
    our_epoch bigint := {epoch};
    -- Declares big integer variables we will use to calculate the uuid
    seq_id bigint;
    now_millis bigint;
    -- !! change this for each shard !!
    -- the id of this DB shard, must be set for each
    -- schema shard you have - you could pass this as a parameter too
    shard_id int := 1;
BEGIN
    -- Selects the next value of the sequence we created earlier
    -- and performs a modulus operator on it so that it fits in to
    -- 10 bits (this is where the 1024 comes from).
    SELECT nextval('{schema}.global_id_sequence') % 1024 INTO seq_id;

SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
    result := (now_millis - our_epoch) << 23; -- left shifts 41 bits (64 - 41)
    result := result | (shard_id << 10); -- left shifts 13 bits (64 - 41 - 13)
    result := result | (seq_id); -- Fills remaining 10 bits (from the modulus
                                 -- operation above)
END;
$$ LANGUAGE PLPGSQL;
