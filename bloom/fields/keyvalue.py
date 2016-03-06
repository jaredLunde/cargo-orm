"""

  `Bloom SQL Key-Value Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import collections
import psycopg2.extras
import psycopg2.extensions

try:
    import ujson as json
except ImportError:
    import json

from bloom.etc.types import *
from bloom.expressions import *
from bloom.fields.field import Field


__all__ = ('JsonLogic', 'JsonBLogic', 'Json', 'JsonB', 'HStore')


class JsonLogic(BaseLogic):

    _field_op = '->'
    _field_text_op = '->>'
    _field_path_op = '#>'
    _field_path_text_op = '#>>'

    def get_element(self, index, as_text=False, **kwargs):
        """ Gets a Json array element

            @index: (#int) index of the element within a Json array
            @as_text: (#bool) True to select the value as text rather than
                its data type
            -> (:class:Expression)
        """
        op = self._field_op
        if as_text:
            op = self._field_text_op
        return Expression(self, op, index, **kwargs)

    get_index = get_element

    def get_field(self, field, as_text=False, **kwargs):
        """ Gets a Json object field

            @field: (#list|#tuple) |[key1, key2]|
            @as_text: (#bool) True to select the value as text rather than
                its data type
            -> (:class:Expression)
        """
        op = self._field_op
        if as_text:
            op = self._field_text_op
        return Expression(self, op, field, **kwargs)

    get_key = get_field

    def get_path(self, path, as_text=False, **kwargs):
        """ Gets a Json object at specified path

            @path: (#list|#tuple) |[key1, key2]|
            @as_text: (#bool) True to select the value as text rather than
                its data type
            -> (:class:Expression)
        """
        op = self._field_path_op
        if as_text:
            op = self._field_path_text_op
        return Expression(self, op, path, **kwargs)

    def each(self, **kwargs):
        """ Expands the outermost JSON object into a set of key/value pairs.
            -> (:class:Function)
        """
        return Functions.json_each(self, **kwargs)

    def each_text(self, **kwargs):
        """ Expands the outermost JSON object into a set of key/value pairs.
            The returned values will be of type text.
            -> (:class:Function)
        """
        return Functions.json_each_text(self, **kwargs)

    def array_length(self, **kwargs):
        """ Finds the number of elements in the outermost JSON array.
            -> (:class:Function)
        """
        return Functions.json_array_length(self, **kwargs)

    def get_fields(self, **kwargs):
        """ Returns set of keys in the outermost JSON object.
            -> (:class:Function)
        """
        return Functions.json_object_keys(self, **kwargs)

    get_keys = get_fields

    def get_elements(self, **kwargs):
        """ Expands a JSON array to a set of JSON values.
            -> (:class:Function)
        """
        return Functions.json_array_elements(self, **kwargs)

    def set_path(self, path, create_missing=True, **kwargs):
        """ Returns target with the section designated by path replaced by
            new_value, or with new_value added if create_missing is true
            (default is true) and the item designated by path does not exist.
            As with the path orientated operators, negative integers that
            appear in path count from the end of JSON arrays.
            -> (:class:Function)
        """
        return Functions.json_set_path(self, path, create_missing, **kwargs)


class JsonBLogic(JsonLogic):

    _contains_op = '@>'
    _contained_op = '<@'
    _key_op = '?'
    _keys_any_op = '?|'
    _keys_all_op = '?&'
    _concat_op = '||'
    _delete_key_op = '-'
    _delete_path_op = '#-'

    def contains(self, other, **kwargs):
        """ Does this JSON value contain the @other JSON path/value
            entries at the top level?
            -> (:class:Expression)
        """
        return Expression(self, self._contains_op, other, **kwargs)

    def contained_by(self, other, **kwargs):
        """ Are these JSON path/value entries contained at the top level
            within the @other JSON value?
            -> (:class:Expression)
        """
        return Expression(self, self._contained_op, other, **kwargs)

    def field_exists(self, field):
        """ Does the @field exist as a top-level field within this JSON value?
            -> (:class:Expression)
        """
        return Expression(self, self._key_op, field, **kwargs)

    key_exists = field_exists

    def fields_exist(self, *fields, all=False, **kwargs):
        """ Do any/@all of these @fields exist as top-level @fields?
            -> (:class:Expression)
        """
        op = self._keys_any_op
        if all:
            op = self._keys_all_op
        return Expression(self, op, list(fields), **kwargs)

    keys_exist = fields_exist

    def concat(self, other, **kwargs):
        """ Concatenate @other jsonb values into this one as a new jsonb value
            -> (:class:Expression)
        """
        return Expression(self, self._concat_op, other, **kwargs)

    def remove_field(self, field, **kwargs):
        """ Delete @key/value pair or string element from this field.
            -> (:class:Expression)
        """
        return Expression(self, self._delete_key_op, field, **kwargs)

    remove_key = remove_field

    def remove_element(self, index, **kwargs):
        """ Delete the array element with specified @index (Negative integers
            count from the end). Throws an error if top level container is
            not an array.
            -> (:class:Expression)
        """
        return self.remove_field(index, **kwargs)

    remove_index = remove_element

    def remove_path(self, path, **kwargs):
        """ Delete the field or element with specified path (for JSON
            arrays, negative integers count from the end)
            -> (:class:Expression)
        """
        return Expression(self, self._delete_path_op, path, **kwargs)

    def each(self, **kwargs):
        """ Expands the outermost JSON object into a set of key/value pairs.
            -> (:class:Function)
        """
        return Functions.jsonb_each(self, **kwargs)

    def each_text(self, **kwargs):
        """ Expands the outermost JSON object into a set of key/value pairs.
            The returned values will be of type text.
            -> (:class:Function)
        """
        return Functions.jsonb_each_text(self, **kwargs)

    def array_length(self, **kwargs):
        """ Finds the number of elements in the outermost JSON array.
            -> (:class:Function)
        """
        return Functions.jsonb_array_length(self, **kwargs)

    def get_fields(self, **kwargs):
        """ Returns set of keys in the outermost JSON object.
            -> (:class:Function)
        """
        return Functions.jsonb_object_keys(self, **kwargs)

    get_keys = get_fields

    def get_elements(self, **kwargs):
        """ Expands a JSON array to a set of JSON values.
            -> (:class:Function)
        """
        return Functions.jsonb_array_elements(self, **kwargs)

    def set_path(self, path, create_missing=True, **kwargs):
        """ Returns target with the section designated by path replaced by
            new_value, or with new_value added if create_missing is true
            (default is true) and the item designated by path does not exist.
            As with the path orientated operators, negative integers that
            appear in path count from the end of JSON arrays.
            -> (:class:Function)
        """
        return Functions.jsonb_set_path(self, path, create_missing, **kwargs)


class KeyValueOps(object):

    def __contains__(self, name):
        return name in self.value

    def __getitem__(self, name):
        return self.value[name]

    def __setitem__(self, name, value):
        self.value[name] = value

    def __delitem__(self, name):
        del self.value[name]

    def __iter__(self):
        return self.value.__iter__()

    def keys(self):
        return self.value.keys()

    def items(self):
        return self.value.items()

    def values(self):
        return self.value.values()

    def get(self, name, default=None):
        return self.value.get(name, default)

    def pop(self, index):
        return self.value.pop(index)

    def update(self, value):
        self.value.update(value)
        return self.value


class _jsontype(object):
    @staticmethod
    def to_db(value):
        return psycopg2.extensions.QuotedString(json.dumps(value)).getquoted()

    def __str__(self):
        # getquoted is binary in Py3
        return self.to_db(self).decode('ascii', 'replace')


class jsondict(dict, _jsontype):
    pass


class jsonlist(list, _jsontype):
    pass


class jsonstr(str, _jsontype):
    pass


class jsonint(int, _jsontype):
    pass

class jsonfloat(float, _jsontype):
    pass


psycopg2.extensions.register_adapter(jsondict, jsondict.to_db)
psycopg2.extensions.register_adapter(jsonlist, jsonlist.to_db)
psycopg2.extensions.register_adapter(jsonstr, jsonstr.to_db)
psycopg2.extensions.register_adapter(jsonint, jsonint.to_db)
psycopg2.extensions.register_adapter(jsonfloat, jsonfloat.to_db)
JSONTYPE = psycopg2.extensions.new_type(
    (JSON, JSONB), "JSONTYPE", json.loads)
psycopg2.extensions.register_type(JSONTYPE)

_jsontypes = (((collections.Mapping, collections.ItemsView, dict), jsondict),
              (str, jsonstr),
              (int, jsonint),
              (float, jsonfloat),
              (collections.Iterable, jsonlist))


def _get_json(val):
    for instance, typ in _jsontypes:
        if isinstance(val, instance):
            return typ(val)
    raise TypeError('Could not adapt type `%s` to json.' % type(val))


class Json(Field, KeyValueOps, JsonLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |JSON|

        The value given to this field must be Json serializable. It is
        automatically encoded and decoded on insertion and retrieval.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'default', 'validation', 'validation_error', '_alias', 'table',
        'cast')
    sqltype = JSON

    def __init__(self, value=Field.empty, cast=None, *args, **kwargs):
        """ `Json`
            :see::meth:Field.__init__
            @cast: type cast for specifying the type of data should be expected
                for the value property, e.g. |dict| or |list|
        """
        self.cast = cast
        value = value or (self.cast() if self.cast is not None else value)
        super().__init__(value, *args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if self.cast:
                value = self.cast(value)
            if value is not None:
                value = _get_json(value)
            self._set_value(value)
        return self.value

    def from_json(self, value):
        """ Loads @value from Json and inserts it as the value of the field """
        return self.__call__(json.loads(value))

    def to_json(self, *opt, **opts):
        """ Dumps :prop:value to Json and returns it
            -> (#str)
        """
        return json.dumps(value, *opt, **opts)

    def remove(self, name):
        self.value.remove(name)
        return self

    def append(self, value):
        self.value.append(value)
        return self

    def insert(self, index, value):
        self.value.insert(index, value)
        return self

    def extend(self, value):
        """ Extends a list with @value """
        self.value.extend(value)
        return self

    def reverse(self):
        """ Reverses a list in place """
        self.value.reverse()
        return self

    def sort(self, key=None, reverse=False):
        """ Sorts a list in place """
        self.value.sort(key=key, reverse=reverse)
        return self


class JsonB(Json, JsonBLogic):
    """ =======================================================================
        Field object for the PostgreSQL field type |JSONB|

        The value given to this field must be able Json serializable. It is
        automatically encoded and decoded on insertion and retrieval.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'not_null', 'value',
        'default', 'validation', 'validation_error', '_alias', 'table',
        'cast')
    sqltype = JSONB

    def __init__(self, value=Field.empty, *args, **kwargs):
        """ `JsonB`
            :see::meth:Field.__init__
        """
        super().__init__(value=value, *args, **kwargs)


class HStoreLogic(JsonBLogic):
    _replace_keys_op = '#='
    _convert_to_array = """%%"""
    _convert_to_items = """%#"""

    '''
    hstore -> text	get value for key (NULL if not present)	'a=>x, b=>y'::hstore -> 'a'	x
    hstore -> text[]	get values for keys (NULL if not present)	'a=>x, b=>y, c=>z'::hstore -> ARRAY['c','a']	{"z","x"}
    hstore || hstore	concatenate hstores	'a=>b, c=>d'::hstore || 'c=>x, d=>q'::hstore	"a"=>"b", "c"=>"x", "d"=>"q"
    hstore ? text	does hstore contain key?	'a=>1'::hstore ? 'a'	t
    hstore ?& text[]	does hstore contain all specified keys?	'a=>1,b=>2'::hstore ?& ARRAY['a','b']	t
    hstore ?| text[]	does hstore contain any of the specified keys?	'a=>1,b=>2'::hstore ?| ARRAY['b','c']	t
    hstore @> hstore	does left operand contain right?	'a=>b, b=>1, c=>NULL'::hstore @> 'b=>1'	t
    hstore <@ hstore	is left operand contained in right?	'a=>c'::hstore <@ 'a=>b, b=>1, c=>NULL'	f
    hstore - text	delete key from left operand	'a=>1, b=>2, c=>3'::hstore - 'b'::text	"a"=>"1", "c"=>"3"
    hstore - text[]	delete keys from left operand	'a=>1, b=>2, c=>3'::hstore - ARRAY['a','b']	"c"=>"3"
    hstore - hstore	delete matching pairs from left operand	'a=>1, b=>2, c=>3'::hstore - 'a=>4, b=>2'::hstore	"a"=>"1", "c"=>"3"
    record #= hstore	replace fields in record with matching values from hstore	see Examples section
    %% hstore	convert hstore to array of alternating keys and values	%% 'a=>foo, b=>bar'::hstore	{a,foo,b,bar}
    %# hstore	convert hstore to two-dimensional key/value array	%# 'a=>foo, b=>bar'::hstore	{{a,foo},{b,bar}}
    '''

    def get_value(self, *keys, **kwargs):
        pass

    def concat(self, *kwags, **kwargs):
        pass


class HStore(Field, KeyValueOps, HStoreLogic):
    sqltype = HSTORE

    def __init__(self, value=None, *args, **kwargs):
        """ `HStore`
            :see::meth:Field.__init__
        """
        super().__init__(value, *args, **kwargs)

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            self._set_value(dict(value) if value is not None else None)
        return self.value

    '''
    hstore(record)	hstore	construct an hstore from a record or row	hstore(ROW(1,2))	f1=>1,f2=>2
    hstore(text[])	hstore	construct an hstore from an array, which may be either a key/value array, or a two-dimensional array	hstore(ARRAY['a','1','b','2']) || hstore(ARRAY[['c','3'],['d','4']])	a=>1, b=>2, c=>3, d=>4
    hstore(text[], text[])	hstore	construct an hstore from separate key and value arrays	hstore(ARRAY['a','b'], ARRAY['1','2'])	"a"=>"1","b"=>"2"
    hstore(text, text)	hstore	make single-item hstore	hstore('a', 'b')	"a"=>"b"
    akeys(hstore)	text[]	get hstore's keys as an array	akeys('a=>1,b=>2')	{a,b}
    skeys(hstore)	setof text	get hstore's keys as a set	skeys('a=>1,b=>2')
    a
    b
    avals(hstore)	text[]	get hstore's values as an array	avals('a=>1,b=>2')	{1,2}
    svals(hstore)	setof text	get hstore's values as a set	svals('a=>1,b=>2')
    1
    2
    hstore_to_array(hstore)	text[]	get hstore's keys and values as an array of alternating keys and values	hstore_to_array('a=>1,b=>2')	{a,1,b,2}
    hstore_to_matrix(hstore)	text[]	get hstore's keys and values as a two-dimensional array	hstore_to_matrix('a=>1,b=>2')	{{a,1},{b,2}}
    hstore_to_json(hstore)	json	get hstore as a json value, converting all non-null values to JSON strings	hstore_to_json('"a key"=>1, b=>t, c=>null, d=>12345, e=>012345, f=>1.234, g=>2.345e+4')	{"a key": "1", "b": "t", "c": null, "d": "12345", "e": "012345", "f": "1.234", "g": "2.345e+4"}
    hstore_to_jsonb(hstore)	jsonb	get hstore as a jsonb value, converting all non-null values to JSON strings	hstore_to_jsonb('"a key"=>1, b=>t, c=>null, d=>12345, e=>012345, f=>1.234, g=>2.345e+4')	{"a key": "1", "b": "t", "c": null, "d": "12345", "e": "012345", "f": "1.234", "g": "2.345e+4"}
    hstore_to_json_loose(hstore)	json	get hstore as a json value, but attempt to distinguish numerical and Boolean values so they are unquoted in the JSON	hstore_to_json_loose('"a key"=>1, b=>t, c=>null, d=>12345, e=>012345, f=>1.234, g=>2.345e+4')	{"a key": 1, "b": true, "c": null, "d": 12345, "e": "012345", "f": 1.234, "g": 2.345e+4}
    hstore_to_jsonb_loose(hstore)	jsonb	get hstore as a jsonb value, but attempt to distinguish numerical and Boolean values so they are unquoted in the JSON	hstore_to_jsonb_loose('"a key"=>1, b=>t, c=>null, d=>12345, e=>012345, f=>1.234, g=>2.345e+4')	{"a key": 1, "b": true, "c": null, "d": 12345, "e": "012345", "f": 1.234, "g": 2.345e+4}
    slice(hstore, text[])	hstore	extract a subset of an hstore	slice('a=>1,b=>2,c=>3'::hstore, ARRAY['b','c','x'])	"b"=>"2", "c"=>"3"
    each(hstore)	setof(key text, value text)	get hstore's keys and values as a set	select * from each('a=>1,b=>2')
     key | value
    -----+-------
     a   | 1
     b   | 2
    exist(hstore,text)	boolean	does hstore contain key?	exist('a=>1','a')	t
    defined(hstore,text)	boolean	does hstore contain non-NULL value for key?	defined('a=>NULL','a')	f
    delete(hstore,text)	hstore	delete pair with matching key	delete('a=>1,b=>2','b')	"a"=>"1"
    delete(hstore,text[])	hstore	delete pairs with matching keys	delete('a=>1,b=>2,c=>3',ARRAY['a','b'])	"c"=>"3"
    delete(hstore,hstore)	hstore	delete pairs matching those in the second argument	delete('a=>1,b=>2','a=>4,b=>2'::hstore)	"a"=>"1"
    populate_record(record,hstore)	record	replace fields in record with matching values from hstore	see Examples section
    '''
