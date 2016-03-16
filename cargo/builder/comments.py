"""

  `Cargo ORM Database Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Comment',)


class Comment(BaseCreator):

    def __init__(self, orm, type, identifier, comment=None):
        """ :see::func:cargo.builder.comment_on """
        super().__init__(orm, identifier)
        self._comment = None
        self.type = self._cast_safe(type)
        self.comment(comment)

    def comment(self, comment):
        """ @comment: (#str) comment content """
        self._comment = Clause('IS', comment)

    @property
    def query(self):
        '''
        ..
        COMMENT ON
        {
          AGGREGATE aggregate_name ( aggregate_signature ) |
          CAST (source_type AS target_type) |
          COLLATION object_name |
          COLUMN relation_name.column_name |
          CONSTRAINT constraint_name ON table_name |
          CONSTRAINT constraint_name ON DOMAIN domain_name |
          CONVERSION object_name |
          DATABASE object_name |
          DOMAIN object_name |
          EXTENSION object_name |
          EVENT TRIGGER object_name |
          FOREIGN DATA WRAPPER object_name |
          FOREIGN TABLE object_name |
          FUNCTION function_name ( [ [ argmode ] [ argname ] argtype [, ...]) |
          INDEX object_name |
          LARGE OBJECT large_object_oid |
          MATERIALIZED VIEW object_name |
          OPERATOR operator_name (left_type, right_type) |
          OPERATOR CLASS object_name USING index_method |
          OPERATOR FAMILY object_name USING index_method |
          POLICY policy_name ON table_name |
          [ PROCEDURAL ] LANGUAGE object_name |
          ROLE object_name |
          RULE rule_name ON table_name |
          SCHEMA object_name |
          SEQUENCE object_name |
          SERVER object_name |
          TABLE object_name |
          TABLESPACE object_name |
          TEXT SEARCH CONFIGURATION object_name |
          TEXT SEARCH DICTIONARY object_name |
          TEXT SEARCH PARSER object_name |
          TEXT SEARCH TEMPLATE object_name |
          TRANSFORM FOR type_name LANGUAGE lang_name |
          TRIGGER trigger_name ON table_name |
          TYPE object_name |
          VIEW object_name
        } IS 'text'
        ..
        '''
        self.orm.reset()
        self._add(Clause("COMMENT ON", self.type, self.name, self._comment))
        return Raw(self.orm)
