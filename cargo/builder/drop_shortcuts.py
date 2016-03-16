"""

  `Cargo SQL shortcut functions for dropping tables, models, role, etc`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
from cargo import Raw, Clause, CommaClause
from cargo.builder.utils import *


__all__ = (
    'drop',
    'drop_creator',
    'Drop',
    'drop_cast',
    'drop_database',
    'drop_domain',
    'drop_operator',
    'drop_extension',
    'drop_schema',
    'drop_index',
    'drop_sequence',
    'drop_function',
    'drop_language',
    'drop_materialized_view',
    'drop_model',
    'drop_table',
    'drop_type',
    'drop_role',
    'drop_rule',
    'drop_tablespace',
    'drop_event_trigger',
    'drop_trigger',
    'drop_user',
    'drop_view'
)


# NOTE: EST 3.5-5 HOURS
class Drop(BaseCreator):

    def __init__(self, orm, type, *identifiers, cascade=False, restrict=False,
                 if_exists=False):
        """ `DROP`
            @orm: (:class:ORM)
            @type: (#str) name of the type you are dropping, e.g. |TABLE|
                or |VIEW|
            @identifiers: (#str or :class:BaseExpression) the name or
                identifier of the specific object you are dropping. For
                instance dropping a schema named 'foo', the input should just
                be |'foo'|, but if you're dropping a cast the identifier would
                be |(source_type AS target_type)|
            @cascade: (#bool) |True| to automatically drop objects that depend
                on the rule.
            @restrict: (#bool) |True| to refuse to drop the rule if any
                objects depend on it. This is the default.
            @if_exists: (#bool) |True| to not throw an error if the rule
                does not exist.
        """
        super().__init__(orm, None)
        self._type = self._cast_safe(type.upper())
        self._identifiers = map(self._cast_safe, identifiers)
        self._if_exists = None
        if if_exists:
            self.if_exists()
        self._cascade = None
        if cascade:
            self.cascade()
        self._restrict = None
        if restrict:
            self.restrict()
        self._opt = []

    @property
    def from_creator(self, orm, creator, **kwargs):
        """ @creator: (:class:BaseCreator) """
        try:
            materialized = creator._materialized
            name = 'MATERIALIZED %s' % creator.__class__.__name__
        except AttributeError:
            name = creator.__class__.__name__
        return Drop(orm, name.upper(), creator._common_name, **kwargs)

    def add_opt(self, *opt):
        """ Adds @opt options after |DROP [TYPE]|, e.g. if a given opt is
            |CONCURRENTLY|, |DROP [TYPE] CONCURRENTLY ...|

            @*opt: (#str) one or several options to prepend the statement with
        """
        self._opt.extend(map(self._cast_safe, opt))
        return self

    def if_exists(self):
        self._if_exists = Clause("IF EXISTS")
        return self

    def cascade(self):
        self._cascade = Clause('CASCADE')
        return self

    def restrict(self):
        self._restrict = Clause('RESTRICT')
        return self

    @property
    def query(self):
        """ ..
            DROP [TYPE] [OPT] [IF EXISTS] name [, ...] [CASCADE | RESTRICT]
            ..
        """
        opt = self._opt.copy()
        if self._if_exists:
            opt.append(self._if_exists)
        opt.append(CommaClause("", *self._identifiers))
        self._add(Clause('DROP %s' % self._type, *opt),
                  self._cascade,
                  self._restrict)
        return Raw(self.orm)


def _cast_return(q, dry=False):
    if dry:
        return q
    return q.execute()


def drop(orm, type, *identifiers, dry=False, **kwargs):
    """ `DROP`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, type, *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_creator(orm, creator, *opt, dry=False, **kwargs):
    """ `Drop an object from a :class:BaseCreator object`
        @orm: (:class:ORM)
        @creator: (:class:BaseCreator)
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query
    """
    drop = Drop.from_creator(orm, creator, *opt, **kwargs)
    return _cast_return(drop, dry)


def drop_extension(orm, *identifiers, dry=False, **kwargs):
    """ `DROP EXTENSION`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'EXTENSION', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_database(orm, *identifiers, dry=False, **kwargs):
    """ `DROP DATABASE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'DATABASE', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_schema(orm, *identifiers, dry=False, **kwargs):
    """ `DROP SCHEMA`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'SCHEMA', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_index(orm, *identifiers, dry=False, concurrently=False, **kwargs):
    """ `DROP INDEX`
        :see::class:Drop
        @concurrently: (#bool) True to drop concurrently
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'INDEX', *identifiers, **kwargs)
    if concurrently:
        drop.add_opt('CONCURRENTLY')
    return _cast_return(drop, dry)


def drop_sequence(orm, *identifiers, dry=False, **kwargs):
    """ `DROP SEQUENCE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'SEQUENCE', *identifiers, **kwargs)
    return _cast_return(drop, dry)



def drop_function(orm, *identifiers, dry=False, **kwargs):
    """ `DROP FUNCTION`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'FUNCTION', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_type(orm, *identifiers, dry=False, **kwargs):
    """ `DROP TYPE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'TYPE', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_role(orm, *identifiers, dry=False, **kwargs):
    """ `DROP ROLE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'ROLE', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_trigger(orm, name, table, dry=False, **kwargs):
    """ `DROP TRIGGER`
        :see::class:Drop
        @name: (#str) name of the trigger
        @table: (#str) name of the table to drop the trigger from
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'TRIGGER', '%s ON %s' % (name, table), **kwargs)
    return _cast_return(drop, dry)


def drop_user(orm, *identifiers, dry=False, **kwargs):
    """ `DROP USER`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'USER', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_view(orm, *identifiers, dry=False, **kwargs):
    """ `DROP VIEW`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'VIEW', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_materialized_view(orm, *identifiers, dry=False, **kwargs):
    """ `DROP ROLE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'MATERIALIZED VIEW', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_cast(orm, *identifiers, dry=False, **kwargs):
    """ `DROP CAST`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'CAST', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_event_trigger(orm, *identifiers, dry=False, **kwargs):
    """ `DROP EVENT TRIGGER`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'EVENT TRIGGER', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_domain(orm, *identifiers, dry=False, **kwargs):
    """ `DROP DOMAIN`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'DOMAIN', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_language(orm, *identifiers, dry=False, **kwargs):
    """ `DROP LANGUAGE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'LANGUAGE', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_operator(orm, *identifiers, dry=False, **kwargs):
    """ `DROP OPERATOR`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'OPERATOR', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_rule(orm, name, table, dry=False, **kwargs):
    """ `DROP RULE`
        @name: (#str) name of the rule
        @table: (#str) name of the table the rule exists in
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'RULE', '%s ON %s' % (name, table), **kwargs)
    return _cast_return(drop, dry)


def drop_tablespace(orm, *identifiers, dry=False, **kwargs):
    """ `DROP TABLESPACE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'TABLESPACE', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_table(orm, *identifiers, dry=False, **kwargs):
    """ `DROP TABLE`
        :see::class:Drop
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drop = Drop(orm, 'TABLE', *identifiers, **kwargs)
    return _cast_return(drop, dry)


def drop_model(*models, dry=False, **kwargs):
    """ `DROP model tables`
        :see::class:Drop
        @models: (:class:Model)
        @dry: (#bool) True to return the :class:Drop object as opposed to
            executing a query

        -> (#list) :class:Drop if dry is |True|, otherwise the client cursor of
            the executed query is returned
    """
    drops = []
    add_drop = drops.append
    for model in models:
        add_drop(drop_table(model, model.table, dry=dry, **kwargs))
    return drops
