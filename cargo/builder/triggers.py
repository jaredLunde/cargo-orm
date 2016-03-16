"""

  `Cargo ORM Trigger Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from cargo.fields.field import Field
from cargo.expressions import *
from cargo.statements import *
from cargo.builder.utils import BaseCreator


__all__ = ('Trigger',)


class Trigger(BaseCreator):

    def __init__(self, orm, name, at, *events, table=None, ref_table=None,
                 type="STATEMENT", function=None, constraint=False,
                 timing=None, condition=None):
        """ `Create a Trigger`
            :see::func:cargo.builders.create_trigger
        """
        super().__init__(orm, name)
        self._at = str(at)
        self._table = None
        self.table(table)
        self._type = None
        self.type(type)

        self._from = None
        if ref_table:
            self.from_reference(ref_table)

        self._raw_events = list(events)
        self._events = None
        if events:
            self.events(*events)

        self._function = None
        if function:
            self.function(function)

        self._constraint = None
        if constraint:
            self.constraint()

        self._condition = None
        if condition:
            self.condition(condition)

        self._timing = None
        if timing:
            self.timing(timing)

    def insert(self):
        self._raw_events.append(Clause('INSERT'))
        self.events(*self._raw_events)
        return self

    def update_of(self, *cols):
        cols = map(self._cast_safe, cols)
        self._raw_events.append(CommaClause('UPDATE OF', *cols))
        self.events(*self._raw_events)
        return self

    def update(self):
        self._raw_events.append(Clause('UPDATE'))
        self.events(*self._raw_events)
        return self

    def delete(self):
        self._raw_events.append(Clause('DELETE'))
        self.events(*self._raw_events)
        return self

    def truncate(self):
        self._raw_events.append(Clause('TRUNCATE'))
        self.events(*self._raw_events)
        return self

    def table(self, table):
        self._table = Clause('ON', self._cast_safe(table))
        return self

    def after(self):
        self._at = Clause('AFTER')
        return self

    def before(self):
        self._at = Clause('BEFORE')
        return self

    def instead_of(self):
        self._at = Clause('BEFORE')
        return self

    def events(self, *events):
        events = map(self._cast_safe, events)
        self._events = Clause(self._at, *events, join_with=" OR ")
        return self

    def constraint(self, constraint):
        self._constraint = Clause('CONSTRAINT')
        return self

    def condition(self, condition):
        self._condition = Clause('WHEN', self._cast_safe(condition), wrap=True)
        return self

    def type(self, type):
        self._type = Clause('FOR EACH', Clause(str(type)))
        return self

    def function(self, func):
        self._function = Clause('EXECUTE PROCEDURE', self._cast_safe(func))
        return self

    def to_function(self, name, *arguments):
        func = Function(name, *arguments)
        self._function = Clause('EXECUTE PROCEDURE', func)
        return self

    def timing(self, when):
        self._timing = Clause(str(when))
        return self

    def immediately(self):
        self._timing = Clause('INITIALLY IMMEDIATE')
        return self

    def deferred(self):
        self._timing = Clause('INITIALLY DEFERRED')
        return self

    def deferrable(self):
        self._timing = Clause('DEFERRABLE')
        return self

    def not_deferrable(slef):
        self._timing = Clause('NOT DEFERRABLE')
        return self

    def from_reference(self, ref_table):
        self._from = Clause('FROM', self._cast_safe(ref_table))

    @property
    def _common_name(self):
        return "{} ON {}".format(self.name, self._table)

    @property
    def query(self):
        '''
        CREATE [ CONSTRAINT ] TRIGGER name
            { BEFORE | AFTER | INSTEAD OF } { event [ OR ... ] }
            ON table_name
            [ FROM referenced_table_name ]
            [ NOT DEFERRABLE |
                [ DEFERRABLE ] [ INITIALLY IMMEDIATE | INITIALLY DEFERRED ] ]
            [ FOR [ EACH ] { ROW | STATEMENT } ]
            [ WHEN ( condition ) ]
            EXECUTE PROCEDURE function_name ( arguments )

        where event can be one of:

            INSERT
            UPDATE [ OF column_name [, ... ] ]
            DELETE
            TRUNCATE
        '''
        self.orm.reset()
        cc = 'CREATE {}TRIGGER'.format(
            self._constraint.string + ' ' if self._constraint else "")
        self._add(Clause(cc, self.name),
                  self._events,
                  self._table,
                  self._from,
                  self._timing,
                  self._type,
                  self._condition,
                  self._function)
        return Raw(self.orm)
