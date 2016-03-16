from bloom.expressions import *
from bloom.statements import *
from bloom.orm import ORM
from vital.debug import Compare, Timer, RandData


orm = ORM()


def raw(orm=None, iorm=None):
    Raw(orm)


def select(orm=None, iorm=None):
    Select(orm)


def pselect(orm=None, iorm=None):
    orm.state.fields = 1, 2, 3, 4, 5
    Select(orm)


def update(orm=None, iorm=None):
    Update(orm)


def delete(orm=None, iorm=None):
    Delete(orm)


orm.state.add(Clause('FROM', safe('foo')),
              Clause('WHERE', Expression('foo', '<>', 'bar')))


iorm = ORM()
iorm.state.add(Clause('INTO', safe('foo')),
               Clause('VALUES', safe(1), safe(2), wrap=True, join_with=', '))


def insert(orm=None, iorm=None):
    Insert(iorm)

c = Compare(select, pselect, raw, update, delete, insert)
c.time(100000, orm=orm, iorm=iorm)
