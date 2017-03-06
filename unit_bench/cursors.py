import copy
import psycopg2

from cargo.expressions import _empty
from cargo.cursors import *
from cargo import ORM, db

from vital.debug import Compare, Timer, RandData, bold


db.open()


'''
(Results after 50000 intervals)
‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒
#1 ¦  321.86µs            'OrderedDictCursor'
#2 ¦  334.85µs     -4.04% 'RealDictCursor'
#3 ¦  335.53µs     -4.25% 'DictCursor'
#4 ¦  357.22µs    -10.99% 'CNamedTupleCursor'
‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒‒
'''


cnt = ORM(cursor_factory=CNamedTupleCursor)
nt = ORM(cursor_factory=psycopg2.extras.NamedTupleCursor)
od = ORM(cursor_factory=OrderedDictCursor)
d = ORM(cursor_factory=psycopg2.extras.DictCursor)
rd = ORM(cursor_factory=psycopg2.extras.RealDictCursor)


print(bold('CNamedTuple'))
t = Timer(cnt.select)
t.time(50000, 1, 2, 3, 4, 5, 'a', 'b', 'c', 'd', 'e', 'f')


'''
Fails
-----
print(bold('NamedTuple'))
t = Timer(nt.select)
t.time(50000, 1, 2, 3, 4, 5, 'a', 'b', 'c', 'd', 'e', 'f')
'''


print(bold('OrderedDict'))
t = Timer(od.select)
t.time(50000, 1, 2, 3, 4, 5, 'a', 'b', 'c', 'd', 'e', 'f')


print(bold('Dict'))
t = Timer(d.select)
t.time(50000, 1, 2, 3, 4, 5, 'a', 'b', 'c', 'd', 'e', 'f')


print(bold('RealDict'))
t = Timer(rd.select)
t.time(50000, 1, 2, 3, 4, 5, 'a', 'b', 'c', 'd', 'e', 'f')
