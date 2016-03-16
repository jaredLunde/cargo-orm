import dis
from cargo.expressions import _empty
from vital.debug import Compare, Timer, RandData


clauses = RandData(str).list(10)


def d1(x, y):
    if x not in y:
        y[x] = _empty


def d2(x, y):
    y[x] = _empty


def d3(x, y):
    y.__setitem__(x, _empty)


dis.dis(d1)
dis.dis(d2)
dis.dis(d3)


d = {'some_key': 20}


c = Compare(d1, d2, d3)
c.time(1000000, 'some_key', d)
