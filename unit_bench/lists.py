import dis
from cargo.expressions import _empty
from vital.debug import Compare, Timer, RandData


clauses = RandData(str).list(10)


def l1():
    l = [_empty] * len(clauses)


def l2():
    l = [None] * len(clauses)


def l3():
    l = [_empty, _empty, _empty, _empty, _empty, _empty, _empty,
         _empty, _empty, _empty]


def l4():
    l = [None, None, None, None, None, None, None, None, None, None]


def l5():
    l = [_empty for _ in range(len(clauses))]


def l6():
    l = [None for _ in range(len(clauses))]



for l in (l1, l2, l3, l4, l5, l6):
    dis.dis(l)


c = Compare(l1, l2, l3, l4, l5, l6)
c.time(1000000)


def inot1(els):
    list(filter(lambda x: x is not _empty, els))


def inot2(els):
    list(filter(lambda x: x is not None, els))


c = Compare(inot1, inot2)
c.time(1000000, clauses)
