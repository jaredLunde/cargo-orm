import dis
import uuid
import time
from sys import intern
from bloom.expressions import _empty
from vital.debug import Compare, Timer, RandData
from random import randint


val = randint(10, 1000000)


def h1(val):
    return '%x' % id(val)


def h2(val):
    return hex(id(val))


def h3(val):
    return val.__hash__().__repr__()


dis.dis(h1)
dis.dis(h2)
dis.dis(h3)


print(h1(val), h2(val), h3(val))

c = Compare(h1, h2, h3)
c.time(1000000, val)
