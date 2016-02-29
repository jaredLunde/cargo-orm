import dis
import uuid
import time
import string
from sys import intern
from bloom.expressions import _empty
from vital.security import iter_random_chars
from vital.debug import Compare, Timer, RandData
from random import randint


def k1(bits):
    out = ""
    for char in iter_random_chars(bits, string.ascii_letters + string.digits):
        out += char
    return out


def k2(bits):
    return "".join(
        char
        for char in iter_random_chars(
            bits, string.ascii_letters + string.digits))


print(k1(512), k2(512))

c = Compare(k1, k2)
c.time(100000, 512)
