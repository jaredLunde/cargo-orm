import copy
from bloom.expressions import _empty
from bloom.cursors import *
from vital.debug import Compare, Timer, RandData


t = Compare(copy.copy, copy.deepcopy)
t.time(100000, ModelCursor)
