import copy
from cargo.expressions import _empty
from cargo.cursors import *
from vital.debug import Compare, Timer, RandData


t = Compare(copy.copy, copy.deepcopy)
t.time(100000, ModelCursor)
