from bloom import *
import json
import pickle
from psycopg2.extensions import *
from psycopg2.extras import *
from vital.debug import Timer, RandData
from vital.cache import high_pickle


# Extended subclass
class bbytes(bytes):
    def first_last(self):
        if self:
            return self[0] + self[-1]
        else:
            return ''

fv = bbytes(b'test')
print(fv)
fv = bbytes(high_pickle.dumps('foo'))
print(high_pickle.loads(high_pickle.dumps('foo')))
print(fv.first_last)
print(high_pickle.loads(fv))
