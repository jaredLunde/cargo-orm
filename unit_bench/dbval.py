from cargo import *
import json
import pickle
from psycopg2.extensions import *
from psycopg2.extras import *
from cargo.fields.binary import cargobytes
from vital.debug import Compare, RandData
from vital.cache import high_pickle


c = Compare(cargobytes, bytes)
c.time(1E6, high_pickle.dumps('foo'))
