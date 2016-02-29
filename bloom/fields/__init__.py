#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Fields`
  ``Field-type classes for the Bloom SQL ORM``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
  ```Usage Example```

  Creates a model with three fields
  ..
    from bloom import *

    # Model object
    class UserModel(Model):
        # Field objects
        uid = Int(primary=True)
        username = Username(not_null=True)
        password = Password(not_null=True)
  ..

  ==============================================================================
  Manipulate fields in the model
  ..
    user = UserModel(username="jared", password="coolpasswordbrah")
    print(user['password'])
  ..
  |$pbkdf2-sha512$19083$VsoZY6y1NmYsZWxtDQEAoBQCoJRSaq01BiAEQMg5JwQg5Pxf...|

  ==============================================================================
  Set values via |__setitem__|
  ..
    user['uid'] = 1234
    print(user['uid'])
  ..
  |1234|

  ==============================================================================
  Creates expressions for querying
  ..
    # Saves the model to the DB
    user.save()

    # Queries the DB
    user.where(
        (user.username == 'jared') |
        (user.username.like('jare%'))
    )
    user.select(user.uid, user.username)
  ..
  |{'uid': 1234, 'username': 'jared'}|

--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.fields.field import Field
from bloom.fields.binary import *
from bloom.fields.boolean import *
from bloom.fields.character import *
from bloom.fields.datetimes import *
from bloom.fields.extras import *
from bloom.fields.geometric import *
from bloom.fields.identifier import *
from bloom.fields.integer import *
from bloom.fields.keyvalue import *
from bloom.fields.networking import *
from bloom.fields.numeric import *
from bloom.fields.ranged import *
from bloom.fields.sequence import *
from bloom.fields.xml import *


# TODO: IMMINENT
# DONE: Binary field (30 MIN)
# TODO: Currency field (40 MIN)
#   see: http://www.postgresql.org/message-id/
#           AANLkTikwEScC8bjpmLPT2XkNy_WeWCrxnWeYiw3DQxU7@mail.gmail.com
# TODO: Range fields e.g. 'int4range'  (90 MIN)
# TODO: HStore field 'hstore'  (120 MIN)
# TODO: 'CIDR': 'cidr'  (40 MIN)
# TODO: 'MACADDR': 'macaddr'  (40 MIN)
# DONE: AES-256 encrypted (30 MIN)
# NOTE: EST 6.5-8 HOURS


# TODO: SOON (Geometric Fields)
#   see:  http://www.postgresql.org/docs/8.2/static/functions-geometry.html
# TODO: 'BOX': 'box' (60 MIN)
# TODO: 'CIRCLE': 'circle' (60 MIN)
# TODO: 'LINE': 'line' (60 MIN)
# TODO: 'LSEG': 'lseg' (60 MIN)
# TODO: 'PATH': 'path' (60 MIN)
# TODO: 'POINT': 'point' (60 MIN)
# TODO: 'POLYGON': 'polygon' (60 MIN)
# NOTE: EST 7-10 HOURS

# TODO: DOWN THE LINE
# TODO: 'XML': 'xml' (120 MIN)
# NOTE: EST 2-3 HOURS
