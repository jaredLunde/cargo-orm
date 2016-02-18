#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital SQL Fields`
  ``Field-type classes for the Vital SQL ORM``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
  ```Usage Example```

  Creates a model with three fields
  ..
    from vital.sql import *

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
   http://github.com/jaredlunde/VitalSQL

"""
from vital.sql.fields.field import Field
from vital.sql.fields.binary import *
from vital.sql.fields.boolean import *
from vital.sql.fields.character import *
from vital.sql.fields.datetimes import *
from vital.sql.fields.extras import *
from vital.sql.fields.geometric import *
from vital.sql.fields.identifier import *
from vital.sql.fields.integer import *
from vital.sql.fields.keyvalue import *
from vital.sql.fields.networking import *
from vital.sql.fields.numeric import *
from vital.sql.fields.ranged import *
from vital.sql.fields.sequence import *
from vital.sql.fields.xml import *


# TODO: IMMINENT
# TODO: Binary field
# TODO: Currency field
#   see: http://www.postgresql.org/message-id/
#           AANLkTikwEScC8bjpmLPT2XkNy_WeWCrxnWeYiw3DQxU7@mail.gmail.com
# TODO: Range fields e.g. 'int4range'
# TODO: HStore field 'hstore'
# TODO: AES-256 encrypted
# TODO: 'CIDR': 'cidr'
# TODO: 'MACADDR': 'macaddr'
# TODO: 'XML': 'xml'

# TODO: SOON (Geometric Fields)
#   see:  http://www.postgresql.org/docs/8.2/static/functions-geometry.html
# TODO: 'BOX': 'box'
# TODO: 'CIRCLE': 'circle'
# TODO: 'LINE': 'line'
# TODO: 'LSEG': 'lseg'
# TODO: 'PATH': 'path'
# TODO: 'POINT': 'point'
# TODO: 'POLYGON': 'polygon'
