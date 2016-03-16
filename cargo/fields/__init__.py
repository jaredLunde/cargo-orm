"""

  `Cargo SQL Fields`
  ``Field-type classes for the Cargo SQL ORM``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
  Creates a model with three fields
  ..
    from cargo import *

    # Model object
    class UserModel(Model):
        # Field objects
        uid = Int(primary=True)
        username = Username(not_null=True)
        password = Password(not_null=True)
  ..

--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
  Manipulate fields in the model
  ..
    user = UserModel(username="jared", password="coolpasswordbrah")
    print(user['password'])
  ..
  |$pbkdf2-sha512$19083$VsoZY6y1NmYsZWxtDQEAoBQCoJRSaq01BiAEQMg5JwQg5Pxf...|

--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
  Set values via |__setitem__|
  ..
    user['uid'] = 1234
    print(user['uid'])
  ..
  |1234|

--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
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
   http://github.com/jaredlunde/cargo-orm

"""
from cargo.fields.field import Field
from cargo.fields.binary import *
from cargo.fields.bit import *
from cargo.fields.boolean import *
from cargo.fields.character import *
from cargo.fields.datetimes import *
from cargo.fields.encrypted import *
from cargo.fields.extras import *
from cargo.fields.geometry import *
from cargo.fields.identifier import *
from cargo.fields.integer import *
from cargo.fields.keyvalue import *
from cargo.fields.networking import *
from cargo.fields.numeric import *
from cargo.fields.ranges import *
from cargo.fields.sequence import *
