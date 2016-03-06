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
from bloom.fields.encrypted import *
from bloom.fields.extras import *
from bloom.fields.geometry import *
from bloom.fields.identifier import *
from bloom.fields.integer import *
from bloom.fields.keyvalue import *
from bloom.fields.networking import *
from bloom.fields.numeric import *
from bloom.fields.ranges import *
from bloom.fields.sequence import *
