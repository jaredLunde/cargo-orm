"""

  `Cargo ORM`

  ===================
  ``Getting Started``

  * Initiate a global thread-local :class:Postgres connection using
    :func:create_client
  * Alternatively initiate a global thread-local :class:ORM using
    :meth:db.open
  ..
    from cargo import db

    #: Creates a thread-safe connection pool which all local SQL queries
    #  will use
    db.open()
  ..
  * Design your first :class:Model
  * Write your first :class:Model to the DB using :class:cargo.builder.Plan
  * Add :mod:fields to the model
  ..
      from cargo import *

      class Users(Model):
          uid = UID()  # Special cargo universally unique ID type
          username = Username(unique=True, index=True, not_null=True)
          email = Email(unique=True, index=True, not_null=True)
          password = Password(unique=True, minlen=8, not_null=True)
          private_key = Key(256, unique=True, not_null=True)
  ..
  * Forge relationships in the model with :class:Relationship and
    :class:ForeignKey
  * Manipulate the model with :meth:Model.add, :meth:Model.save,
    :meth:Model.get and :meth:Model.remove
  ..
    User = Users()
    #: Adds the user 'Jared' to the DB, returns the Jared model
    Jared = User.add(username="Jared",
                     password="coolpasswordbrah",
                     private_key=Users.private_key.generate())
    #: Updates the private key and password for Jared
    Jared.private_key.new()
    Jared['password'] = 'newcoolpassword'
    #: Persists the changes to Jared to the DB
    Jared.save()
    #: Removes Jared from the DB
    Jared.remove()
  ..

--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
from cargo.exceptions import *
from cargo.clients import *
from cargo.cursors import *
from cargo.fields import *
from cargo.orm import *
from cargo.statements import *
from cargo.expressions import *
from cargo.relationships import *
from cargo.validators import *
# NOTE: http://www.postgresql.org/docs/9.5/static/bookindex.html


__author__ = "Jared Lunde"
__version__ = "0.8.0"
__license__ = "MIT"
