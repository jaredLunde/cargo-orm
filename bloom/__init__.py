"""

  `Bloom ORM`
  ``A lightweight, easy to use PostgreSQL ORM``

  ===================
  ``Getting Started``

  * Initiate a SQL connection pool
  ..
    from bloom import *

    #: Creates a thread-safe connection pool which all local SQL queries
    #  will use
    create_pool(minconn=2, maxconn=10)
  ..
  * Create your first Model with :class:Model or :class:Model
  * Add fields to the model with :mod:bloom.fields
  * Add relationships to the model with :class:Relationship and
    :class:ForeignKey ..
    from bloom import *
    from cool_app.models import Images

    class Users(Model):
        username = Username(primary=True)
        password = Password(unique=True, minlen=8, not_null=True)
        authkey = AuthKey(size=256, unique=True, not_null=True)
        images = Relationship('Images.uploader_id', backref="uploader")
  ..

  * Manipulate the model, see :meth:Model.save and :meth:Model.select
  ..
    u = Users(username="jared", password="coolpasswordbrah")
    u.save()

    u.where(u.username == 'jared')
    u.select(u.authkey)
  ..

  * Delete a model record, see :meth:Model.delete
  ..
    u.delete()
  ..

--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom import etc
from bloom.exceptions import *
from bloom.clients import *
from bloom.cursors import *
from bloom.fields import *
from bloom.orm import *
from bloom.statements import *
from bloom.expressions import *
from bloom.relationships import *
from bloom.validators import *
# NOTE: http://www.postgresql.org/docs/9.5/static/bookindex.html


__author__ = "Jared Lunde"
__version__ = "0.8.0"
__license__ = "MIT"
