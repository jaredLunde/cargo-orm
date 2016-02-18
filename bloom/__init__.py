#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital ORM`
  ``A lightweight, easy to use PostgreSQL ORM inspired by Peewee and others``

  ===================
  ``Getting Started``

  * Initiate a SQL connection pool
  ..
    from vital.sql import *

    #: Creates a thread-safe connection pool which all local SQL queries
    #  will use
    create_pool(minconn=2, maxconn=10)
  ..
  * Create your first Model with :class:Model or :class:RestModel
  * Add fields to the model with :mod:vital.sql.fields
  * Add relationships to the model with :class:Relationship and
    :class:ForeignKey ..
    from vital.sql import *
    from cool_app.models import Images

    class Users(RestModel):
        username = Username(primary=True)
        password = Password(unique=True, minlen=8, not_null=True)
        authkey = AuthKey(size=256, unique=True, not_null=True)
        images = Relationship('Images.uploader_id', backref="uploader")
  ..

  * Manipulate the model, see :meth:RestModel.save and :meth:RestModel.select
  ..
    u = Users(username="jared", password="coolpasswordbrah")
    u.save()

    u.where(u.username == 'jared')
    u.select(u.authkey)
  ..

  * Delete a model record, see :meth:RestModel.delete
  ..
    u.delete()
  ..

--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

"""
from vital.sql.exceptions import *
from vital.sql.clients import *
from vital.sql.cursors import *
from vital.sql.fields import *
from vital.sql.orm import *
from vital.sql.statements import *
from vital.sql.expressions import *
from vital.sql.relationships import *


__author__ = "Jared Lunde"
__version__ = "0.9.0"
__license__ = "MIT"
