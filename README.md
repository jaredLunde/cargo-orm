# Vital
A fast, user-friendly, web & api framework written in Python. The Vital suite
includes a WSGI framework, RESTful SQL models, Redis-backed caching and session
management, User management, Upload handling controllers, and more.

## Kola
A fast, readable and feature-rich fork of the WSGI framework Bottle.
    (http://bottlepy.org/docs/dev/index.html)

### Hello World
```python
from vital.kola import * # Imports __all__
app = Kola() # 'callable' specified in uWSGI settings

@get
def hello(x):
    return "Hello world!"
```

#### Multiple instances
```python
from vital.kola import * # Imports __all__
app = Kola('/path/to/your/@config_file.json')
api = Kola('/path/to/your/@config_file2.json')
```

#### Enter (executed prior to routing) & Exit (excuted just before the response is returned) hooks
```python
@enter
def hello():
    response.set_header('access-control-allow-credentials', 'true')
@exit
def goodbye():
    response.set_header('x-app-runtime', timer.stop())
```

#### RESTful routing with plugins and callbacks
```python
# Easy-to-use
api.get("/user/<username(\w+)>", UserController)

# Accepts lists of (route_path, callback) pairs
GET = [
    ("/user/<username(\w+)>", UserController),
    ("/image/<image_id(\d+)>", ImagesController),]
api.get(GET, RouteDefaultCallback)

# Accepts decorators with and without paths
@options
def OPTIONS(x):
    return x

@post("/<user_id(\d+)>/<action>")
def PostRouteCallback(x):
    return x
```


### Kola Routing
Configurable routes used for matching request URIs to their proper callbacks and :class:View.

#### Path rules
- `*` matches any e.g. `@route("*")` or `@route("/*.html")`
- `<group_name>` returns `{'group_name': <group_name>}` to the
    callback.

    For example `@route("/api/user/<username>/<action>")`
    will return the dict `{"username": <username>, "action": <action>}`
    to the callback function.
- `(^raw$)` regex goes in parentheses, anywhere you want to put it,
    with any regex you choose. When included in a named group
    it will add the rule to that group.

    For example `@route("/api/user/<username(\w+)>/<action(\d+)>")`
    will cause \<username\> to only accept A-Za-z0-9_ characters,
    and \<action\> will only accept numeric characters.
    Another example without named groups
    `@route("/*/(.+)/([aAeEiIoOuUyY]\.html)")`

#### Callback & Plugin Rules
- Must accept only one argument, to which the route `dict()` will be returned
- Callback returns whatever it is you are sending as a response to the WSGI handler, whether that be a dict, a :class:View, plain text, bytes, etc.

#### Examples
Use a decorator to route to all request methods matching the URI
'/ping/<pong>' where \<pong\> can only be the characters A-Za-z0-9_,
with a callback bound to 'pong'

```python
from vital.kola import *

@route('/ping/<pong(\w+)>')
def pong(x):
    ''' -> #dict {'pong': x['pong']} '''
    return x
```


Add a route to GET requests with the REQUEST_URI
'/ping/<pong>' where \<pong\> can only be the characters 0-9,
with a callback bound to 'pong'
```python
from vital.kola import *

kola = Kola()
def pong(x):
    ''' Returns #dict {'pong': x['pong']} '''
    return x
kola.get('/ping/<pong(\d+)>', pong)
```


Add several routes to GET requests, with several callbacks, defaulting to 404
```python
from vital.kola import *

kola = Kola()
def pong(x):
    ''' Returns #dict {'pong': x['pong']} '''
    return x

GET = [
    ('*', View(404)), # 404 default
    ('/ping/<pong(\d+)>', pong),
    ('/ping/*', lambda x: return x),
    ('/ping/*/*/(\d+)/<long_pong((\w+).html$)', lambda x: return x),
    ('/ping?pong=([a-z0-9_-]+)', pong) ]
kola.get(GET)
```

©2015, Bottle, Marcel Hellkamp
©2015, Refactor by Jared Lunde

### Templates
Easy-to-use web templates backed by Jinja2 (http://jinja.pocoo.org/docs/dev/).

#### Example
```python
from vital.kola import Kola, template, View, get
web = Kola("/path/to/vital.json")

view = View('home') # -> template.render(
                    #       'pages/home.tpl', **config_options)
web.get("/", view)
web.get("/upload", View('upload'))

@get
def users(x):
    options = {"user": x["user"]}
    return template.render("pages/user.tpl", **options)
```

### Sessions
Memory-efficient, pythonic, lazy loading Redis sessions.

#### Example
```python
from vital.kola import session

# Sessions are dict-like structures
session["hello"] = "world"
print(session["hello"]) # "world"

session.update({"hello2": "world2", "hello3": "world3"})
print("hello" in session) # True
print(session) # {"hello": "world", "hello2": "world2", "hello3": "world3"}

session.save() # Persists to Redis
del session # Removes the session from Redis
print(session) # {}
```


### Caching
Memory-efficient, pythonic caching with a Redis backend.

©2015 Jared Lunde
