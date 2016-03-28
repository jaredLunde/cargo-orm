from cargo import Model, db, Query
from cargo.fields import *
from cargo.relationships import ForeignKey
from cargo.builder import Plan, drop_schema

from vital.debug import Compare, line, banner


class Users(Model):
    ordinal = ('uid', 'username', 'password', 'join_date', 'key')
    uid = UID()
    username = Username(index=True, unique=True, not_null=True)
    password = Password(minlen=8, not_null=True)
    join_date = Timestamp(default=Timestamp.now())
    key = Key(256, index=True, unique=True)

    def __repr__(self):
        return '<' + str(self.username.value) + ':' + str(self.uid.value) + '>'


class Posts(Model):
    ordinal = ('uid', 'author', 'title', 'slug', 'content', 'tags',
               'post_date')
    uid = UID()
    title = Text(500, not_null=True)
    author = ForeignKey('Users.uid',
                        relation='posts',
                        index=True,
                        not_null=True)
    slug = Slug(UniqueSlugFactory(8),
                unique=True,
                index=True,
                not_null=True)
    content = Text(not_null=True)
    tags = Array(Text(), index='gin')
    post_date = Timestamp(default=Timestamp.now())


def build(schema):
    db.open(schema=schema)
    drop_schema(db, schema, cascade=True, if_exists=True)
    Plan(Users()).execute()
    Plan(Posts()).execute()


def teardown(schema):
    drop_schema(db, schema, cascade=True)


if __name__ == '__main__':
    #: Build
    schema = '_cargo_testing'
    build(schema)
    ITERATIONS = 5E4

    #: Play
    c = Compare(Users, Posts, name='Initialization')
    c.time(ITERATIONS)

    User = Users()
    Post = Posts()

    c = Compare(User.copy, Post.copy, name='Copying')
    c.time(ITERATIONS)

    c = Compare(User.clear_copy, Post.clear_copy, name='Clear Copying')
    c.time(ITERATIONS)

    Jared = User.add(username='Jared',
                     password='somepassword',
                     key=User.key.generate())
    Katie = User.add(username='Katie',
                     password='somepassword',
                     key=User.key.generate())
    Cream = User(username='Cream',
                 password='somepassword',
                 key=User.key.generate())

    Post.add(title='Check out this cool new something.',
             slug='Check out this cool new something.',
             author=Jared.uid,
             content='Lorem ipsum dolor.',
             tags=['fishing boats', 'cream-based sauces'])

    Post.add(title='Check out this cool new something 2.',
             slug='Check out this cool new something 2.',
             author=Jared.uid,
             content='Lorem ipsum dolor 2.',
             tags=['fishing boats', 'cream-based sauces'])

    banner('Users Iterator')
    user = User.iter()
    print('Next:', next(user))
    print('Next:', next(user))
    print('Next:', next(user))

    banner('Users')
    for user in User:
        print(user)

    banner('Posts')
    for x, post in enumerate(Post):
        if x > 0:
            line()
        print('Title:', post.title)
        print('Slug:', post.slug)
        print('Author:', post.author.pull().username)
        print('Content:', post.content)

    banner('Posts by Jared')
    for x, post in enumerate(Jared.posts.pull()):
        if x > 0:
            line()
        print('Title:', post.title)
        print('Slug:', post.slug)
        print('Author:', post.author.pull().username)
        print('Content:', post.content)

    banner('One post by Jared')
    Jared.posts.pull_one()
    print('Title:', Jared.posts.title)
    print('Slug:', Jared.posts.slug)
    print('Author:', Jared.posts.author.pull().username)
    print('Content:', Jared.posts.content)

    #: Fetchall
    def orm_run():
        return User.select()

    def raw_exec():
        cur = User.client.connection.cursor()
        cur.execute('SET search_path TO _cargo_testing;'
                    'SELECT * FROM users WHERE true;')
        ret = cur.fetchall()
        return ret

    def orm_exec():
        cur = User.client.cursor()
        cur.execute('SET search_path TO _cargo_testing;'
                    'SELECT * FROM users WHERE true;')
        ret = cur.fetchall()
        return ret

    Naked = User.copy().naked()

    def orm_naked_run():
        return Naked.run(Query('SELECT * FROM users WHERE true;'))

    def orm_naked_exec():
        return Naked.execute('SELECT * FROM users WHERE true;').fetchall()

    c = Compare(orm_run,
                raw_exec,
                orm_exec,
                orm_naked_run,
                orm_naked_exec,
                name='SELECT')
    c.time(ITERATIONS)

    #: Fetchone
    def orm_run():
        return User.where(True).get()

    def raw_exec():
        cur = User.client.connection.cursor()
        cur.execute('SET search_path TO _cargo_testing;'
                    'SELECT * FROM users WHERE true;')
        ret = cur.fetchone()
        return ret

    def orm_exec():
        cur = User.client.cursor()
        cur.execute('SET search_path TO _cargo_testing;'
                    'SELECT * FROM users WHERE true;')
        ret = cur.fetchone()
        return ret

    Naked = User.copy().naked()

    def orm_naked_run():
        q = Query('SELECT * FROM users WHERE true;')
        q.one = True
        return Naked.run(q)

    def orm_naked_exec():
        return Naked.execute('SELECT * FROM users WHERE true;').fetchone()

    c = Compare(orm_run,
                raw_exec,
                orm_exec,
                orm_naked_run,
                orm_naked_exec,
                name='GET')
    c.time(ITERATIONS)

    #: Teardown
    teardown(schema)
