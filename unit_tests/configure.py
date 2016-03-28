import os
import unittest
import psycopg2

from vital.security import randhex

from cargo import db, Model as _Model, fields
from cargo.fields import *
from cargo.expressions import *
from cargo.statements import Insert
from cargo.builder import *


db.open()


def run_tests(*tests, **opts):
    suite = unittest.TestSuite()
    for test_class in tests:
        tests = unittest.defaultTestLoader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    ut = unittest.TextTestRunner(**opts)
    return ut.run(suite)


def run_discovered(path=None):
    path = path or os.path.dirname(os.path.realpath(__file__))
    ut = unittest.TextTestRunner(verbosity=2, failfast=True)
    tests = []
    suite = unittest.TestSuite()
    for test in unittest.defaultTestLoader.discover(
            path, pattern='*.py', top_level_dir=None):
        suite.addTests((t for t in test
                        if t not in tests and not tests.append(t)))
    return ut.run(suite)


def setup():
    drop_schema(db, 'cargo_tests', cascade=True, if_exists=True)
    create_schema(db, 'cargo_tests')


def cleanup():
    drop_schema(db, 'cargo_tests', cascade=True, if_exists=True)


def new_field(type='text', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randhex(24)
    field.table = table or randhex(24)
    return field


def new_expression(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Expression(new_field(), '=', cast(12345))


def new_function(cast=int, alias=None):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Function('some_func', cast(12345), alias=alias)


def new_clause(name='FROM', *vals, **kwargs):
    vals = vals or ['foobar']
    return Clause(name, *vals, **kwargs)


class Model(_Model):
    schema = 'cargo_tests'
    uid = UID()


class Foo(_Model):
    schema = 'cargo_tests'
    ordinal = ('uid', 'textfield')
    uid = Int(index=True, unique=True)
    textfield = Text()


class FooB(Foo):
    pass


class BaseTestCase(unittest.TestCase):

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()


class StatementTestCase(BaseTestCase):
    orm = Foo(schema='cargo_tests', naked=True)
    orm_b = FooB(schema='cargo_tests', naked=True)

    @classmethod
    def setUpClass(cls):
        from cargo.builder.extras import UIDFunction
        setup()
        Plan(Foo()).execute()
        Plan(FooB()).execute()
        UIDFunction(cls.orm).execute()

    def setUp(self):
        for field in self.orm.fields:
            field.clear()
        self._populate()
        self.orm.reset()
        self.orm_b.reset()

    def tearDown(self):
        for orm in (self.orm, self.orm_b):
            orm.reset()
            orm.where(True).delete()

    def _populate(self):
        self.orm.state.add(new_clause('INTO', safe('foo')))
        self.orm_b.state.add(new_clause('INTO', safe('foo_b')))
        def values(orm, start):
            orm.fields[0](start)
            orm.fields[1](randhex(10))
            yield orm.fields[0]
            yield orm.fields[1]
        for orm in (self.orm, self.orm_b):
            start = 1234
            orm.values(*values(orm, start))
            start += 1
            orm.values(*values(orm, start))
            start += 1
            orm.values(*values(orm, start))
        Insert(self.orm).execute()
        Insert(self.orm_b).execute()


class LogicTestCase(unittest.TestCase):

    def validate_expression(self, expression, left, operator, right,
                            params=None, values=None):
        self.assertIsInstance(expression, Expression)
        self.assertIs(expression.left, left)
        self.assertEqual(expression.operator, operator)
        self.assertEqual(expression.right, right)
        if params is not None:
            self.assertDictEqual(expression.params, params)
        elif values:
            for value in values:
                self.assertIn(value, list(expression.params.values()))

    def validate_function(self, function, func, args, alias=None, values=None):
        self.assertIsInstance(function, Function)
        self.assertEqual(function.func, func)
        self.assertTupleEqual(function.args, tuple(args))
        self.assertEqual(function.alias, alias)
        if values:
            for value in values:
                self.assertIn(value, list(function.params.values()))


#: Builder setup
class BuilderTestCase(BaseTestCase):
    orm = Foo(schema='cargo_tests', naked=True)

    @classmethod
    def setUpClass(cls):
        setup()
        Plan(Foo()).execute()

    def setUp(self):
        self.orm.clear()

    def tearDown(self):
        self.orm.clear()


#: Geometry setup
class GeoModel(Model):
    path = Path()
    lseg = LSeg()
    poly = Polygon()
    point = Point()
    line = Line()
    box = Box()
    circle = Circle()
    array_path = Array(Path())
    array_lseg = Array(LSeg())
    array_poly = Array(Polygon())
    array_point = Array(Point())
    array_line = Array(Line())
    array_box = Array(Box())
    array_circle = Array(Circle())


class GeoPlan(Plan):
    model = GeoModel()


class GeoTestCase(BaseTestCase):
    orm = GeoModel()

    @classmethod
    def setUpClass(cls):
        setup()
        GeoPlan().execute()


#: Integer setup
class IntModel(Model):
    integer = Int()
    bigint = BigInt()
    smallint = SmallInt()
    array_integer = Array(Int())
    array_bigint = Array(BigInt())
    array_smallint = Array(SmallInt())
    enc_integer = Encrypted(Encrypted.generate_secret(), Int())
    enc_bigint = Encrypted(Encrypted.generate_secret(), BigInt())
    enc_smallint = Encrypted(Encrypted.generate_secret(), SmallInt())
    array_enc_integer = Array(enc_integer)
    array_enc_bigint = Array(enc_bigint)
    array_enc_smallint = Array(enc_smallint)


class IntPlan(Plan):
    model = IntModel()


class IntTestCase(BaseTestCase):
    orm = IntModel()

    @classmethod
    def setUpClass(cls):
        setup()
        IntPlan().execute()


#: Character setup
class CharModel(Model):
    char = Char(maxlen=200)
    varchar = Varchar(maxlen=200)
    text = Text()
    array_char = Array(Char(maxlen=200))
    array_varchar = Array(Varchar(maxlen=200))
    array_text = Array(Text())
    enc_char = Encrypted(Encrypted.generate_secret(), Char(maxlen=200))
    enc_varchar = Encrypted(Encrypted.generate_secret(), Varchar(maxlen=200))
    enc_text = Encrypted(Encrypted.generate_secret(), Text())
    array_enc_char = Array(enc_char)
    array_enc_varchar = Array(enc_varchar)
    array_enc_text = Array(enc_text)


class CharPlan(Plan):
    model = CharModel()


class CharTestCase(BaseTestCase):
    orm = CharModel()

    @classmethod
    def setUpClass(cls):
        setup()
        CharPlan().execute()


#: Networking setup
class NetModel(Model):
    ip = IP()
    cidr = Cidr()
    mac = MacAddress()
    array_ip = Array(IP())
    array_cidr = Array(Cidr())
    array_mac = Array(MacAddress())
    enc_ip = Encrypted(Encrypted.generate_secret(), IP())
    enc_cidr = Encrypted(Encrypted.generate_secret(), Cidr())
    enc_mac = Encrypted(Encrypted.generate_secret(), MacAddress())
    array_enc_ip = Array(enc_ip)
    array_enc_cidr = Array(enc_cidr)
    array_enc_mac = Array(enc_mac)


class NetPlan(Plan):
    model = NetModel()


class NetTestCase(BaseTestCase):
    orm = NetModel()

    @classmethod
    def setUpClass(cls):
        setup()
        NetPlan().execute()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()


#: Numeric setup
class NumModel(Model):
    dec = Decimal()
    float4 = Float()
    float8 = Double()
    currency = Currency()
    money = Money()
    array_dec = Array(Decimal())
    array_float4 = Array(Float())
    array_float8 = Array(Double())
    array_currency = Array(Currency())
    array_money = Array(Money())
    enc_dec = Encrypted(Encrypted.generate_secret(), Decimal())
    enc_float4 = Encrypted(Encrypted.generate_secret(), Float())
    enc_float8 = Encrypted(Encrypted.generate_secret(), Double())
    enc_currency = Encrypted(Encrypted.generate_secret(), Currency())
    enc_money = Encrypted(Encrypted.generate_secret(), Money())
    array_enc_dec = Array(enc_dec)
    array_enc_float4 = Array(enc_float4)
    array_enc_float8 = Array(enc_float8)
    array_enc_currency = Array(enc_currency)
    array_enc_money = Array(enc_money)


class NumPlan(Plan):
    model = NumModel()


class NumTestCase(BaseTestCase):
    orm = NumModel()

    @classmethod
    def setUpClass(cls):
        setup()
        NumPlan().execute()


#: Extras setup
class ExtrasModel(Model):
    username = Username()
    email = Email()
    password = Password()
    slug = Slug()
    key = Key()
    phone = PhoneNumber()
    duration = Duration()
    array_username = Array(Username())
    array_email = Array(Email())
    array_password = Array(Password())
    array_slug = Array(Slug())
    array_key = Array(Key())
    array_phone = Array(PhoneNumber())
    array_duration = Array(Duration())
    enc_username = Encrypted(Encrypted.generate_secret(),
                             Username(not_null=False))
    enc_email = Encrypted(Encrypted.generate_secret(), Email())
    enc_password = Encrypted(Encrypted.generate_secret(), Password())
    enc_slug = Encrypted(Encrypted.generate_secret(), Slug())
    enc_key = Encrypted(Encrypted.generate_secret(), Key())
    enc_phone = Encrypted(Encrypted.generate_secret(), PhoneNumber())
    enc_duration = Encrypted(Encrypted.generate_secret(), Duration())
    array_enc_username = Array(enc_username)
    array_enc_email = Array(enc_email)
    array_enc_password = Array(enc_password)
    array_enc_slug = Array(enc_slug)
    array_enc_key = Array(enc_key)
    array_enc_phone = Array(enc_phone)
    array_enc_duration = Array(enc_duration)


class ExtrasPlan(Plan):
    model = ExtrasModel()


class ExtrasTestCase(BaseTestCase):
    orm = ExtrasModel()

    @classmethod
    def setUpClass(cls):
        setup()
        ExtrasPlan().execute()


#: Binary setup
class BinaryModel(Model):
    binary_field = Binary()
    array_binary_field = Array(Binary())
    enc_binary_field = Encrypted(Encrypted.generate_secret(), Binary())
    array_enc_binary_field = Array(enc_binary_field)


class BinaryPlan(Plan):
    model = BinaryModel()


class BinaryTestCase(BaseTestCase):
    orm = BinaryModel()

    @classmethod
    def setUpClass(cls):
        setup()
        BinaryPlan().execute()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()


#: Binary setup
class BitModel(Model):
    bit_field = Bit(4)
    varbit_field = Varbit(4)
    array_bit_field = Array(Bit(4))
    array_varbit_field = Array(Varbit(4))


class BitPlan(Plan):
    model = BitModel()


class BitTestCase(BaseTestCase):
    orm = BitModel()

    @classmethod
    def setUpClass(cls):
        setup()
        BitPlan().execute()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()


#: Boolean setup
class BooleanModel(Model):
    boolean = Bool()
    array_boolean = Array(Bool())


class BooleanPlan(Plan):
    model = BooleanModel()


class BooleanTestCase(BaseTestCase):
    orm = BooleanModel()

    @classmethod
    def setUpClass(cls):
        setup()
        BooleanPlan().execute()


#: DateTime setup
class DateTimeModel(Model):
    time = Time()
    timetz = TimeTZ()
    ts = Timestamp()
    tstz = TimestampTZ()
    date = Date()
    array_time = Array(Time())
    array_timetz = Array(TimeTZ())
    array_ts = Array(Timestamp())
    array_tstz = Array(TimestampTZ())
    array_date = Array(Date())
    enc_time = Encrypted(Encrypted.generate_secret(), Time())
    enc_date = Encrypted(Encrypted.generate_secret(), Date())
    enc_ts = Encrypted(Encrypted.generate_secret(), Timestamp())
    array_enc_time = Array(enc_time)
    array_enc_date = Array(enc_date)
    array_enc_ts = Array(enc_ts)


class DateTimePlan(Plan):
    model = DateTimeModel()


class DateTimeTestCase(BaseTestCase):
    orm = DateTimeModel()

    @classmethod
    def setUpClass(cls):
        setup()
        DateTimePlan().execute()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()


#: Identifier setup
class UIDModel(Model):
    pass


class StrUIDModel(Model):
    uid = StrUID()


class SerialModel(_Model):
    schema = 'cargo_tests'
    uid = None
    serial = Serial()


class SmallSerialModel(SerialModel):
    serial = SmallSerial()


class BigSerialModel(SerialModel):
    serial = BigSerial()


class UUIDModel(Model):
    uid = UID(primary=False)
    uuid = UUID()
    array_uuid = Array(UUID())


class IdentifierTestCase(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        setup()
        for m in (UIDModel(), StrUIDModel(), SerialModel(), SmallSerialModel(),
                  BigSerialModel(), UUIDModel()):
            Plan(model=m).execute()


#: KeyValue setup
class KeyValueModel(Model):
    json_field = Json()
    jsonb_field = JsonB()
    hstore_field = HStore()
    array_json_field = Array(Json())
    array_jsonb_field = Array(JsonB())
    array_hstore_field = Array(HStore())
    enc_json = Encrypted(Encrypted.generate_secret(), Json())
    enc_jsonb = Encrypted(Encrypted.generate_secret(), JsonB())
    enc_hstore = Encrypted(Encrypted.generate_secret(), HStore())
    array_enc_json = Array(enc_json)
    array_enc_jsonb = Array(enc_jsonb)
    array_enc_hstore = Array(enc_hstore)


class KeyValuePlan(Plan):
    model = KeyValueModel()


class KeyValueTestCase(BaseTestCase):
    orm = KeyValueModel()

    @classmethod
    def setUpClass(cls):
        setup()
        KeyValuePlan().execute()

    @classmethod
    def tearDownClass(cls):
        cleanup()

    def setUp(self):
        self.orm.clear()


#: Range setup
class RangeModel(Model):
    integer = IntRange()
    bigint = BigIntRange()
    date = DateRange()
    numeric = NumericRange()
    timestamp = TimestampRange()
    timestamptz = TimestampTZRange()
    array_integer = Array(IntRange())
    array_bigint = Array(BigIntRange())
    array_date = Array(DateRange())
    array_numeric = Array(NumericRange())
    array_timestamp = Array(TimestampRange())
    array_timestamptz = Array(TimestampTZRange())


class RangePlan(Plan):
    model = RangeModel()


class RangeTestCase(BaseTestCase):
    orm = RangeModel()

    @classmethod
    def setUpClass(cls):
        setup()
        RangePlan().execute()


#: Sequence setup
class SequenceModel(Model):
    enum = Enum('red', 'white', 'blue')
    array_enum = Array(Enum('red', 'white', 'blue'))


class SequencePlan(Plan):
    model = SequenceModel()

    def after(self):
        self.model.array_enum.register_type(db.client)
        self.model.enum.register_type(db.client)


class SequenceTestCase(BaseTestCase):
    orm = SequenceModel()

    @classmethod
    def setUpClass(cls):
        setup()
        SequencePlan().execute()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()
