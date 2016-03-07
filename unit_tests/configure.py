import os
import unittest

from bloom import create_db, db, Model as _Model
from bloom.fields import *
from bloom.builder import *


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
            path, pattern='[A-Z]*.py', top_level_dir=None):
        suite.addTests((t for t in test
                        if t not in tests and not tests.append(t)))
    return ut.run(suite)


def setup():
    create_db()
    drop_schema(db, 'bloom_tests', cascade=True, if_exists=True)
    create_schema(db, 'bloom_tests')


def cleanup():
    drop_schema(db, 'bloom_tests', cascade=True, if_exists=True)


class Model(_Model):
    schema = 'bloom_tests'
    uid = UID()


class BaseTestCase(unittest.TestCase):

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
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


class GeoBuilder(Builder):
    model = GeoModel()


class GeoTestCase(BaseTestCase):
    orm = GeoModel()

    @classmethod
    def setUpClass(cls):
        setup()
        GeoBuilder().run()


#: Integer setup
class IntModel(Model):
    integer = Int()
    bigint = BigInt()
    smallint = SmallInt()


class IntBuilder(Builder):
    model = IntModel()


class IntTestCase(BaseTestCase):
    orm = IntModel()

    @classmethod
    def setUpClass(cls):
        setup()
        IntBuilder().run()


#: Character setup
class CharModel(Model):
    char = Char(maxlen=200)
    varchar = Varchar(maxlen=200)
    text = Text()


class CharBuilder(Builder):
    model = CharModel()


class CharTestCase(BaseTestCase):
    orm = CharModel()

    @classmethod
    def setUpClass(cls):
        setup()
        CharBuilder().run()


#: Networking setup
class NetModel(Model):
    ip = IP()
    cidr = Cidr()
    mac = MacAddress()


class NetBuilder(Builder):
    model = NetModel()


class NetTestCase(BaseTestCase):
    orm = NetModel()

    @classmethod
    def setUpClass(cls):
        setup()
        NetBuilder().run()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()


#: Numeric setup
class NumModel(Model):
    num = Numeric()
    dec = Decimal()
    float4 = Float()
    float8 = Double()
    currency = Currency()
    money = Money()


class NumBuilder(Builder):
    model = NumModel()


class NumTestCase(BaseTestCase):
    orm = NumModel()

    @classmethod
    def setUpClass(cls):
        setup()
        NumBuilder().run()


#: Extras setup
class ExtrasModel(Model):
    username = Username()
    email = Email()
    password = Password()
    slug = Slug()
    key = Key()
    enc_bin = Encrypted(Encrypted.generate_secret(), Binary())
    enc_text = Encrypted(Encrypted.generate_secret(), Text())
    enc_int = Encrypted(Encrypted.generate_secret(), Int())
    enc_float = Encrypted(Encrypted.generate_secret(), Float())
    enc_json = Encrypted(Encrypted.generate_secret(), JsonB())


class ExtrasBuilder(Builder):
    model = ExtrasModel()


class ExtrasTestCase(BaseTestCase):
    orm = ExtrasModel()

    @classmethod
    def setUpClass(cls):
        setup()
        ExtrasBuilder().run()


#: Binary setup
class BinaryModel(Model):
    uid = UID()
    binary_field = Binary()


class BinaryBuilder(Builder):
    model = BinaryModel()


class BinaryTestCase(BaseTestCase):
    orm = BinaryModel()

    @classmethod
    def setUpClass(cls):
        setup()
        BinaryBuilder().run()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()


#: Boolean setup
class BooleanModel(Model):
    boolean = Bool()


class BooleanBuilder(Builder):
    model = BooleanModel()


class BooleanTestCase(BaseTestCase):
    orm = BooleanModel()

    @classmethod
    def setUpClass(cls):
        setup()
        BooleanBuilder().run()


#: DateTime setup
class DateTimeModel(Model):
    time = Time()
    ts = Timestamp()
    date = Date()


class DateTimeBuilder(Builder):
    model = DateTimeModel()


class DateTimeTestCase(BaseTestCase):
    orm = DateTimeModel()

    @classmethod
    def setUpClass(cls):
        setup()
        DateTimeBuilder().run()

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


class SerialModel(Model):
    uid = None
    serial = Serial()


class SmallSerialModel(SerialModel):
    serial = SmallSerial()


class BigSerialModel(SerialModel):
    serial = SmallSerial()


class UUIDModel(Model):
    uid = UID(primary=False)
    uuid = UUID()


class IdentifierTestCase(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        setup()
        for m in (UIDModel(), StrUIDModel(), SerialModel(), SmallSerialModel(),
                  BigSerialModel(), UUIDModel()):
            Builder(model=m).run()


#: KeyValue setup
class KeyValueModel(Model):
    json = Json()
    jsonb = JsonB()
    hstore = HStore()


class KeyValueBuilder(Builder):
    model = KeyValueModel()


class KeyValueTestCase(BaseTestCase):
    orm = KeyValueModel()

    @classmethod
    def setUpClass(cls):
        setup()
        KeyValueBuilder().run()

    @classmethod
    def tearDownClass(self):
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


class RangeBuilder(Builder):
    model = RangeModel()


class RangeTestCase(BaseTestCase):
    orm = RangeModel()

    @classmethod
    def setUpClass(cls):
        setup()
        RangeBuilder().run()


#: Sequence setup
class SequenceModel(Model):
    enum = Enum(('red', 'white', 'blue'))
    arr = Array(Text())


class SequenceBuilder(Builder):
    model = SequenceModel()


class SequenceTestCase(BaseTestCase):
    orm = SequenceModel()

    @classmethod
    def setUpClass(cls):
        setup()
        SequenceBuilder().run()

    @classmethod
    def tearDownClass(self):
        cleanup()

    def setUp(self):
        self.orm.clear()
