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
    '''enc_bin = Encrypted(Encrypted.generate_secret(), Binary())
    enc_text = Encrypted(Encrypted.generate_secret(), Text())'''


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
    uid = UID()
    binary_field = Binary()


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
    uid = UID()
    bit_field = Bit(4)
    varbit_field = Varbit(4)


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
            Plan(model=m).execute()


#: KeyValue setup
class KeyValueModel(Model):
    json_field = Json()
    jsonb_field = JsonB()
    hstore_field = HStore()


class KeyValuePlan(Plan):
    model = KeyValueModel()


class KeyValueTestCase(BaseTestCase):
    orm = KeyValueModel()

    @classmethod
    def setUpClass(cls):
        setup()
        KeyValuePlan().execute()

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
    timestamptz = TimestampTZRange()


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
    #enum = Enum(('red', 'white', 'blue'))
    enum = Array()
    arr = Array(Text())


class SequencePlan(Plan):
    model = SequenceModel()


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
