from cargo.expressions import _empty, parameterize, Expression, Function,\
                              Clause
from vital.debug import Compare, Timer, RandData


def p1(inp):
    parameterize(inp)


def p2(inp):
    Expression(1, '=', 1)


def p3(inp):
    Function('generate_id', 1, 2)


def p4(inp):
    Clause('values', 1, 2)


c = Compare(p1, p2, p3, p4)
c.time(1000000, 'one')
