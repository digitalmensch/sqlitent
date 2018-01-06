import pytest


# # Testing the functionality documented in README.rst


from collections import namedtuple
from sqlitent import sqlitent


# ## Setup


Point = namedtuple('Point', ['x', 'y'])
Car = namedtuple('Car', [
    'brand',
    'model',
    'configuration',
    'hp',
])


# ## Fixtures


@pytest.fixture
def db():
    return sqlitent(':memory:')


@pytest.fixture
def p1():
    return Point(11, y=22)


@pytest.fixture
def p2():
    return Point(33, 22)

@pytest.fixture
def p3():
    return Point(None, None)


@pytest.fixture
def c1():
    return Car('Audi', 'A1', 'Sport 1.8 TFSI S tronic', 192)


@pytest.fixture
def c2():
    return Car('Audi', 'A1', '1.6 TDI S tronic', 116)


@pytest.fixture
def populated_db(db, p1, p2, c1, c2):
    db.insert(p1, p2, c1, c2)
    return db


# ## Querying for namedtuples


def test_many_unknown_argument(populated_db):
    with pytest.raises(Exception):
        list(populated_db.many(Point, partytime='now'))


def test_many_missing_tupletype(populated_db):
    with pytest.raises(Exception):
        list(populated_db.many())


def test_many_unknown_tuple(populated_db):
    assert list(populated_db.many(Point, x=-1, y=-1)) == []


def test_many_type(populated_db):
    p = list(populated_db.many(Point))
    assert len(p) == 2
    assert isinstance(p[0], Point)
    assert isinstance(p[1], Point)
    assert p[0] in populated_db
    assert p[1] in populated_db
    c = list(populated_db.many(Car))
    assert len(c) == 2
    assert isinstance(c[0], Car)
    assert isinstance(c[1], Car)
    assert c[0] in populated_db
    assert c[1] in populated_db


def test_many_criteria(populated_db):
    p1 = list(populated_db.many(Point, x=11))
    p2 = list(populated_db.many(Point, x=33))
    assert len(p1) == 1
    assert len(p2) == 1
    assert isinstance(p1[0], Point)
    assert isinstance(p2[0], Point)
    assert p1[0] is not p2[0]
    assert p1[0] != p2[0]
    assert p1[0] in populated_db
    assert p2[0] in populated_db
    c1 = list(populated_db.many(Car, hp=192))
    c2 = list(populated_db.many(Car, hp=116))
    assert len(c1) == 1
    assert len(c2) == 1
    assert isinstance(c1[0], Car)
    assert isinstance(c2[0], Car)
    assert c1[0] is not c2[0]
    assert c1[0] != c2[0]
    assert c1[0] in populated_db
    assert c2[0] in populated_db


def test_popmany(populated_db):
    p1 = list(populated_db.many(Point, x=11))
    p2 = list(populated_db.many(Point, x=33))
    assert len(p1) == 1
    assert len(p2) == 1
    assert isinstance(p1[0], Point)
    assert isinstance(p2[0], Point)
    assert p1[0] is not p2[0]
    assert p1[0] != p2[0]
    assert p1[0] in populated_db
    assert p2[0] in populated_db
    assert populated_db.popmany(Point, x=11) == p1
    assert len(p1) == 1
    assert p1[0] not in populated_db
    assert p2[0] in populated_db
    c1 = list(populated_db.many(Car, hp=192))
    c2 = list(populated_db.many(Car, hp=116))
    assert len(c1) == 1
    assert len(c2) == 1
    assert isinstance(c1[0], Car)
    assert isinstance(c2[0], Car)
    assert c1[0] is not c2[0]
    assert c1[0] != c2[0]
    assert c1[0] in populated_db
    assert c2[0] in populated_db
    assert populated_db.popmany(Car, hp=192) == c1
    assert len(c1) == 1
    assert c1[0] not in populated_db
    assert c2[0] in populated_db


def test_popmany_unknown_argument(populated_db):
    with pytest.raises(Exception):
        list(populated_db.popmany(Point, partytime='now'))


def test_popmany_missing_tupletype(populated_db):
    with pytest.raises(Exception):
        list(populated_db.popmany())


def test_popmany_unknown_tuple(populated_db):
    assert len(populated_db) == 4
    assert list(populated_db.popmany(Point, x=-1, y=-1)) == []
    assert len(populated_db) == 4


def test_popmany_type(populated_db):
    assert len(populated_db) == 4
    ps = populated_db.popmany(Point)
    assert len(ps) == 2
    assert len(populated_db) == 2
    cs = populated_db.popmany(Car)
    assert len(cs) == 2
    assert len(populated_db) == 0


def test_one_unknown_argument(populated_db):
    with pytest.raises(Exception):
        populated_db.one(Point, partytime='now')


def test_one_missing_tupletype(populated_db):
    with pytest.raises(Exception):
        populated_db.one()


def test_one_unknown_tuple(populated_db):
    assert len(populated_db) == 4
    assert populated_db.one(Point, x=-1, y=-1) is None
    assert len(populated_db) == 4


def test_pop_unknown_argument(populated_db):
    with pytest.raises(Exception):
        populated_db.pop(Point, partytime='now')


def test_pop_missing_tupletype(populated_db):
    with pytest.raises(Exception):
        populated_db.pop()


def test_pop_unknown_tuple(populated_db):
    assert len(populated_db) == 4
    assert populated_db.pop(Point, x=-1, y=-1) is None
    assert len(populated_db) == 4


def test_one_type(populated_db):
    p1 = populated_db.one(Point)
    assert p1 is not None
    assert p1 in populated_db
    c1 = populated_db.one(Car)
    assert c1 is not None
    assert c1 in populated_db


def test_pop_type(populated_db):
    p1 = populated_db.pop(Point)
    assert p1 is not None
    assert p1 not in populated_db
    c1 = populated_db.pop(Car)
    assert c1 is not None
    assert c1 not in populated_db


def test_one_criteria(populated_db):
    p1 = populated_db.one(Point, y=22)
    assert p1 is not None
    assert 22 == p1.y
    assert p1 in populated_db
    c1 = populated_db.one(Car, hp=116)
    assert c1 is not None
    assert 116 == c1.hp
    assert c1 in populated_db


def test_pop_criteria(populated_db):
    p1 = populated_db.pop(Point, y=22)
    assert p1 is not None
    assert 22 == p1.y
    assert p1 not in populated_db
    c1 = populated_db.pop(Car, hp=116)
    assert c1 is not None
    assert 116 == c1.hp
    assert c1 not in populated_db

