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


# ## Adding Namedtuples


def test_add(db, p1, p2):
    # this is the intended usage:
    # sqlitent.add takes one namedtuple and returns None
    assert len(db) == 0
    assert p1 not in db
    assert p2 not in db
    assert db.add(p1) == None
    assert p1 in db
    assert p2 not in db
    assert len(db) == 1
    assert db.add(p2) == None
    assert len(db) == 2
    assert p1 in db
    assert p2 in db


def test_add_idempotent(db, p1, p3):
    # sqlitent.add is idempotent
    assert len(db) == 0
    assert p1 not in db
    assert p3 not in db
    for _ in range(10):
        assert db.add(p1) == None
        assert db.add(p3) == None
        assert p1 in db
        assert p3 in db
        assert len(db) == 2


def test_add_signature(db, p1, p2):

    # calling with multiple arguments should fail
    with pytest.raises(Exception):
        db.add(p1, p2)

    with pytest.raises(Exception):
        db.add(p2, p1)

    with pytest.raises(Exception):
        db.add(p2, 1)

    # calling with anything other than a namedtuple should fail
    for other in [int, float, str, bytes, object]:
        with pytest.raises(Exception):
            db.add(other())


def test_insert_one(db, p1, p2):
    # sqlitent.insert can take one namedtuple and returns None
    assert len(db) == 0
    assert p1 not in db
    assert p2 not in db
    assert db.insert(p1) == None
    assert len(db) == 1
    assert p1 in db
    assert p2 not in db
    assert db.insert(p2) == None
    assert len(db) == 2
    assert p1 in db
    assert p2 in db


def test_insert_many(db, p1, p2):
    # sqlitent.insert can take multiple namedtuple and returns None
    assert len(db) == 0
    assert p1 not in db
    assert p2 not in db
    assert db.insert(p1, p2) == None
    assert len(db) == 2
    assert p1 in db
    assert p2 in db


def test_insert_list(db, p1, p2):
    # sqlitent.insert can take list(s) of namedtuples and returns None
    assert len(db) == 0
    assert p1 not in db
    assert p2 not in db
    assert db.insert([p1, p2]) == None
    assert len(db) == 2
    assert p1 in db
    assert p2 in db


def test_insert_mixed(db, p1, p2, c1, c2):
    # sqlitent.insert can take lists and single namedtuples of many
    # types in any combination and returns None
    assert len(db) == 0
    assert p1 not in db
    assert p2 not in db
    assert c1 not in db
    assert c2 not in db
    assert db.insert(c1, [p1, [c2]], p2, [], p2) == None
    assert p1 in db
    assert p2 in db
    assert c1 in db
    assert c2 in db
    assert len(db) == 4


def test_insert_idempotent(db, p1, p3, c1):
    # sqlitent.insert is idempotent
    assert len(db) == 0
    assert p1 not in db
    assert p3 not in db
    assert c1 not in db
    for _ in range(10):
        assert db.insert([p1], [], [[], [p3]], c1) == None
        assert p1 in db
        assert p3 in db
        assert c1 in db
        assert len(db) == 3


def test_insert_signature(db, p1, c1):
    # calling with anything other than a namedtuple should fail
    for other in [int, float, str, bytes, object]:
        with pytest.raises(Exception):
            db.insert(c1, other(), p1)
    for other in [int, float, str, bytes, object]:
        with pytest.raises(Exception):
            db.insert([p1], [], [[], [other()]], c1)


# ## Removing Namedtuples


def test_remove(populated_db, p1, p2, c1, c2):
    # remove takes namedtuples out of the database
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    assert populated_db.remove(p1) == None
    assert p1 not in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 3
    assert populated_db.remove(p2) == None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 2
    assert populated_db.remove(c1) == None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 not in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 1
    assert populated_db.remove(c2) == None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 not in populated_db
    assert c2 not in populated_db
    assert len(populated_db) == 0


def test_remove_idempotent(populated_db, p1, p2, c1, c2):
    # remove is idempotent
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    for _ in range(10):
        assert populated_db.remove(p1) == None
        assert populated_db.remove(c1) == None
        assert p1 not in populated_db
        assert p2 in populated_db
        assert c1 not in populated_db
        assert c2 in populated_db
        assert len(populated_db) == 2


def test_remove_signature_num_args(populated_db, p1, c1):
    # calling with multiple arguments should fail
    with pytest.raises(Exception):
        assert populated_db.remove(p1, c1) == None

    with pytest.raises(Exception):
        assert populated_db.remove(c1, p1) == None

    with pytest.raises(Exception):
        assert populated_db.remove(p1, 1) == None


def test_remove_signature_types(populated_db):
    # calling with anything other than a namedtuple should fail
    for other in [int, float, str, bytes, object]:
        with pytest.raises(Exception):
            assert populated_db.remove(other()) == None


def test_remove_unknown_namedtuple(populated_db):
    # removing unknown namedtuples should not do anything
    Xyz = namedtuple('Xyz', ['a', 'b'])
    for n in range(10):
        assert populated_db.remove(Xyz(n, n+1)) == None


def test_delete_one(populated_db, p1):
    # delete may be called with a single namedtuple
    assert p1 in populated_db
    assert len(populated_db) == 4
    assert populated_db.delete(p1) == None
    assert p1 not in populated_db
    assert len(populated_db) == 3


def test_delete_many(populated_db, p1, p2, c1, c2):
    # delete may be called with a variable number of namedtuples
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    assert populated_db.delete(p1, p2, c1, c2) == None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 not in populated_db
    assert c2 not in populated_db
    assert len(populated_db) == 0


def test_delete_list(populated_db, p1, p2, c1, c2):
    # delete may be called with lists of namedtuples
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    assert populated_db.delete([p1], [p2, c1], [c2]) == None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 not in populated_db
    assert c2 not in populated_db
    assert len(populated_db) == 0


def test_delete_mixed(populated_db, p1, p2, c1, c2):
    # delete may be called with a mix of single namedtuples
    # and lists of namedtuples
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    assert populated_db.delete([[p1], [[], [[c1]]]], c2, [], p2) == None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 not in populated_db
    assert c2 not in populated_db
    assert len(populated_db) == 0


def test_delete_idempotent(populated_db, p1, p2, c1, c2):
    # delete is idempotent
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    for _ in range(10):
        assert populated_db.delete([[p1], [[], [[c1]]]], c2, [], p2) == None
        assert p1 not in populated_db
        assert p2 not in populated_db
        assert c1 not in populated_db
        assert c2 not in populated_db
        assert len(populated_db) == 0


def test_delete_signature(populated_db, p1, p2, c1, c2):
    # calling delete with anything other than a namedtuple should fail
    for other in [int, float, str, bytes, object]:
        with pytest.raises(Exception):
            populated_db.delete(p1, [p2, [c2]], [c1, other()], p1)


# ## Getting namedtuples


def test_iter(populated_db, p1, p2, p3, c1, c2):
    xs = [p1, p2, p3, c1, c2]
    assert populated_db.insert(xs) == None
    n = len(populated_db)
    assert 5 == n
    assert populated_db.insert(xs) == None
    assert len(populated_db) == n
    for x in xs:
        assert x in populated_db
    for x in populated_db:
        assert x in xs
