import pytest

from collections import namedtuple
from sqlitent import sqlitent, fuzzy

# # Testing the functionality documented in README.rst
#
# ## Setup

# >>> db = sqlitent('database.sqlite', autocommit=True)
# >>>
# >>> Point = namedtuple('Point', ['x', 'y'])
# >>> p1 = Point(11, y=22)
# >>> p1
# Point(x=11, y=22)
# >>> p2 = p._replace(x=33)
# Point(x=33, y=22)
# >>> Car = namedtuple('Car', [
# ...     'brand',
# ...     'model',
# ...     'configuration',
# ...     'hp',
# ... ])

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
    return sqlitent(':memory:', _types=[Point, Car])


@pytest.fixture
def p1():
    return Point(11, y=22)


@pytest.fixture
def p2():
    return Point(33, 22)


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
#
# >>> db.add(p1)
# >>> db.insert(p1, p2)
# >>> db.insert([p1], [[p1], p2])
# >>> db.insert(c)
# >>> db.insert([c, p1])  # tuples may be of different types


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


def test_add_idempotent(db, p1):
    # sqlitent.add is idempotent
    assert len(db) == 0
    assert p1 not in db
    for _ in range(10):
        assert db.add(p1) == None
        assert p1 in db
        assert len(db) == 1


@pytest.mark.parametrize("other", [int, float, str, bytes, list, dict, object, tuple])
def test_add_signature(db, p1, p2, others):

    # calling with multiple arguments should fail
    with pytest.raises(Exception):
        db.add(p1, p2)

    with pytest.raises(Exception):
        db.add(p2, p1)

    with pytest.raises(Exception):
        db.add(p2, 1)

    # calling with anything other than a namedtuple should fail
    for other in others:
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


def test_insert_idempotent(db, p1, p2, c1):
    # sqlitent.insert is idempotent
    assert len(db) == 0
    assert p1 not in db
    assert p2 not in db
    assert c1 not in db
    for _ in range(10):
        assert db.insert([p1], [], [[], [p2]], c1) == None
        assert p1 in db
        assert p2 in db
        assert c1 in db
        assert len(db) == 3


@pytest.mark.parametrize("other", [int, float, str, bytes, list, dict, object, tuple])
def test_insert_signature(db, p1, c1, others):
    # calling with anything other than a namedtuple should fail
    for other in others:
        with pytest.raises(Exception):
            db.insert(c1, other(), p1)
    for other in others:
        with pytest.raises(Exception):
            db.insert([p1], [], [[], [other()]], c1)


# # Removing Namedtuples
#
# >>> db.remove(p1)
# >>> db.delete(p1, p2)
# >>> db.delete([p1, p2, p1, p2]
# >>> db.delete(p1, [p2, [], [p1]])


def test_remove(populated_db, p1, p2, c1, c2):
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    assert populated_db.remove(p1) = None
    assert p1 not in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 3
    assert populated_db.remove(p2) = None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 2
    assert populated_db.remove(c1) = None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 not in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 1
    assert populated_db.remove(c2) = None
    assert p1 not in populated_db
    assert p2 not in populated_db
    assert c1 not in populated_db
    assert c2 not in populated_db
    assert len(populated_db) == 0


def test_remove_idempotent(populated_db, p1, p2, c1, c2):
    assert p1 in populated_db
    assert p2 in populated_db
    assert c1 in populated_db
    assert c2 in populated_db
    assert len(populated_db) == 4
    for _ in range(10):
        assert populated_db.remove(p1) = None
        assert populated_db.remove(c1) = None
        assert p1 not in populated_db
        assert p2 in populated_db
        assert c1 not in populated_db
        assert c2 in populated_db
        assert len(populated_db) == 2


@pytest.mark.parametrize("other", [int, float, str, bytes, list, dict, object, tuple])
def test_remove_signature(populated_db, p1, c1, others):

    # calling with multiple arguments should fail
    with pytest.raises(Exception):
        populated_db.remove(p1, c1)

    with pytest.raises(Exception):
        populated_db.remove(c1, p1)

    with pytest.raises(Exception):
        populated_db.remove(p1, 1)

    # calling with anything other than a namedtuple should fail
    for other in others:
        with pytest.raises(Exception):
            populated_db.remove(other())


def test_delete_one(populated_db, p1):
    assert p1 in populated_db
    assert len(populated_db) == 4
    assert populated_db.delete(p1) == None
    assert p1 not in populated_db
    assert len(populated_db) == 3


def test_delete_many(populated_db, p1, p2, c1, c2):
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


@pytest.mark.parametrize("other", [int, float, str, bytes, list, dict, object, tuple])
def test_delete_signature(populated_db, p1, p2, c1, c2, others):
    # calling with anything other than a namedtuple should fail
    for other in others:
        with pytest.raises(Exception):
            populated_db.delete(p1, [p2, [c2]], [c1, other()], p1)
