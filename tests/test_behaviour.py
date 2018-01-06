import pytest
import sqlitent
import collections
from random import random


################################################################################
# # Fixtures
################################################################################

@pytest.fixture
def empty():
    return sqlitent.sqlitent(':memory:')


@pytest.fixture
def point():
    return collections.namedtuple('point', ['x', 'y', 'z'])


@pytest.fixture
def nonempty(empty, point):
    for _ in range(25):
        p = point(random(), random(), random())
        print(p)
        assert empty.add(p) == None

    return empty


################################################################################
# # Testing basic operations
################################################################################

def test_empty(empty):
    assert not len(empty)
    assert list(empty) == []


def test_nonemtpy(nonempty):
    assert len(nonempty)
    assert list(nonempty)


def test_add(empty, point):
    num_entries = len(empty)

    ps = [
        point(random(), random(), random()),
        point(    None, random(), random()),
        point(random(),     None, random()),
        point(random(), random(),     None)
    ]

    for p in ps:
        num_entries = len(empty)
        assert empty.add(p) == None
        assert len(empty) == num_entries + 1
        assert p in empty


def test_remove(nonempty):
    for p in nonempty:
        num_entries = len(nonempty)
        assert p in nonempty
        assert nonempty.remove(p) == None
        assert len(nonempty) == num_entries + -1
        assert p not in nonempty

    assert not len(nonempty)
    assert list(nonempty) == []


################################################################################
# # Testing bulk operations
################################################################################

def test_insert(empty, point):
    num_entries = len(empty)
    assert empty.insert([
        point(random(), random(), random()),
        point(    None, random(), random()),
        point(random(),     None, random()),
        point(random(), random(),     None)
    ]) == None
    assert len(empty) == num_entries + 4


def test_delete(nonempty):
    assert nonempty.delete(list(nonempty)) == None
    assert len(nonempty) == 0
