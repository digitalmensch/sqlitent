sqlitent - namedtuples inside sqlite databases
==============================================

**THIS PROJECT IS CURRENTLY A STUB WITHOUT CODE**

sqlitent provides a set-like interface on top of SQLite_. Values can
be None, int, float, str, and bytes. Tuple types are distinguished by
name (case sensitive!) and number of fields. Documentation_.

sqlitent was inspired by sqlitedict_ and kv_.

Example
-------

::

    >>> from collections import namedtuple
    >>> from sqlitent import sqlitent, fuzzy
    >>> db = sqlitent('database.sqlite', autocommit=True)
    >>>
    >>> Point = namedtuple('Point', ['x', 'y'])
    >>> p1 = Point(11, y=22)
    >>> p1
    Point(x=11, y=22)
    >>> p2 = p._replace(x=33)
    Point(x=33, y=22)
    >>> Car = namedtuple('Car', [
    ...     'brand',
    ...     'model',
    ...     'configuration',
    ...     'hp',
    ... ])
    >>> c = Car('Audi', 'A1', 'Sport 1.8 TFSI S tronic', 192)
    >>> c
    Car(brand='Audi', model='A1', configuration='Sport 1.8 TFSI S tronic', hp=192)
    >>> db.register(Point, Car)  # necessary to read tuples

In addition to the methods which the set type provides, there are convenience
methods (insert, delete) that handle multiple tuples at once.

::

    >>> db.add(p1)
    >>> db.remove(p1)
    >>> db.insert(p1, p2)
    >>> db.delete(p1, p2)
    >>> db.insert([p1], [[p1], p2])
    >>> db.insert(c)
    >>> db.insert([c, p1])  # tuples may be of different types

Of course, sqlitent also supports membership checking and counting:

::

    >>> d = c._replace(configuration='1.6 TDI S tronic', hp=116)
    >>> d in db
    False
    >>> len(db)
    3
    >>> db.insert(d)
    >>> d in db
    True
    >>> len(db)  # ...now we have 4 unique tuples in the database.
    4

There are various ways to retreive tuples from the database:

::

    >>> p3 = db.one(Point)  # not deterministic
    >>> p3
    Point(x=11, y=22)
    >>> p3 = db.one(p3)  # deterministic (since p3 is fully specified)
    >>> p3 in db
    True
    >>> db.pop(p3)  # remove and return
    Point(x=11, y=22)
    >>> p3 in db
    False
    >>> db.pop(Point)  # not deterministic
    Point(x=33, y=22)
    >>> db.pop(Point)  # returns None since there are no more Point tuples
    >>>

Obviously there are also functions to retrieve or pop multiple values out
of the database. sqlitent also supports fuzzy matching on text fields and
filter functions:

::

    >>> cs = db.many(Car)  # get all cars
    >>> cs
    <generator object _sqlitent_iter at 0x10f39bb48>
    >>> list(cs)
    [Car(brand='Audi', model='A1', configuration='Sport 1.8 TFSI S tronic', hp=192),
     Car(brand='Audi', model='A1', configuration='1.6 TDI S tronic', hp=116)]
    >>> d = list(db.popmany(Car, configuration=fuzzy('%TDI%')))
    >>> any(x in db for x in d)  # we removed all TDI cars from the database
    False
    >>> list(db.many(Car, hp=lambda v: v > 150))
    [Car(brand='Audi', model='A1', configuration='Sport 1.8 TFSI S tronic', hp=192)]
    >>> list(db.many(Car, brand='Kia'))
    []

There is a locking facility that uses SQLite's transaction API:

::

    >>> with db.lock():
    ...     p = db.pop(Point, x=11)
    ...     db.add(p._replace(x=p.x+1))

The code is MIT licensed.

.. _Sqlite: https://sqlite.org/
.. _Documentation: https://digitalmensch.github.io/sqlitent/
.. _sqlitedict: https://github.com/RaRe-Technologies/sqlitedict
.. _kv: https://github.com/mgax/kv
.. _code: https://github.com/digitalmensch/sqlitent
