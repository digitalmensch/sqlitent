import collections.abc
import collections
import sqlite3
import pickle
import itertools
import types

################################################################################
# Helpers
################################################################################

def _sqlname(name):
    ''' Appends a hex-encoded version of the name to itself to distinguish
        between lower and upper case.

        >>> _sqlname('AAA')
        'AAA_414141'
        >>> _sqlname('aaa')
        'aaa_616161'
    '''

    return f'{name}_{name.encode("ascii").hex()}'


def _sqltype(_type):
    ''' Returns the corresponding Sqlite datatype given a Python type.

        >>> _sqltype(int)
        'INTEGER'
        >>> _sqltype(object)
        'BLOB'
    '''

    if int   == _type: return 'INTEGER'
    if float == _type: return 'REAL'
    if str   == _type: return 'TEXT'
    if bytes == _type: return 'BLOB'
    return 'BLOB'


def _identity(something):
    return something


def _istrivial(val):
    return val  in (type(None), int, float, str, bytes) or \
      type(val) in (type(None), int, float, str, bytes)


def _flatten(it):
    if hasattr(type(it), '__bases__') and tuple in type(it).__bases__ and hasattr(it, '_fields'):
        yield it
    elif isinstance(it, (tuple, list, set)):
        yield from itertools.chain.from_iterable(map(_flatten, it))
    else:
        raise Exception('datatype!')


################################################################################
# sqlitent API
################################################################################

class sqlitent(collections.abc.Collection):

    def __init__(self, database, encode=pickle.dumps, decode=pickle.loads):
        self.__db = sqlite3.connect(database)

        # We need to keep track of recognized namedtuples and how to encode
        # and decode them.
        self.__tupletypes = set()
        self.__encoder = {}
        self.__decoder = {}

        # We cache frequently used SQL statements
        self.__insert_stmt = {}
        self.__select_stmt = {}
        self.__delete_stmt = {}
        self.__count_stmt = {}

    def __register(self, tupletype, fields):
        # Registers a namedtuple with the database. All fields in the tupletype
        # need to be mapped to a trivial type in fields.
        print(self)
        print(tupletype)
        print(tupletype.__bases__)
        print(fields)

        # Is tupletupe really a namedtuple?
        assert tuple in tupletype.__bases__, 'expected namedtuple'
        assert hasattr(tupletype, '_fields'), 'expected namedtuple'
        assert hasattr(tupletype, '_make'), 'expected namedtuple'
        assert hasattr(tupletype, '_source'), 'expected namedtuple'
        assert hasattr(tupletype, '_replace'), 'expected namedtuple'
        assert hasattr(tupletype, '_asdict'), 'expected namedtuple'
        assert all(hasattr(tupletype, n) for n in tupletype._fields), 'expected namedtuple'

        # We require that all fields in the namedtuple are typed to create a
        # typed database table and handle encoding complex types.
        assert all(f in fields for f in tupletype._fields), 'untyped field(s)'

        fields = collections.OrderedDict([(f, fields[f]) for f in tupletype._fields])

        encs = [_identity if _istrivial(t) else self.__encode for t in fields.values()]
        def _encode(tup): return tuple(enc(v) for enc, v in zip(encs, tup))
        self.__encoder[tupletype] = _encode

        decs = [_identity if _istrivial(t) else self.__decode for t in fields.values()]
        def _decode(tup): return tupletype._make(dec(v) for dec, v in zip(decs, tup))
        self.__decoder[tupletype] = _decode

        self.__insert_stmt[tupletype] = self.__build_insert_stmt(tupletype.__name__, fields.keys())
        self.__select_stmt[tupletype] = self.__build_select_stmt(tupletype.__name__, fields.keys())
        self.__delete_stmt[tupletype] = self.__build_delete_stmt(tupletype.__name__, fields.keys())
        self.__count_stmt[tupletype] = self.__build_count_stmt(tupletype.__name__)
        self.__execute(self.__build_create_table_stmt(tupletype.__name__, fields))

        # If we get to this point, everything is ready to deal instances of
        # tupletype. Hence tupletype is added to the recognized namedtuples.
        self.__tupletypes.add(tupletype)

    def __build_insert_stmt(self, name, fieldnames):
        cols = ','.join(map(_sqlname, fieldnames))
        gaps = ','.join('?' for _ in fieldnames)
        return f'INSERT OR IGNORE INTO {_sqlname(name)} ({cols}) VALUES ({gaps});'

    def __build_select_stmt(self, name, fieldnames=[]):
        clauses = ' AND '.join(f'{_sqlname(f)} IS ?' for f in fieldnames)
        where = f' WHERE {clauses}' if clauses else ''
        return f'SELECT * FROM {_sqlname(name)}{where};'

    def __build_delete_stmt(self, name, fieldnames=[]):
        clauses = ' AND '.join(f'{_sqlname(f)} IS ?' for f in fieldnames)
        where = f' WHERE {clauses}' if clauses else ''
        return f'DELETE FROM {_sqlname(name)}{where};'

    def __build_count_stmt(self, name):
        return f'SELECT count(*) FROM {_sqlname(name)};'

    def __build_create_table_stmt(self, name, fieldtypes):
        defs = ','.join(f'{_sqlname(f)} {_sqltype(v)}' for f, v in fieldtypes.items())
        cols = ','.join(map(_sqlname, fieldtypes.keys()))
        return f'CREATE TABLE IF NOT EXISTS {_sqlname(name)} ({defs}, UNIQUE ({cols}));'

    def __assert_registered(self, tupletype):
        if tupletype not in self.__tupletypes:
            raise Exception(f'unknown tupletype: {tupletype}')

    def __execute(self, stmt, *args, **kwargs):
        cur = self.__db.cursor().execute(stmt, *args, **kwargs)
        self.__db.commit()
        return cur

    def __contains__(self, nt):
        if type(nt) not in self.__tupletypes:
            return False
        return bool(list(self.__execute(self.__select_stmt[type(nt)], nt)))

    def __iter__(self):
        return itertools.chain.from_iterable(
            map(self.__decoder[t], self.__execute(self.__build_select_stmt(t.__name__)))
            for t in self.__tupletypes
        )

    def __len__(self):
        return sum(self.__execute(self.__count_stmt[t]).fetchone()[0] for t in self.__tupletypes)

    def add(self, nt):
        ''' Add a namedtuple to the database. Registers the namedtuple class
            with the database if necessary.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> p = Pal('Jim', 35)
            >>> db.remove(p)
        '''

        tupletype = type(nt)
        if tupletype not in self.__tupletypes:
            self.__register(tupletype, {f: type(v) for f, v in nt._asdict().items()})
        if None in nt and nt in self:
            return # abort if exists, because Sqlite's NULL isn't unique
        self.__execute(self.__insert_stmt[tupletype], nt)

    def insert(self, *nts):
        ''' Insert one or more namedtuples to the database.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> p = Pal('Jim', 35)
            >>> db.remove(p)
        '''

        for tup in set(_flatten(nts)):
            self.add(tup)

    def remove(self, nt):
        ''' Remove one matching namedtuple from the database.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> p = Pal('Jim', 35)
            >>> db.remove(p)
        '''

        tupletype = type(nt)
        self.__assert_registered(tupletype)
        self.__execute(self.__delete_stmt[tupletype], nt)

    def delete(self, *nts):
        ''' Remove one or more namedtuples from the database.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> p = Pal('Jim', 35)
            >>> db.delete([p, p])
        '''

        for tup in set(_flatten(nts)):
            self.remove(tup)

    def one(self, tupletype, **kwargs):
        ''' Return one matching namedtuple or None.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> p = Pal('Jim', 35)
            >>> db.one(Pal, name='Jim')
            Pal('Jim', 35)
        '''

        self.__assert_registered(tupletype)
        for tup in self.many(tupletype, **kwargs):
            return tup
        return None

    def pop(self, tupletype, **kwargs):
        ''' Return one matching namedtuple or None and remove the returned
            namedtuple from the database.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> p = Pal('Jim', 35)
            >>> db.pop(Pal, name='Jim')
            Pal('Jim', 35)
        '''

        self.__assert_registered(tupletype)
        tup = self.one(tupletype, **kwargs)
        if tup is not None:
            self.remove(tup)
        return tup

    def many(self, tupletype, **kwargs):
        ''' Return zero or more matching namedtuples.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> db.many(Pal, name='Jim')
            [...]
        '''

        if not all(k in tupletype._fields for k in kwargs):
            raise Exception(f'{tupletype} doesn\'t have one of your keywords')
        if tupletype not in self.__tupletypes:
            raise Exception(f'unknown tupletype: {tupletype}')

        sqlargs = []
        sqlvals = []
        filters = []
        for field, value in sorted(kwargs.items()):
            if isinstance(value, types.FunctionType):
                filters.append(lambda t: value(getattr(t, field)))
            else:
                sqlargs.append(field)
                sqlvals.append(value)

        stmt = self.__build_select_stmt(tupletype.__name__, sqlargs)
        it = self.__execute(stmt, sqlvals)

        it = map(self.__decoder[tupletype], it)
        for fn in filters:
            it = filter(fn, it)
            print(fn, it)

        yield from it

    def popmany(self, tupletype, **kwargs):
        ''' Return zero or more matching namedtuples and removes them
            from the database.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> db.popmany(Pal, name='Jim')
            [...]
        '''

        tups = list(self.many(tupletype, **kwargs))
        self.delete(tups)
        return tups
