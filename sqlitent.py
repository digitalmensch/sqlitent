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
    return val in (type(None), int, float, str, bytes)


################################################################################
# sqlitent API
################################################################################

class sqlitent(collections.abc.Collection):

    def __init__(self, database, encode=pickle.dumps, decode=pickle.loads):
        self.__db = sqlite3.connect(database)
        self.__insert_cache = {}
        self.__select_cache = {}
        self.__single_cache = {}
        self.__count_cache = {}
        self.__delete_cache = {}
        self.__encode = encode
        self.__decode = decode
        self.__tup = set()

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

    def __register(self, tupletype, **fields):
        ''' Register a namedtuple with the database. Creates a table in the
            database if necessary and prepares all required SQL statements.
            >>> Pal = collections.namedtuple('Pal', ['name', 'age'])
            >>> db = sqlitent(':memory:')
            >>> db.register(Pal, name=str, age=int)
            >>>
        '''

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

    def __isnamedtuple(self, nt):
        return isinstance(nt, tuple) and hasattr(nt, '_fields') and \
                   all(hasattr(nt, n) for n in nt._fields) and \
                   hasattr(nt, '_source') and hasattr(nt, '_replace') and \
                   hasattr(nt, '_asdict')

    def __assert_is_namedtuple(self, nt):
        if not self.__isnamedtuple(nt):
            raise Exception(f'expected namedtuple, instead got {type(nt)}: {nt}')

    def __tablename(self, nt):
        return _sqlname(type(nt).__name__)

    def __execute(self, stmt, *args, **kwargs):
        print(stmt)
        print(args)
        print(kwargs)
        return self.__db.cursor().execute(stmt, *args, **kwargs)

    def __setuptable(self, nt):
        self.__register(type(nt), **dict((f, type(v)) for f, v in zip(nt._fields, nt)))

        table = self.__tablename(nt)
        nt_type = type(nt)
        fields = ','.join(_sqlname(field) for field in nt._fields)

        # build and cache the insert statement
        stmt = f'INSERT OR IGNORE INTO {table} ({fields})'
        stmt += 'VALUES (' + ','.join(['?'] * len(nt)) + ');'
        self.__insert_cache[nt_type] = stmt

        # build and cache the delete statement for a fully specified tuple
        stmt = ' AND '.join(f'{_sqlname(f)} IS ?' for f in nt._fields)
        stmt = f'DELETE FROM {table} WHERE {stmt};'
        self.__delete_cache[nt_type] = stmt

    def __to_ntlist(self, it):
        def _flatten(it):
            if isinstance(it, str) or isinstance(it, bytes):
                # these need to be handled first because they cause infinite recursion
                raise Exception(f'expected namedtuple, instead got {type(it)}: {it}')
            elif isinstance(it, collections.abc.Iterable):
                if self.__isnamedtuple(it):
                    yield it
                else:
                    yield from itertools.chain.from_iterable(map(_flatten, it))
            else:
                yield it
        for value in _flatten(it):
            if not self.__isnamedtuple(value):
                raise Exception(f'expected namedtuple, instead got {type(value)}: {value}')
            yield value

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
        self.__assert_is_namedtuple(nt)
        tupletype = type(nt)
        if tupletype not in self.__tupletypes:
            print('========= NEW =========')
            print(tupletype)
            print(nt)
            self.__register(tupletype, **{f: type(v) for f, v in nt._asdict().items()})
        if None in nt and nt in self:
            # abort if exists, because NULL doesn't violate uniqueness in Sqlite
            return
        self.__execute(self.__insert_stmt[tupletype], nt)
        self.__db.commit()

    def insert(self, *nts):
        tmp = set(self.__to_ntlist(nts))
        for nt in tmp:
            self.add(nt)

    def remove(self, nt):
        tupletype = type(nt)
        if tupletype not in self.__tupletypes:
            raise Exception(f'unknown tupletype: {tupletype}')
        self.__execute(self.__delete_stmt[tupletype], nt)

    def delete(self, *nts):
        for nt in set(self.__to_ntlist(nts)):
            self.remove(nt)

    def one(self, nt_type, **kwargs):
        for nt in self.many(nt_type, **kwargs):
            return nt
        return None

    def pop(self, nt_type, **kwargs):
        tmp = self.one(nt_type, **kwargs)
        if tmp is not None:
            self.remove(tmp)
        return tmp

    def many(self, tupletype, **kwargs):
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

    def popmany(self, nt_type, **kwargs):
        tmp = list(self.many(nt_type, **kwargs))
        self.delete(tmp)
        return tmp
