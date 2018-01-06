import collections.abc
import sqlite3
import pickle
import itertools
import types


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

    def __isnamedtuple(self, nt):
        return isinstance(nt, tuple) and hasattr(nt, '_fields') and \
                   all(hasattr(nt, n) for n in nt._fields) and \
                   hasattr(nt, '_source') and hasattr(nt, '_replace') and \
                   hasattr(nt, '_asdict')

    def __sqlname(self, name):
        # SQL is case insensitive. Append the hex encoded value for uniqueness.
        unique = name.encode('ascii').hex()
        return f'{name}_{unique}'

    def __sqltype(self, nt_type):
        if int   == nt_type: return 'INTEGER'
        if float == nt_type: return 'REAL'
        if str   == nt_type: return 'TEXT'
        if bytes == nt_type: return 'BLOB'
        return ''

    def __tablename(self, nt):
        return self.__sqlname(type(nt).__name__)

    def __execute(self, stmt, *args, **kwargs):
        return self.__db.cursor().execute(stmt, *args, **kwargs)

    def __setuptable(self, nt):
        table = self.__tablename(nt)
        nt_type = type(nt)
        fields = ','.join(self.__sqlname(field) for field in nt._fields)

        # build and run the create table statement
        stmt = ','.join(
            f'{self.__sqlname(f)} {self.__sqltype(type(getattr(nt, f)))}'
            for f in nt._fields
        )
        stmt = f'CREATE TABLE IF NOT EXISTS {table} ({stmt}, UNIQUE ({fields}));'
        self.__execute(stmt)
        self.__db.commit()

        # build and cache the insert statement
        stmt = f'INSERT OR IGNORE INTO {table} ({fields})'
        stmt += 'VALUES (' + ','.join(['?'] * len(nt)) + ');'
        self.__insert_cache[nt_type] = stmt

        # build and cache the select statement for all tuples
        self.__select_cache[nt_type] = f'SELECT {fields} FROM {table};'

        # build and cache the select statement for a fully specified tuple
        stmt = ' AND '.join(f'{self.__sqlname(f)} IS ?' for f in nt._fields)
        stmt = f'SELECT {fields} FROM {table} WHERE {stmt};'
        self.__single_cache[nt_type] = stmt

        # build and cache the select count(*) statement
        self.__count_cache[nt_type] = f'SELECT count(*) FROM {table};'

        # build and cache the delete statement for a fully specified tuple
        stmt = ' AND '.join(f'{self.__sqlname(f)} IS ?' for f in nt._fields)
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
        if not self.__isnamedtuple(nt):
            raise Exception(f'expected namedtuple, instead got {type(nt)}: {nt}')
        nt_type = type(nt)
        if nt_type not in self.__insert_cache:
            self.__setuptable(nt)
        return bool(list(self.__execute(self.__single_cache[nt_type], nt)))

    def __iter__(self):
        return itertools.chain.from_iterable(
            map(nt_type._make, self.__execute(stmt))
            for nt_type, stmt in self.__select_cache.items()
        )

    def __len__(self):
        return sum(
            self.__execute(stmt).fetchone()[0] for stmt in self.__count_cache.values()
        )

    def add(self, nt):
        if not self.__isnamedtuple(nt):
            raise Exception(f'expected namedtuple, instead got {type(nt)}: {nt}')
        nt_type = type(nt)
        if nt_type not in self.__insert_cache:
            self.__setuptable(nt)
        if None in nt and nt in self:
            # abort if exists, because NULL doesn't violate uniqueness in Sqlite
            return
        self.__execute(self.__insert_cache[nt_type], nt)
        self.__db.commit()

    def insert(self, *nts):
        tmp = set(self.__to_ntlist(nts))
        for nt in tmp:
            self.add(nt)

    def remove(self, nt):
        if not self.__isnamedtuple(nt):
            raise Exception(f'expected namedtuple, instead got {type(nt)}: {nt}')
        nt_type = type(nt)
        if nt_type in self.__delete_cache:
            self.__execute(self.__delete_cache[nt_type], nt)
            self.__db.commit()

    def delete(self, *nts):
        tmp = set(self.__to_ntlist(nts))
        for nt in tmp:
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

    def many(self, nt_type, **kwargs):
        if not all(k in nt_type._fields for k in kwargs):
            raise Exception(f'{nt_type} doesn\'t have one of your keywords')

        clauses = []
        sqlparams = []
        filters = []
        for field, value in sorted(kwargs.items()):
            if isinstance(value, types.FunctionType):
                filters.append(lambda t: value(getattr(t, field)))
            else:
                clauses.append(f'{self.__sqlname(field)} IS ?')
                sqlparams.append(value)

        table = self.__sqlname(nt_type.__name__)
        stmt = f'SELECT * FROM {table}' + (' WHERE ' + ' AND '.join(clauses) if clauses else '') + ';'

        it = self.__execute(stmt, sqlparams)
        print(it)
        it = map(nt_type._make, it)
        print(it)
        for fn in filters:
            it = filter(fn, it)
            print(fn, it)

        yield from it

    def popmany(self, nt_type, **kwargs):
        tmp = list(self.many(nt_type, **kwargs))
        self.delete(tmp)
        return tmp
