# distutils: language = c++
# distutils: sources = koalas/io/_csv_reader.cpp

import numpy as np
cimport numpy as np
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from collections import deque
from libcpp cimport bool
from libcpp.string cimport string
import json

cdef extern from "_csv_reader.hpp" namespace "koalas":

    cdef cppclass _Field:
        Py_UNICODE* s
        int length

    cdef cppclass _CsvReader:
        _CsvReader(const _CsvDialect* dialect_) except +
        _CsvChunk* read_chunk(const Py_UNICODE* buff, const int buffer_length, bool last_chunk)
        Py_UNICODE* remaining
        size_t remaining_length

    cdef cppclass _CsvChunk:
        const _Field* get(int i, int j) const
        int nb_rows() const
        int nb_columns() const
        bool ok() const
        string error_msg

    cdef cppclass _CsvDialect:
        _CsvDialect() except +
        Py_UNICODE delimiter
        Py_UNICODE quotechar
        Py_UNICODE escapechar
        bool doublequote
        # bool skipinitialspace;
        # LINE_TERMINATOR lineterminator;
        # Quoting quoting;


cdef class CsvDialect:

    cdef _CsvDialect* _csv_dialect

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __cinit__(self,):
        self._csv_dialect = new _CsvDialect()

    cdef _CsvDialect* get_csv_dialect(self,):
        return self._csv_dialect

    property quotechar:
        
        def __get__(self,):
            return self._csv_dialect.quotechar

        def __set__(self, quotechar):
            if len(quotechar) != 1:
                raise ValueError("Quote char must be a single char.")
            self._csv_dialect.quotechar = ord(quotechar[0])

    property delimiter:

        def __get__(self,):
            return self._csv_dialect.delimiter

        def __set__(self, delimiter):
            if len(delimiter) != 1:
                raise ValueError("Separator char must be a single char.")
            self._csv_dialect.delimiter = ord(delimiter[0])

    property escapechar:

        def __get__(self,):
            return self._csv_dialect.escapechar

        def __set__(self, escapechar):
            if len(escapechar) != 1:
                raise ValueError("Separator char must be a single char.")
            self._csv_dialect.escapechar = ord(escapechar[0])

    property doublequote:

        def __get__(self,):
            return self._csv_dialect.doublequote

        def __set__(self, doublequote):
            self._csv_dialect.doublequote = doublequote

    def __repr__(self,):
        return """CsvDialect(quotechar=%s,delimiter=%s,escapechar=%s,doublequote=%s)""" %\
            tuple(map(json.dumps, [self.quotechar, self.delimiter, self.escapechar, self.doublequote]))


cdef class CsvChunk:

    cdef _CsvChunk* _csv_chunk


    def __dealloc__(self):
        del self._csv_chunk

    def get(self, int i, int j):
        cdef const _Field* field = self._csv_chunk.get(i, j)
        if field == NULL:
            return None
        else:
            return field.s[:field.length]

    def to_array(self,):
        I = self.nb_rows()
        J = self.nb_columns()
        res = np.empty((I, J), dtype=np.object)
        for i in range(I):
            for j in range(J):
                cell = self.get(i, j)
                res[i, j] = cell
        return res
    
    def nb_rows(self,):
        return self._csv_chunk.nb_rows()

    def nb_columns(self,):
        return self._csv_chunk.nb_columns()


def create_array(chunks):
    print chunks
    if not chunks:
        return np.empty((0, 0), dtype=np.object)
    nb_cols = max(chunk.nb_columns() for chunk in chunks)
    nb_rows = sum(chunk.nb_rows() for chunk in chunks)
    res = np.empty((nb_rows, nb_cols), dtype=np.object)
    row_id = 0
    col_id = 0
    while chunks:
        chunk = chunks.popleft()
        I = chunk.nb_rows()
        J = chunk.nb_columns()
        for i in range(I):
            for j in range(J):
                res[row_id, j] = chunk.get(i, j)
            row_id += 1
    return res

def reader(stream, csv_dialect, buffer_length=10):
    return CsvReader(stream, csv_dialect, buffer_length)

cdef class CsvReader:

    cdef object stream
    cdef object buffer_length
    cdef _CsvReader * csv_reader
    
    def __cinit__(self, stream, CsvDialect csv_dialect, *args, **kwargs):
        cdef _CsvDialect * _csv_dialect = csv_dialect.get_csv_dialect()
        self.csv_reader = new _CsvReader(_csv_dialect)

    def __init__(self, stream, csv_dialect, buffer_length=10000000):
        """
        dialect --
        stream -- a file-like object streaming unicode characters
        """
        self.stream = stream
        self.buffer_length = buffer_length

    def __dealloc__(self):
        del self.csv_reader

    def read_all(self,):
        """ Read the csv in stream and returns a numpy array
        containing unicode objects.

        length -- the number of char handled per csv_chunk
        """
        csv_chunks = deque(self._iter_chunks(self.stream, self.buffer_length))
        return create_array(csv_chunks)

    def remaining(self,):
        return self.csv_reader.remaining[:self.csv_reader.remaining_length]

    cdef CsvChunk _read_chunk(self, buff, length, last_chunk):
        csv_chunk = CsvChunk()
        cdef _CsvChunk* _csv_chunk = self.csv_reader.read_chunk(buff, length, last_chunk)
        error_msg = unicode(_csv_chunk.error_msg)
        csv_chunk._csv_chunk = _csv_chunk
        if len(error_msg) > 0:
            raise ValueError(error_msg)
        return csv_chunk

    def _iter_chunks(self, stream, buffer_size):
        """ Yields csv chunk obtained by 
        continuoulsy reading at most <buffer_size> unicode
        character, and parsing it.
        """
        while True:
            buff = stream.read(buffer_size)
            assert isinstance(buff, unicode)
            length = len(buff)
            last_chunk = length < buffer_size
            chunk = self._read_chunk(buff, length, last_chunk)
            print "\n\n\n----------"
            print "Buffer", buff
            print "Remaining after", self.remaining()
            print "To array", chunk.to_array()
            yield chunk
            if last_chunk:
                break


