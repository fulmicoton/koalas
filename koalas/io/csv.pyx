# distutils: language = c++
# distutils: sources = koalas/io/_csv_reader.cpp

import numpy as np
cimport numpy as np
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from collections import deque
from libcpp cimport bool
from libc.stdlib cimport malloc, free
from libcpp.vector cimport vector
from libcpp.string cimport string
import json

DEFAULT_BUFFER_SIZE = 1000000

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
        const _Field* get(size_t i, size_t j) const
        int nb_rows() const
        int nb_cols() const
        void remove_row(int row_id)
        bool ok() const
        string error_msg
        bool fill_int(int col, np.int_t* dest) const
        bool fill_float(int col, np.float_t* dest) const

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


cdef class ChunkCollection:

    cdef int nb_cols
    cdef int nb_rows
    cdef object chunks

    def __cinit__(self, chunks):
        self.chunks = deque(chunks)
        self.nb_rows = sum(chunk.nb_rows() for chunk in self.chunks)
        self.nb_cols = self.chunks[0].nb_cols()

    def get_columns(self, dtypes):
        res = []
        J = self.nb_cols
        if J != len(dtypes):
            raise ValueError("Expected as many dtypes value as there is columns.")
        for (col_id, dtype) in enumerate(dtypes):
            col_res = np.empty((self.nb_rows,), dtype=dtype)
            if dtype == np.int64:
                self.fill_int(col_id, col_res)
            elif dtype == np.float64:
                self.fill_float(col_id, col_res)
            else:
                self.fill_unicode(col_id, col_res)   
            res.append(col_res)
        return res

    cpdef bool fill_int(self, size_t col, np.ndarray[np.int64_t, ndim=1] arr):
        cdef size_t offset = 0
        for (chunkid, chunk) in enumerate(self.chunks):
            if not chunk.fill_int(col, arr, offset):
                return False
            offset += chunk.nb_rows()
        return True


    cpdef bool fill_float(self, size_t col, np.ndarray[np.float64_t, ndim=1] arr):
        cdef size_t offset = 0
        for chunk in self.chunks:
            if not chunk.fill_float(col, arr, offset):
                return False
            offset += chunk.nb_rows()
        return True

    cpdef bool fill_unicode(self, size_t col, np.ndarray[object, ndim=1] arr):
        cdef size_t offset = 0
        for chunk in self.chunks:
            for i in range(chunk.nb_rows()):
                arr[offset] = chunk.get(i, col) 
                offset += 1
        return True

    cpdef object guess_types(self,):
        res = []
        cdef np.ndarray[np.int64_t, ndim=1] int_buffer = np.empty((self.nb_rows,), dtype=np.int64)
        cdef np.ndarray[np.float64_t, ndim=1] float_buffer = np.empty((self.nb_rows,), dtype=np.float64)
        for col in range(self.nb_cols):
            if self.fill_int(<int>col, int_buffer):
                res.append(np.int64)
            elif self.fill_float(<int>col, float_buffer):
                res.append(np.float64)
            else:
                res.append(np.object)
        return res


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

    def pop_row(self, row_id=0):
        row = self.row(row_id)
        self._csv_chunk.remove_row(row_id)
        return row

    def row(self, i):
        J = self.nb_cols()
        row = np.empty((J,), dtype=np.object)
        for j in range(J):
            row[j] = self.get(i, j)
        return row

    def to_array(self,):
        I = self.nb_rows()
        J = self.nb_cols()
        res = np.empty((I, J), dtype=np.object)
        for i in range(I):
            for j in range(J):
                cell = self.get(i, j)
                res[i, j] = cell
        return res

    def nb_rows(self,):
        return self._csv_chunk.nb_rows()

    def nb_cols(self,):
        return self._csv_chunk.nb_cols()    

    cpdef fill_int(self, size_t col, np.ndarray[np.int_t, ndim=1] arr, size_t offset):
        return self._csv_chunk.fill_int(col, <np.int_t*>arr.data + offset)

    cpdef fill_float(self, int col, np.ndarray[np.float_t, ndim=1] arr, size_t offset):
        return self._csv_chunk.fill_float(col, <np.float_t*>arr.data + offset)


DEFAULT_DIALECT = CsvDialect()


def reader(stream, dialect=DEFAULT_DIALECT, buffer_length=DEFAULT_BUFFER_SIZE):
    return CsvReader(stream, dialect, buffer_length)


cdef class CsvReader:

    cdef object stream
    cdef object buffer_length
    cdef _CsvReader * csv_reader
    
    def __cinit__(self, stream, CsvDialect csv_dialect, *args, **kwargs):
        cdef _CsvDialect * _csv_dialect = csv_dialect.get_csv_dialect()
        self.csv_reader = new _CsvReader(_csv_dialect)

    def __init__(self, stream, csv_dialect, buffer_length=DEFAULT_BUFFER_SIZE):
        """
        dialect --
        stream -- a file-like object streaming unicode characters
        """
        self.stream = stream
        self.buffer_length = buffer_length

    def __dealloc__(self):
        del self.csv_reader

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

    def chunks(self, buffer_size=DEFAULT_BUFFER_SIZE):
        """ Yields csv chunk obtained by 
        continuoulsy reading at most <buffer_size> unicode
        character, and parsing it.
        """
        while True:
            buff = self.stream.read(buffer_size)
            assert isinstance(buff, unicode)
            length = len(buff)
            last_chunk = length < buffer_size
            chunk = self._read_chunk(buff, length, last_chunk)
            if chunk.nb_rows() > 0:
                yield chunk
            if last_chunk:
                break


