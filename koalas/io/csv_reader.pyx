# distutils: language = c++
# distutils: sources = koalas/io/_csv_reader.cpp

import numpy as np
cimport numpy as np
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from collections import deque


cdef extern from "_csv_reader.hpp" namespace "koalas":

    cdef cppclass _Field:
        Py_UNICODE* s
        int length

    cdef cppclass _CsvReader:
        _CsvReader(const _CsvDialect* dialect_) except +
        _CsvChunk* read_chunk(Py_UNICODE* buff, const int buffer_length)

    cdef cppclass _CsvChunk:
        _CsvChunk(int buffersize) except +
        const _Field* get(int i, int j) const
        int nb_rows() const
        int nb_columns() const

    cdef cppclass _CsvDialect:
        _CsvDialect() except +
        void set_quote(Py_UNICODE quote)
        void set_separator(Py_UNICODE separator)



cdef class CsvDialect:

    cdef _CsvDialect* _csv_dialect

    def __cinit__(self,):
        self._csv_dialect = new _CsvDialect()

    cdef _CsvDialect* get_csv_dialect(self,):
        return self._csv_dialect

    cdef void _set_quote(self, Py_UNICODE quote):
        self._csv_dialect.set_quote(quote)

    cdef void _set_separator(self, Py_UNICODE separator):
        self._csv_dialect.set_separator(separator)

    def set_quote(self, quote_char):
        if len(quote_char) != 1:
            raise ValueError("Quote char must be a single char.")
        self._set_quote(ord(quote_char[0]))

    def set_separator(self, separator_char):
        if len(separator_char) != 1:
            raise ValueError("Separator char must be a single char.")
        self._set_separator(ord(separator_char[0]))



cdef class CsvChunk:
    
    cdef _CsvChunk * _csv_chunk

    cdef object _buff

    def __cinit__(self, _buff):
        Py_INCREF(_buff)
        self._buff = _buff

    def __dealloc__(self):
        Py_DECREF(self._buff)
        del self._csv_chunk

    def get(self, int i, int j):
        cdef const _Field* field = self._csv_chunk.get(i, j)
        if field == NULL:
            return None
        else:
            return field.s[:field.length]

    def get_row(self, row_id):
        row = []
        j = 0
        while True:
            cell = self.get(row_id, j)
            if cell is None:
                return row
            else:
                row.append(cell)
            j += 1

    def nb_rows(self,):
        return self._csv_chunk.nb_rows()

    def nb_columns(self,):
        return self._csv_chunk.nb_columns()

def create_array(chunks):
    nb_cols = max(chunk.nb_columns() for chunk in chunks)
    nb_rows = sum(chunk.nb_rows() for chunk in chunks) - len(chunks) + 1
    res = np.empty((nb_rows, nb_cols), dtype=np.object)
    row_id = 0
    col_id = 0
    unclosed_row = None
    unclosed_col = None
    incomplete_cell = None
    while chunks:
        chunk = chunks.popleft()
        I = chunk.nb_rows()
        J = chunk.nb_columns()
        start_row = 0
        if unclosed_row is not None:
            first_row = chunk.get_row(0)
            start_row += 1
            row = (unclosed_row[:-1] + [unclosed_row[-1] + first_row[0]] + first_row[1:])[:J]
            for (j, v)  in enumerate(row):
                res[row_id, j] = v
            row_id += 1
        last_row = I - (1 if chunks else 0)
        for i in range(start_row, last_row):
            for j in range(J):
                field = chunk.get(i, j)
                res[row_id, j] = field
            row_id += 1
        unclosed_row = chunk.get_row(I-1)
    return res


cdef class CsvReader:

    cdef _CsvReader * csv_reader
            
    def __cinit__(self, CsvDialect csv_dialect):
        cdef _CsvDialect * _csv_dialect = csv_dialect.get_csv_dialect()
        self.csv_reader = new _CsvReader(_csv_dialect)

    def __dealloc__(self):
        del self.csv_reader

    def read(self, stream):
        return self._read(stream)

    cdef _read(self, stream):
        i = 0
        csv_chunks = deque()
        while True:
            i+=1
            buff = stream.read(1000000)
            assert isinstance(buff, unicode)
            buff_length = len(buff)
            if buff_length == 0:
                break
            csv_chunk = CsvChunk(buff)
            csv_chunk._csv_chunk = self.csv_reader.read_chunk(buff, buff_length)
            csv_chunks.append(csv_chunk)
        return create_array(csv_chunks)

