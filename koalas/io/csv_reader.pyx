# distutils: language = c++
# distutils: sources = koalas/io/_csv_reader.cpp

import numpy as np
cimport numpy as np
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF


cdef extern from "_csv_reader.hpp" namespace "koalas":

    cdef cppclass _CsvReader:
        _CsvReader() except + 
        _CsvChunk* read_chunk(Py_UNICODE* buff, const int buffer_length)


    cdef cppclass _CsvChunk:
        _CsvChunk(int buffersize) except +
        const Py_UNICODE* get(int i, int j) const
        int nb_rows() const
        int nb_columns() const


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
        cdef const Py_UNICODE* cell = self._csv_chunk.get(i, j)
        if cell == NULL:
            return None
        else:
            return cell

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
    res = np.zeros((nb_rows, nb_cols), dtype=np.object)
    row_id = 0
    col_id = 0
    unclosed_row = None
    unclosed_col = None
    incomplete_cell = None
    for chunk in chunks:
        I = chunk.nb_rows()
        J = chunk.nb_columns()
        start_row = 0
        if unclosed_row is not None:
            first_row = chunk.get_row(0)
            start_row += 1
            row = (unclosed_row[:-1] + [unclosed_row[-1] + first_row[0]] + first_row[1:])[:J]
            for (j, v)  in enumerate(row):
                res[row_id, j] = v
        for i in range(start_row, I-1):
            for j in range(J):
                cell = chunk.get(i, j)
                res[row_id, j] = cell
            row_id += 1
        unclosed_row = chunk.get_row(I-1)
    return res

cdef class CsvReader:

    cdef _CsvReader * csv_reader

    def __cinit__(self,):
        self.csv_reader = new _CsvReader()

    def __dealloc__(self):
        del self.csv_reader

    def read(self, stream):
        return self._read(stream)

    cdef _read(self, stream):
        i = 0
        csv_chunks = []
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

    def __dealloc__(self):
        del self.csv_reader
