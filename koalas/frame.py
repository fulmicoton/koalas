from collections import OrderedDict
import numpy as np
#from koalas import io
from io import csv


class Column(object):
    ''' Backs up data.
    '''

    def get(self,):
        """Returns a numpy array with the
        column values"""
        raise NotImplementedError()

    def nb_rows(self,):
        raise NotImplementedError()

    def inc_ref(self, col_id):
        pass

    def dec_ref(self,):
        pass

    def dtype(self, col_id):
        raise NotImplementedError()


class MemoryColumn(Column):

    def __init__(self, data):
        self.data = data
        self.ref_count = 0

    def get(self,):
        return self.data

    def inc_ref(self,):
        self.ref_count += 1

    def dec_ref(self,):
        self.ref_count -= 1

    def cell(self, row):
        return self.data[row]

    @property
    def nb_rows(self,):
        return self.data.shape[0]

    @property
    def dtype(self,):
        return self.data.dtype

    def pick(self, row_selector):
        return MemoryColumn(self.data[row_selector])

    def __str__(self,):
        return "MemCol:" + str(self.data)


class ColumnRef(object):

    __slots__ = ('column',)

    def __init__(self, column,):
        self.column = column
        self.column.inc_ref()

    def __str__(self,):
        return "ColRef:" + str(self.column)

    def __del__(self,):
        self.column.dec_ref()

    def cell(self, row):
        return self.column.cell(row)

    def data_copy(self,):
        return self.column.get().copy()

    @property
    def nb_rows(self,):
        return self.column.nb_rows

    @property
    def dtype(self,):
        return self.column.dtype

    def pick(self, row_selector):
        return ColumnRef(self.column.pick(row_selector))


import re
NB_GUESS_LINES = 1000
FLOAT_PTN = re.compile("^\d+\.\d*$")
INT_PTN = re.compile("^[0-9]+$")


def guess(column):
    """ given a list of values
    returns a "guessed" type """
    nb_others = 0
    nb_floats = 0
    nb_ints = 0
    for val in column:
        if val is not None:
            if INT_PTN.match(val):
                nb_ints += 1
            elif FLOAT_PTN.match(val):
                nb_floats += 1
            else:
                nb_others += 1
    if nb_ints > 10 * nb_others:
        return np.int
    elif nb_floats + nb_ints > 10 * nb_others:
        return np.float
    else:
        return np.object


class DataFrame(object):

    __slots__ = (
        '__col_map',
        'nb_rows',
    )

    def __init__(self, col_map):
        self.__col_map = col_map
        self.nb_rows = col_map.values()[0].nb_rows
    
    @staticmethod
    def from_csv(f, dtypes=None, **kwargs):
        chunk_it = csv.reader(f, **kwargs).chunks()
        first_chunk = chunk_it.next()
        if first_chunk is None:
            raise ValueError("Empty csv file.")
        while first_chunk.nb_rows() == 0:
            first_chunk = chunk_it.next()
            if first_chunk is None:
                raise ValueError("Empty csv file.")
        headers = first_chunk.pop_row()
        chunks = csv.ChunkCollection([first_chunk] + list(chunk_it))
        if dtypes is None:
            guess_data = chunks.first_nb_rows(NB_GUESS_LINES)
            dtypes = map(guess, guess_data.transpose())
        columns = chunks.get_columns(dtypes)
        return DataFrame.from_items(zip(headers, columns))

    @property
    def dtypes(self,):
        return tuple(col.dtype for col in self.__col_map.values())

    @staticmethod
    def from_items(col_items):
        col_map = OrderedDict()
        nb_rows = None
        for (col_name, col_values) in col_items:
            column = MemoryColumn(col_values.copy())
            column_ref = ColumnRef(column)
            col_map[col_name] = column_ref
            if nb_rows is None:
                if len(col_values.shape) != 1:
                    raise ValueError("Column must have vector-like shaped.")
                nb_rows = col_values.shape[0]
            else:
                if (nb_rows,) != col_values.shape:
                    raise ValueError("Column %s has shape %s, expected %s.") % \
                        (col_name, str(col_values.shape), str(col_values.shape))
        return DataFrame(col_map=col_map,)

    @staticmethod
    def from_dict(col_dict):
        return DataFrame.from_items(col_dict.items())

    def _col_id(self, col):
        if col not in self.__col_map:
            ValueError("Column %s not in DataFrame." % col)
        return self.__col_map[col]

    def __getitem__(self, select_spec):
        if isinstance(select_spec, tuple):
            assert len(select_spec) == 2, ("DataFrame [] notations handles and most "
                                           "[row_selector], and [row_selector,column_selector]")
        else:
            return self.pick(select_spec)
        return self.row_select(self, slice)

    def __setitem__(self, col, value):
        col_id = self._col_id(col)
        self.data[:, col_id] = value

    def pick(self, row_selector):
        """ like select but for rows """
        return self.map_columns(lambda col_val: col_val.pick(row_selector))

    def map_columns(self, f):
        return DataFrame(col_map=OrderedDict(
            (col_name, f(col_val))
            for (col_name, col_val) in self.__col_map.items()
        ))

    def select(self, columns):
        """ Select a subset of columns """
        col_map = OrderedDict(
            (col_name, self.__col_map[col_name])
            for col_name in columns
        )
        return DataFrame(col_map=col_map)

    @property
    def nb_cols(self,):
        return len(self.__col_map)

    def head(self, nb_rows=10):
        if self.nb_rows <= 10:
            return self
        return DataFrame(column_index=self.column_index, data=self.data[:nb_rows])

    @property
    def shape(self,):
        return (self.nb_rows, self.nb_cols)

    def row(self, row):
        return np.array([
            col_ref.cell(row)
            for col_ref in self.__col_map.values()
        ])

    def record(self, row):
        return dict(self.columns, self.data[row])

    def str_header(self,):
        return " | ".join(self.columns)

    def str_body(self,):
        return "\n".join(
            self.str_row(row_id)
            for row_id in range(min(10, self.nb_rows))
        )

    def str_row(self, i):
        return " | ".join(map(str, self.row(i)))

    def __str__(self,):
        return self.str_header() + "\n" + self.str_body()

    @property
    def columns(self,):
        return self.__col_map.keys()
