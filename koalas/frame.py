from collections import OrderedDict
import numpy as np
from io import csv
import codecs


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

    def __array__(self,):
        return self.column.get()

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


def open_stream(f):
    """ from an argument f, returns
    a value unicode stream"""
    if hasattr(f, "read"):
        return f
    elif isinstance(f, basestring):
        # we assume this is a filepath
        # for a utf-8 encode file
        return codecs.open(f, encoding="utf-8")
    else:
        raise ValueError("Function expected either a filepath or a file-like object.")


class DataFrame(object):

    __slots__ = (
        '__col_map',
        'nb_rows',
    )

    def __init__(self, col_map):
        self.__col_map = col_map
        if len(col_map) == 0:
            self.nb_rows = 0
        else:
            self.nb_rows = col_map.values()[0].nb_rows

    @staticmethod
    def from_csv(f, columns=None, dtypes=None, **kwargs):
        stream = open_stream(f)
        chunk_it = csv.reader(stream, **kwargs).chunks()
        first_chunk = chunk_it.next()
        if first_chunk is None:
            raise ValueError("Empty csv file.")
        while first_chunk.nb_rows() == 0:
            first_chunk = chunk_it.next()
            if first_chunk is None:
                raise ValueError("Empty csv file.")
        if columns is None:
            headers = first_chunk.pop_row()
        elif columns == 'range':
            headers = map(unicode, range(first_chunk.nb_cols()))
        else:
            headers = columns
        chunks = csv.ChunkCollection([first_chunk] + list(chunk_it))
        if dtypes is None:
            dtypes = chunks.guess_types()
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

    @property
    def _common_dtype(self,):
        """ Returns the dtype of the array returned by __array__.

        Basically if the dtype returned by all columns is the same,
        return it. If they differ, return np.object.
        """
        dtypes_set = set(self.dtypes)
        if len(dtypes_set) == 1:
            return dtypes_set.pop()
        else:
            return np.object

    def __array__(self,):
        arr = np.empty(self.shape, self._common_dtype)
        for (col_id, col_arr) in enumerate(self.column_arrays):
            arr[:, col_id] = col_arr
        return arr

    def pick(self, row_selector):
        """ like select but for rows """
        return self.map_columns(lambda col_val: col_val.pick(row_selector))

    @property
    def column_arrays(self,):
        """ Returns a list of 1D numpy array representing the columns"""
        return [np.array(col) for col in self.__col_map.values()]

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
