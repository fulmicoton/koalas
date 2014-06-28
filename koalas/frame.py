from collections import OrderedDict
import numpy as np
from weakref import ref


class ArrayWrapper(object):

    __slots__ = (
        'array',
        'references'
    )

    def __init__(self, data):
        self.data = data
        self.references = []

    def add_ref(self, df):
        self.references.append(df)


class DataFrame(object):

    __slots__ = (
        'column_index',
        '_data',
        'column_idx')

    @def data():
        doc = "The data property."
        def fget(self):
            return self. data
        def fset(self, data_):
            self._data.references.remove_ref(self,)
            self. data = data_
            self._data.references.add_ref(self,)
        def fdel(self):
            raise NotImplementedError()
        return locals()
    data = property(* data())

    @staticmethod
    def from_items(column_items):
        nb_columns = len(column_items)
        if nb_columns == 0:
            return DataFrame([], np.zeros((0, 0)))
        (column_names, column_values) = zip(*column_items)
        nb_rows = column_values[0].shape[0]
        for (_, column_value) in column_items:
            column_shape = column_value.shape
            if column_shape != (nb_rows,):
                raise ValueError("Column values does not have a vector-like shape")
        data = np.empty((nb_rows, nb_columns))
        column_index = OrderedDict()
        for (column_id, (column, column_data)) in enumerate(column_items):
            column_index[column] = column_id
            data[:, column_id] = column_data
        return DataFrame(column_index=column_index,
                         data=data)

    @staticmethod
    def from_map(column_map):
        return DataFrame.from_items(column_map.items())

    def __getitem__(self, col):
        if col not in self.column_index:
            raise ValueError("Column %s is not in DataFrame" % col)
        col_id = self.column_index[col]
        return self.data[:, col_id]

    def _col_id(self, col):
        if col not in self.column_index:
            ValueError("Column %s not in DataFrame." % col)
        return self.column_index[col]

    def __setitem__(self, col, value):
        col_id = self._col_id(col)
        self.data[:, col_id] = value

    def select(self, columns):
        """ Select a subset of columns """
        column_ids = []
        column_index = OrderedDict()
        for column in columns:
            col_id = self._col_id(column)
            column_ids.append(col_id)
        return DataFrame(column_index, self.data[:, column_index])

    @property
    def nb_cols(self,):
        return self.shape[1]

    @property
    def nb_rows(self,):
        return self.shape[0]

    def head(self, nb_rows=10):
        if self.nb_rows <= 10:
            return self
        return DataFrame(column_index=self.column_index, data=self.data[:nb_rows, :])

    def __init__(self, column_index, data):
        self.column_index = column_index
        self.data = data

    @property
    def shape(self,):
        return self.data.shape

    def row(self, row):
        return self.data[row, self.column_idx]

    def record(self, row):
        return dict(self.columns, self.data[row])

    def str_header(self,):
        return " | ".join(self.columns)

    def str_body(self,):
        return " | ".join(
            self.str_row(row_id)
            for row_id in range(min(10, self.nb_rows))
        )

    def str_row(self, i):
        return " | ".join(self.row(i))

    def __str__(self,):
        return self.str_header() + self.str_body()

    @property
    def columns(self,):
        return self.column_index.keys()
