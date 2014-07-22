import csv
import numpy as np
from itertools import izip_longest, izip
from frame import DataFrame
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


def iter_blocks(lines, dtypes, block_nb_lines):
    while True:
        columns = [
            np.array((block_nb_lines,), dtype=dtype)
            for dtype in dtypes
        ]
        line_count = 0
        for line in lines:
            line_count += 1
            if line_count == block_nb_lines:
                break
        else:
            if line_count != 0:
                yield [
                    column[:line_count]
                    for column in columns
                ]
            return
        yield columns


def from_csv(f, dtypes=None, **kwargs):
    lines = csv.reader(f, **kwargs)
    headers = lines.next()
    # nb_columns = len(headers)
    guess_data = [row for (_, row) in izip(xrange(NB_GUESS_LINES), lines)]
    guess_data = list(izip_longest(*guess_data))
    if dtypes is None:
        dtypes = map(guess, guess_data)
    columns = [
        np.array(col_data, dtype=col_dtype)
        for (col_dtype, col_data) in zip(dtypes, guess_data)
    ]
    return DataFrame.from_items(zip(headers, columns))
