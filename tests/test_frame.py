from koalas import DataFrame
import numpy as np
from os import path as osp
import random
from koalas import io

TEST_COLUMNS = ["col_%i" % i for i in range(5)]
random.shuffle(TEST_COLUMNS)
TEST_DF = DataFrame.from_items([(col_name, np.random.random((10,))) for col_name in TEST_COLUMNS])


def open_resource(rel_path, prefix_path=osp.dirname(__file__)):
    rel_path = osp.join(prefix_path, rel_path)
    return open(rel_path, 'r')


def test_columns():
    assert TEST_DF.columns == TEST_COLUMNS


def test_shape():
    assert TEST_DF.nb_rows == 10
    assert TEST_DF.nb_cols == 5
    assert TEST_DF[:6].shape == (6, 5)


def test_row_selection():
    assert TEST_DF.select(["col_3"]).columns == ['col_3']

def test_import_csv():
    df = io.from_csv(open_resource('test.csv'))
    print df.dtypes
    assert df.dtypes == (np.int, np.float, np.object)
