# -*- coding: utf-8 -*-

from koalas.io.csv_reader import CsvDialect, CsvReader
import time
import pandas
import csv
from collections import OrderedDict
import itertools
from StringIO import StringIO


PARAMETERS = {
   "quotechar": ["\"", "\'"],
   "delimiter": ["\t", ","],
   "escapechar": ["\\", "|"],
   "doublequote": [False, True],
}.items()


# PARAMETERS = {
#     "quotechar": ["\""],
#     "delimiter": [","],
#     "escapechar": ["\\"],
#     "doublequote": [False, True],
# }.items()


def iter_csvcodec_params():
    (KEYS, VALUES) = zip(*PARAMETERS)
    for vals in itertools.product(*VALUES):
        yield dict(zip(KEYS, vals))



def parsed_csv(s, params):
    ss = StringIO(s)
    reader = csv.reader(ss, **params)
    return list(reader)

def parsed_koalas(s, params):
    ss = StringIO(s)
    dialect = CsvDialect(**params)
    csv_reader = CsvReader(dialect)
    return csv_reader.read(ss)

#dialect = csv_reader.CsvDialect()
#dialect.delimiter = ";"
#reader = csv_reader.CsvReader(dialect)






# """
# def test_koalas_csv():
#     import codecs
#     with codecs.open("test.csv", "r", "utf-8") as f:
#         a = time.time()
#         m = reader.read(f)
#         print m[-100:]
#         b = time.time()
#         print "time", b - a


# def test_pandas_csv():
#     a = time.time()
#     df = pandas.DataFrame.from_csv(open("test.csv"), encoding="utf-8", sep=';')

#     b = time.time()
#     print "pandas", b-a
# """

TEST_STRINGS = [
    #u"a,b,c",
    u"a,d,c\na,b,c\n",
]

def to_tuples(res):
    return tuple(
        tuple (
            unicode(field)
            for field in row
        )
        for row in res
    )

def run_test(test_string, parameter):
    res_csv = parsed_csv(test_string, parameter)
    res_koalas = parsed_koalas(test_string, parameter)
    assert to_tuples(res_csv) == to_tuples(res_koalas)

if __name__ == '__main__':
    for test_string in TEST_STRINGS:
        print "\n\n-------"
        print test_string.encode("utf-8")
        for parameter in iter_csvcodec_params():
            print "--"
            print parameter
            run_test("c" + test_string, parameter)
    
    #import timeit
    #print(timeit.timeit("test_koalas()", number=1, setup="from __main__ import test_koalas"))

#.tokenize_str(u"aaa,bcwerwerwerweこあらa").__class__
#print "a"
#print csv_reader.tokenize_str(u"aaa,bcwerwerwerweこあらa").__class__
#print csv_reader.tokenize_str(u"aaa,bこcwerwerwerweこあらa").encode("utf-8")
