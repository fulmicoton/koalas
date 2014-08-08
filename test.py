# -*- coding: utf-8 -*-

from koalas.io.csv_reader import CsvDialect, reader
import csv
import itertools
from StringIO import StringIO


"""
Known, and wanted discrepancy with csv).

- return a matrix (doh)
- returns unicode object


Known bug
- interned string may mess up if chunk buffer is len 1
"""

PARAMETERS = {
    "quotechar": ["\"", "\'"],
    "delimiter": ["\t", ","],
    "escapechar": ["\\", "|"],
    "doublequote": [True],
}.items()


def iter_csvcodec_params():
    (KEYS, VALUES) = zip(*PARAMETERS)
    for vals in itertools.product(*VALUES):
        yield dict(zip(KEYS, vals))


def parsed_csv(s, params):
    ss = StringIO(s)
    return list(csv.reader(ss, **params))


def copy_string(s):
    DUMMY_STRING = u"jambon"
    return (DUMMY_STRING + s)[len(DUMMY_STRING):]


def parsed_koalas(s, params):
    ss = StringIO(copy_string(s))
    dialect = CsvDialect(**params)
    return reader(ss, dialect).read_all()

TEST_STRINGS = [
    u"a,b,c",
    u"a,b,c",
    u"a,b,c\"",
    u"a,d,c\na,b,c\n",
    u"a,d,c\na,b,c\na",
    u"a,d,c\na,b,c\na,",
    u"a,\"bb\"\"b\"c",
    u"a,\"bb,\"\"b\"c",
    u"\"a,b\",\"b,c\"",
]


def process_row(row):
    """ Remove trailing nones
    """
    row_it = iter(row)
    yield unicode(row_it.next())
    while True:
        field = row_it.next()
        if field is None:
            break
        else:
            yield unicode(field)


def to_tuples(res):
    return tuple(
        tuple(process_row(row))
        for row in res
    )


def run_test(test_string, parameter):
    res_csv = to_tuples(parsed_csv(test_string, parameter))
    print "csv", res_csv
    res_koalas = to_tuples(parsed_koalas(test_string, parameter))
    print "koalas", res_koalas
    print "------0"
    assert res_csv == res_koalas

if __name__ == '__main__':
    for test_string in TEST_STRINGS:
        print "\n\n-------"
        print (test_string + u"|").encode("utf-8")
        for parameter in iter_csvcodec_params():
            print "--"
            print parameter
            run_test(test_string, parameter)
