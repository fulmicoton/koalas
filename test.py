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
    #u"a,b,c\"",
    u"a,d,c\na,b,c\n",
    u"a,d,c\na,b,c\na",
    u"a,d,c\na,b,c\na,",
    u"a,d,c\na,b,c\na,\n",
    u"a,\"bb\"\"b\",c",
    #u"a,\"bb,\"\"b\"c",
    #u"\"a,b\",\"b,c\"",
]


def _process_row(row):
    """ Remove trailing nones
    """
    row_it = iter(row)
    # yield unicode(row_it.next())
    while True:
        field = row_it.next()
        #print "field", field
        if field is None:
            break
        else:
            yield unicode(field)


def process_row(row):
    row = list(row)
    res = list(_process_row(row))
    return res


def to_tuples(res):
    return tuple(
        tuple(process_row(row))
        for row in res
    )


def run_test(test_string, parameter):
    res_csv = to_tuples(parsed_csv(test_string, parameter))
    try:
        res_koalas = to_tuples(parsed_koalas(test_string, parameter))
    except ValueError:
        print "\n--------------"
        print parameter
        print test_string
        print "Refused "
        return
    if res_csv != res_koalas:
        print "\n--------------"
        print "Error"
        print parameter
        print u"<" + test_string.encode("utf-8") + u">"
        print "csv", res_csv
        print "koalas", res_koalas
    else:
        print "Ok"
    

if __name__ == '__main__':
    for test_string in TEST_STRINGS:
        for parameter in iter_csvcodec_params():
            run_test(test_string, parameter)
