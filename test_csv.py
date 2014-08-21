# -*- coding: utf-8 -*-

#from koalas.io.csv import reader
from koalas import DataFrame
from StringIO import StringIO
import numpy as np
import time
from koalas.io.csv import CsvDialect
import pandas as pd

#csv_dialect=DEFAULT_DIALECT

csv_dialect = CsvDialect()
csv_dialect.delimiter = u';'

def test_empty():
    # data = u"a,d,c\nc,4,1.1\nd,2,2.1\ne,4,3.2\nf,3,3.2"
    data = u"a,s,d\nc,,d\nd,e\ne,f\nf,f\ng,a"
    stream = StringIO(data)
    #df = pd.read_csv("acquisitions_full.csv",)
    #df = DataFrame.from_csv(stream) #"acquisitions_full.csv")
    dtypes = None
    for i in range(100):
        print "-----"
        start = time.time()
        df = pd.read_csv("test.csv", delimiter=";")
        end = time.time()
        print "pandas %s s" % (end - start)
        start = time.time()
        df = DataFrame.from_csv("test.csv", csv_dialect=csv_dialect, dtypes=dtypes)
        dtypes = df.dtypes
        end = time.time()
        print "koalas %s s" % (end - start)
    # df = df.select(["acquired_year"])
    
    #sub_df= df.select(["acquired_year"]) #print end-start, 's'
    # print sub_df
    #print df.dtypes[17]
    #print df.columns[17]
    #print len(df.dtypes)
    # cols = list(df.columns)[:3]
    #print cols
    # print unicode(df.select(cols)).encode("utf-8")
    #res = reader(stream).read_all()
    #print res
    #assert res.shape == (2, 2)

test_empty()
#import pandas as pd
#import numpy as np
#data = u"a,b\nc,1\nd,2\ne,4\nf,6\ng,8\nh,a"
#help(pd.read_csv)
#df = pd.read_csv(StringIO(data), dtype={"a":np.object,"b":np.int})
#print df
#print df.dtypes

#test_empty()

#def test_frame():


# def test_non_unicode():
#     reader = csv_reader.CsvReader()
#     data = "aaa"
#     thrown = False
#     try:
#         reader.tokenize_str(data)
#     except ValueError:
#         thrown = True
#     assert thrown
#
# def test_emptyline():
#     reader = csv_reader.CsvReader()
#     data = u"\na,b,c\n"
#     res = reader.tokenize_str(data)
#     assert res.shape == (2, 3)
#     assert res[0, 0] == ""
#     assert res[0, 1] == ""
#     assert res[0, 2] == ""
#     assert res[1, 0] == "a"
#     assert res[1, 1] == "b"
