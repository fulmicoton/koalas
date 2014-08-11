# -*- coding: utf-8 -*-

#from koalas.io.csv import reader
from koalas import DataFrame
from StringIO import StringIO


def test_empty():
    data = u"a,b\nc,1\nd,2\ne,3"
    stream = StringIO(data)
    df = DataFrame.from_csv(stream)
    #res = reader(stream).read_all()
    #print res
    #assert res.shape == (2, 2)

test_empty()

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
