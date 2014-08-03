# -*- coding: utf-8 -*-

from koalas.io import csv_reader
import time
import pandas

reader = csv_reader.CsvReader()


def test_koalas_csv():
    import codecs
    with codecs.open("test.csv", "r", "utf-8") as f:
        a = time.time()
        m = reader.read(f)
        #print m[:10]
        b = time.time()
        print "time", b - a
        #data = f.read()[:4000000]
        #csv_reader.tokenize_str(data)


def test_pandas_csv():
    a = time.time()
    df = pandas.DataFrame.from_csv(open("test.csv"), encoding="utf-8", sep=';')
    b = time.time()
    print "pandas", b-a


if __name__ == '__main__':
    test_koalas_csv()
    test_pandas_csv()
    test_koalas_csv()
    test_pandas_csv()
    test_koalas_csv()
    test_pandas_csv()
    test_koalas_csv()
    test_pandas_csv()
    test_koalas_csv()
    test_pandas_csv()
    print csv_reader.titi()
    #import timeit
    #print(timeit.timeit("test_koalas()", number=1, setup="from __main__ import test_koalas"))

#.tokenize_str(u"aaa,bcwerwerwerweこあらa").__class__
#print "a"
#print csv_reader.tokenize_str(u"aaa,bcwerwerwerweこあらa").__class__
#print csv_reader.tokenize_str(u"aaa,bこcwerwerwerweこあらa").encode("utf-8")
