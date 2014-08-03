

all: ./koalas/io/csv_reader.so
	# python -m cProfile -o profile_output toto.py
	python test.py
	#nosetests test_csv.py

./koalas/io/csv_reader.so: ./koalas/io/_csv_reader.cpp ./koalas/io/_csv_reader.hpp  ./koalas/io/csv_reader.pyx
	python setup.py build_ext --inplace