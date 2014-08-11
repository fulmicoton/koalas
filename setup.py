from distutils.core import setup
#from setuptools import setup
from Cython.Build import cythonize
import numpy as np
from Cython.Distutils import build_ext

setup(
  name='koalas',
  ext_modules=cythonize(
            "koalas/io/csv.pyx",
            sources=["koalas/io/_csv_reader.cpp"],
            language="c++",
            cmdclass={"build_ext": build_ext},
            ),
  include_dirs = [np.get_include()],
  
  # install_requires=[
  #   'numpy==1.8.1',
  #   'Cython==0.20.2',
  #   ],

)

#
#setup(ext_modules = cythonize(
        #"_rectangle.pyx",
        #sources=["Rectangle.cpp"],
        #language="c++",
        #cmdclass={"build_ext": build_ext},))
