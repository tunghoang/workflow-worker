import os,glob,numpy
from distutils.core import Extension, setup
from Cython.Build import cythonize
ext_modules=[
	Extension("_readdataSat",    	["_readdataSat.c"]),
	Extension("_readdataQPE",    	["_readdataQPE.c"]),
]
setup(ext_modules=cythonize(ext_modules))
