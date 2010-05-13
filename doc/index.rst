.. psubprocess documentation master file, created by
   sphinx-quickstart on Mon May 10 09:45:04 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

psubprocess
===========

is a lightweight and unobtrusive application that turns non-parallel applications into parallel ones transparently.

psubprocess has been built up to ease the use of multiprocessor systems and PC-clusters. It is common to have one of those hardware systems at hand, but, in most cases, the software is not prepared to squeeze the power offered by them. psubprocess is an automatic parallelization solution that deals with the most simple and common case, we have a lot of items to process and a software that can deal with each item in an independent way. In these cases doCluster splits the input files, run independent processes for each batch of items and merges the outputs in a completely transparent way.

Transforming a non-parallel application into a parallel one using psubprocess is trivial. Just install it and run your application, there is no need to recompile or to change its interface.

For example let's suppose that we have a program that processes an input file with 1000 items (one per line) and creates an output file with 1000 modified items using with the command::

  $ foo_analysis -i input_file -o output_file

To parallelize it using psubprocess we would just do::

  $ run_in_parallel.py -c "foo_analysis >#-i# hola.txt <#-o# output_file"

psubprocess can also be used as a library to parallelize your own python software.

Contents:

.. toctree::
   :maxdepth: 2
   :hidden:

   introduction
   download

