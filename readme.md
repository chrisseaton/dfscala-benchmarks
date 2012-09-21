DFScala Benchmarks
==================

What is this?
-------------

This is a package of benchmarks that demonstrate the use of DFScala [1].

Who wrote this?
---------------

DFScala was written primarily by Salman Khan and Daniel Goodman at the
University of Manchester. It is research involving Chris Seaton, Behram Khan,
Yegor Guskov, Mikel Luján and Ian Watson.

The corresponding researcher is Daniel Goodman, goodmand@cs.man.ac.uk.

Licence
-------

See licence.txt.

Dependencies
------------

*   Scala 2.9.1
*   SBT 0.11
*   DFScala 0.1 (included)
*   MUTS 2.8 rv41 (included)

Compiling
---------

    sbt compile

Running the benchmarks
----------------------

The benchmarks use MUTS as a Java agent, so it cannot be run using the `sbt` command
without instrumenting the whole of SBT. Instead, use the scripts provided:

    ./fibonacci 10

    ./matrixmult 4

    ./kmeans -t 4 -f input/kmeans/random1000_12

Acknowledgements
----------------

The Teraflux project is funded by the European Commission Seventh Framework
Programme. Chris Seaton is an EPSRC funded student. Mikel Luján is a Royal
Society University Research Fellow.

The inputs for KMeans come from STAMP [2]. See input/kmeans/stamp-licence.txt.

References
----------

[1] D. Goodman, S. Khan, C. Seaton, B. Khan, M. Luján, and I. Watson.
DFScala: High level dataflow support for Scala. In Second International
Workshop on Data-Flow Models For Extreme Scale Computing (DFM), 2012.

[2] C. C. Minh, J. Chung, C. Kozyrakis, and K. Olukotun. STAMP: Stanford
transactional applications for multi-processing. In Proceedings of IEEE
International Symposium on Workload Characterization, 2008.
