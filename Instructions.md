# ColDB #

## Overview ##

The goal of the project is to design and build a main-memory optimized column-store. 

By the end of the project you will have designed, implemented, and evaluated several key elements of a modern data system and you will have experienced several design tradeoffs in the same way they are experienced in all major data industry labs. This is a heavy but fun project!

The project runs through the whole semester. There are a total of 4 milestones with specific expected software deliverables, which will be accompanied with a design document. The deliverables will be tested using predefined automated tests for functionality and, as extra credit, performance. 

We will provide these tests before-hand so you can test your system as you develop it. We will provide you with starting code that implements the basic client-server functionality, i.e., anything that has to do with how a server communicates with the client over a socket. In addition, we will provide a parser for the domain specific language. In this way your focus will be on designing and building the core of the database server, i.e., the essential core data processing algorithms and data structures of a database system. Whenever applicable we will let you know if there are existing libraries that are OK to use.

The project includes work on storage methods such as arrays for plain storage and B-trees for clustered or secondary indexes. It also includes work on access methods such as fast scans that are optimized for modern hardware and can utilize properties such as CPU prefetching and multi-cores. We expect you to provide main-memory optimized designs and implementation for several core operators of a db system: select, fetch, join, aggregations and basic math. In addition, the project includes work on updates and efficient execution of workloads that include multiple concurrent queries using scan sharing. 

Putting everything together you will have a data system that gets as input an intermediate language, i.e., a language used by db optimizers to describe a physical query plan. The system will be able to run complete select-project-join queries and it will be at least an order of magnitude faster than traditional row-store database systems in analytical workloads.

## Logistics ##

**Implementation Language:** The implementation language for the project is C. This is because a low-level language is required to design and build efficient systems as we need to have full control of how we utilize memory and computational resources.

**Why not C++:** As a class, logistically it only makes sense to support a single language. We require C instead of C++ because C is the purest language in terms of its low-level control over memory allocation and processor commands. By removing more advanced language constructs such as virtual function tables, lambda expressions, etc., we code in a way that requires constant thought about how our design is being used physically by the processor and memory subsystems.

**Test Machine:** All evaluation and formal testing for this class will occur on our lab machine. In particular, credit for assignments is given when your submitted code runs correctly on our lab machine; no credit is given for assignments which run correctly on your local machine but not on the lab machine. The lab machine will run automated tests multiple times per week and you will receive automated email notifications on the status of your code, and so you will be aware of what tests are passing on the lab machine. We have ordered a new machine to test this code. The server will be configured to run Ubuntu Server 16.04.3 (Linux kernel 4.4.0)

**Compiler:** gcc 5.4.0 will be used to compile your code on the test machine before running it. You should not rely on any features or functionality that have been added since this version. gcc is generally backward compatible so if you are using an older version of gcc you should be fine but you should confirm this by running your code on the test machine. You are free to use a different compiler on your own machine but it is your own responsibility to make sure that your code compiles on the test machine. Compilation using gcc 5.4.0 can be easily verified through the use of virtual machines to setup an environment identical to the test machine.

**Libraries:** In general, we will not allow any library that has to do with data structures. The reason is that one of the main objectives of the course is to learn about the trade-offs in designing these data structures/algorithms yourself. By controlling the data structures, you have control over every last bit in your system and you can minimize the amount of data your system moves around (which is the primary bottleneck).

**IDE:** You may use any IDE and development environment that you are familiar with. As long as your project compiles on our machine, everything is OK. We do not support, though, specific IDEs. If you are looking for an IDE, simple yet powerful IDEs include Vim, Emacs, and Sublime Text.

**Evaluation:** Individual deliverables should pass the automated tests on the test machine. However, you will not be judged only on how well your system works; it should be clear that you have designed and implemented the whole system, i.e., you should be able to perform changes on-the-fly, explain design details, etc.

## Milestones ##

**Milestone 1: Basic column-store**

In this milestone, the goal is to design and implement the basic functionality of a column-store with the ability to run single-table queries. The first part of this milestone is about the storage and organization of relational data in the form of columns, one per attribute. This requires the creation and persistence of a database catalog containing metadata about the tables in your database as well as the creation and maintenance of files to store the data contained in these tables. While the data in your database should be persistent, it does not need to be fault tolerant (you may assume power is never interrupted, disks don't fail, etc). Your system should support storing integer data.

The next step is to implement a series of scan-based operators on top of the columns to support queries with selection, projection, aggregation and math operators. Then using these operators, you will synthesize single-table query plans such as `SELECT max(A) FROM R where B< 20 and C > 30` that work with a bulk processing data flow. It should be noted that even when columns store only integer data, the results of aggregation statements (such as `AVG`) may produce non-integer results.

**Milestone 2: Add fast scans, using scan sharing & multi-cores** 

The second milestone is about making your scans fast. You will be adding support for scan sharing to minimize data movement and making scans use multiple cores to fully utilize parallelization opportunities of modern CPUs.

Your system should be able to run `N>>1` queries in parallel by reading data only once for the different queries' select operators. The end goal is to be able to run simple single-table queries (as in the first milestone) but use the new select operators that are capable of scan sharing.

To introduce more opportunities and enable shared scans, we introduce batching operators. The client can declare a batch of queries to execute and then tell the server to execute them all at once. The server then must coordinate with the client when it is done with this batch. During this batching operation, it can be assumed that no print commands will be executed.

The end result of this milestone should be a linear scale up with the number of concurrent queries and number of cores. Your final deliverable should include a performance report (graphs and discussion) to demonstrate that you can achieve such a performance boost. This report should discuss your results with respect to the various parameters that affect shared scan performance such as the number of queries and the number of cores.

We also expect you to answer the following question: How many concurrent queries can your shared scan handle? Is there an upper limit? If yes, why?

**Milestone 3: Indexing**

The goal is to add indexing support to boost select operators. Your column-store should support memory-optimized B-tree indexes and sorted columns in both clustered and unclustered form. In the first case, all columns of a table follow the sort order of a leading column. We expect query plans to differentiate between the two cases and properly exploit their physical layout properties. The end goal is to be able to run single-table queries (as in the first milestone) but use the new index-based select operators instead. Your column-store should support multiple clustered indices as well as multiple unclustered indices. To support multiple clustered indices, your database must organize and keep track of several copies of your base data. To make the creation of these clustered indices easier, we place several restrictions on them:

1. All clustered indices will be declared before the insertion of data to a table.

2. The first declared clustered index will be the principal copy of the data. Only this copy of the data will support unclustered indices. 

In addition, in this milestone we expect that you implement a very basic query optimizer. In particular, we expect your system to decide between using an index or using a scan for the select operator. This will require the keeping of very basic statistics such as a rough histogram of the data distribution for each column and will allow you to demonstrate the performance gains from your Milestone 3 index design.

**Milestone 4: Joins**

For the fourth milestone you will be adding join support to your system. For full credit on this milestone, we expect you to support cache-conscious hash joins and nested-loop join that utilize all cores of the underlying hardware. In addition, you need to support query plans that use these new join operators, e.g.: `SELECT max(R.d), min(S.g) FROM R, S WHERE R.a = S.a AND R.c >= 1 AND R.c =< 9 AND S.f >= 31 AND S.f =< 99`

Make sure you minimize random access during fetch operators after a join. The final deliverable for this milestone includes a performance report of nested loops vs hash joins, and a comparison of a grace-hash join vs. a one-pass hash join for large data sizes.
