# CQLDUMP #

CQLDUMP arose from the need to extract dump from Apache Cassandra DB without the obligation to extract all its content, which in general can be very large.

It allows the use of the where clause to filter and limit the number of records that will be exported to the dump.

The dump is ready to be imported using Cassandra's source command, including the creation commands for Keyspace and the exported Table.

### Details ###

* Developed in Python
* Version: 0.0.1

### Installation ###

* Clone this repository
```bash

    $ git clone git@bitbucket.org:getrak/cqldump.git

```
* Enter the cloned directory
```bash

    $ cd cqldump

```
* Install the tool
```bash

    $ sudo python3 setup.py install

```
Note: It is necessary to have Python installed, for more information on how to do the installation visit the link: https://www.python.org/downloads/

### Usage ###

```bash

    Usage: cqldump [host] [keyspace] [table] [options ...]  > [file_name.cql]

    Options:

		--u, --user		Username
		--p, --password	Password
		--ssl			Key file path for SSL connection
		--w, --where	Where Clause
		-h, --help      output usage information
        
```