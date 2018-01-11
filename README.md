# diffdb
Differential state tracking for Go


[![GoDoc](https://godoc.org/github.com/relvacode/diffdb?status.svg)](https://godoc.org/github.com/relvacode/diffdb)


DiffDB is a library for tracking changes to Go objects by hashing the contents and comparing it to a previous version. It makes uses of BoltDB to persist state history to disk.

DiffDB was created to store ETL client state and only process changes to a remote datasource such as MySQL. This allows longer running or more computationally expensive operations to run outside of the database query context.