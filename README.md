# Xtorm

## About

This is a database API specifically for MySQL's (8.0.15+) X Protocol API. 
It radically differs from most database APIs as it does not execute a statement at a time, but rather a complete transaction/unit of work. 

This means
* Reduced network roundtrips, only a single net.Conn Write() per unit of work
* Transaction duration times minimised
* No client connection state, as no tracking of
	* open transactions
	* prepared statements

    Reduces complexity when using a pool of connections.

* Simplified error handling, only have to check the transaction commit succeed. 
* Preparing statements and executing within the same unit of work.

## Error Handling

MySQL error handling is managed by MySQL expectation blocks. By opening a expectation block with the expectation of no errors, MySQL will fail all remaining statements in the block as soon as an error has occurred.

So the expected typical structure of a unit of work

1. Open expectation block
2. Prepare statements
3. Begin transaction
4. Insertions, updates, deletions, prepared statement executions, parameterised statement executions
5. Commit transaction
6. Close expectation block

If an error occurs in steps 1-4 then the commit will be skipped returning an error "Expectation failed: no_error". If the commit fails, then that will return an error. 


## TODO

- [ ] SELECTs. No SELECT support atm.
- [ ] Save IDs from LAST_INSERT_ID() into variables so can insert rows into multiple tables in one UoW. No protobuf method using mysql's xprotocol for SET instructions so this would have to be regular SQL & StmtExecute. Expr Variables are currently not supported in MySQL 8.0.27. "ERROR 5153 (HY000): Mysqlx::Expr::Expr::VARIABLE is not supported yet"
- [ ] More/better pool implementations.