/**
 * Key Takeways (from Github):
 * The primary key field needs to be named YCSB_KEY
 * The other fields need to be prefixed with FIELD and count up starting from 1
 * Add the same number of FIELDs as you specify in the YCSB core properties, default is 10.
 * The type of the fields is not so important as long as they can accept strings of the length that you specify in the YCSB core properties, default is 100.
 */
DROP TABLE IF EXISTS  usertable;
CREATE TABLE usertable (
	YCSB_KEY VARCHAR(255) PRIMARY KEY,
	FIELD0 TEXT, FIELD1 TEXT,
	FIELD2 TEXT, FIELD3 TEXT,
	FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT,
	FIELD8 TEXT, FIELD9 TEXT
);