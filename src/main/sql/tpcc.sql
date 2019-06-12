/**
 For anything which is not specifically below 32767, we use INTEGER.
 This specifically applies to anything which scales with the number of warehouses.
 Using INTEGER, we are able to support around 1 billion warehouses.
 That should be enough for the scale of this project.
 */


/**
 * See Page 12
 */
DROP TABLE IF EXISTS tpcc_warehouse CASCADE;
CREATE TABLE tpcc_warehouse (
  W_ID       INTEGER PRIMARY KEY,
  W_NAME     VARCHAR(10) NOT NULL,
  W_STREET_1 VARCHAR(20) NOT NULL,
  W_STREET_2 VARCHAR(20) NOT NULL,
  W_CITY     VARCHAR(20) NOT NULL,
  W_STATE    CHAR(2) NOT NULL,
  W_ZIP      CHAR(9) NOT NULL,
  /* Sales Tax */
  W_TAX      NUMERIC(4, 4) NOT NULL,
  /* Year to date balance */
  W_YTD      NUMERIC(12, 2) NOT NULL
);

/**
 * See Page 13
 */
DROP TABLE IF EXISTS tpcc_district CASCADE;
CREATE TABLE tpcc_district (
  D_ID        SMALLINT NOT NULL,
  D_W_ID      INTEGER NOT NULL,
  D_NAME      VARCHAR(10) NOT NULL,
  D_STREET_1  VARCHAR(20) NOT NULL,
  D_STREET_2  VARCHAR(20) NOT NULL,
  D_CITY      VARCHAR(20) NOT NULL,
  D_STATE     CHAR(2) NOT NULL,
  D_ZIP       CHAR(9) NOT NULL,
  /* Sales Tax */
  D_TAX       NUMERIC(4, 4) NOT NULL,
  /* Year to date balance */
  D_YTD       NUMERIC(12, 2) NOT NULL,
  /* Next available Order number */
  D_NEXT_O_ID INTEGER NOT NULL,
  PRIMARY KEY (D_W_ID, D_ID),
  FOREIGN KEY (D_W_ID) REFERENCES tpcc_warehouse (W_ID)
);

/**
 * See Page 14
 */
DROP TABLE IF EXISTS tpcc_customer CASCADE;
CREATE TABLE tpcc_customer (
  C_ID          INTEGER NOT NULL,
  C_D_ID        SMALLINT NOT NULL,
  C_W_ID        INTEGER NOT NULL,
  C_FIRST       VARCHAR(16) NOT NULL,
  C_MIDDLE      VARCHAR(2) NOT NULL,
  C_LAST        VARCHAR(16) NOT NULL,
  C_STREET_1    VARCHAR(20) NOT NULL,
  C_STREET_2    VARCHAR(20) NOT NULL,
  C_CITY        VARCHAR(20) NOT NULL,
  C_STATE       CHAR(2) NOT NULL,
  C_ZIP         CHAR(9) NOT NULL,
  C_PHONE       CHAR(16) NOT NULL,
  C_SINCE       TIMESTAMP NOT NULL,
  /** GC = Good, BC = Bad */
  C_CREDIT      CHAR(2) NOT NULL,
  C_CREDIT_LIM  NUMERIC(12, 2) NOT NULL,
  C_DISCOUNT    NUMERIC(4, 4) NOT NULL,
  C_BALANCE     NUMERIC(12, 2) NOT NULL,
  C_YTD_PAYMENT NUMERIC(12, 2) NOT NULL,
  C_PAYMENT_CNT NUMERIC(4) NOT NULL,
  C_DELIVERY_CNT NUMERIC(4) NOT NULL,
  /** Miscellaneous information */
  C_DATA        VARCHAR(500) NOT NULL,
  PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
  FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES tpcc_district (D_W_ID, D_ID)
);

/**
 * See Page 15
 */
DROP TABLE IF EXISTS tpcc_history CASCADE;
CREATE TABLE tpcc_history (
  H_C_ID   INTEGER NOT NULL,
  H_C_D_ID SMALLINT NOT NULL,
  H_C_W_ID INTEGER NOT NULL,
  H_D_ID   SMALLINT NOT NULL,
  H_W_ID   INTEGER NOT NULL,
  H_DATE   TIMESTAMP NOT NULL,
  H_AMOUNT NUMERIC(6, 2) NOT NULL,
  /** Miscellaneous Information */
  H_DATA   VARCHAR(24) NOT NULL,
  FOREIGN KEY (H_C_W_ID, H_C_D_ID, H_C_ID) REFERENCES tpcc_customer (C_W_ID, C_D_ID, C_ID),
  FOREIGN KEY (H_W_ID, H_D_ID) REFERENCES tpcc_district (D_W_ID, D_ID)
);

/**
 * See Page 16. Done first since new_order references order
 */
DROP TABLE IF EXISTS "tpcc_order" CASCADE;
CREATE TABLE "tpcc_order" (
  O_ID         INTEGER NOT NULL,
  O_D_ID       SMALLINT NOT NULL,
  O_W_ID       INTEGER NOT NULL,
  O_C_ID       INTEGER NOT NULL,
  O_ENTRY_D    TIMESTAMP NOT NULL,
  O_CARRIER_ID SMALLINT,  /** can be null */
  /** Count of order-lines */
  O_OL_CNT     NUMERIC(2) NOT NULL,
  O_ALL_LOCAL  NUMERIC(1) NOT NULL,
  PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
  FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES tpcc_customer (C_W_ID, C_D_ID, C_ID)
);

/**
 * See Page 15
 */
DROP TABLE IF EXISTS tpcc_new_order CASCADE;
CREATE TABLE tpcc_new_order (
  NO_O_ID INTEGER NOT NULL,
  NO_D_ID SMALLINT NOT NULL,
  NO_W_ID INTEGER NOT NULL,
  PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID),
  FOREIGN KEY (NO_W_ID, NO_D_ID, NO_O_ID) REFERENCES "tpcc_order" (O_W_ID, O_D_ID, O_ID)
);


/**
 * See Page 17. Done first since stock references item
 */
DROP TABLE IF EXISTS tpcc_item CASCADE;
CREATE TABLE tpcc_item (
  /** 100k items are populated*/
  I_ID    INTEGER NOT NULL,
  /** Image ID associated to Item*/
  I_IM_ID INTEGER NOT NULL,
  I_NAME  VARCHAR(24) NOT NULL,
  I_PRICE NUMERIC(5, 2) NOT NULL,
  /** Brand Information*/
  I_DATA  VARCHAR(50) NOT NULL,
  PRIMARY KEY (I_ID)
);

/**
 * See Page 17. Done first since order_line references stock
 */
DROP TABLE IF EXISTS tpcc_stock CASCADE;
CREATE TABLE tpcc_stock (
  /** 100k populated per warehouse */
  S_I_ID       INTEGER NOT NULL,
  S_W_ID       INTEGER NOT NULL,
  S_QUANTITY   NUMERIC(4) NOT NULL,
  S_DIST_01    CHAR(24) NOT NULL,
  S_DIST_02    CHAR(24) NOT NULL,
  S_DIST_03    CHAR(24) NOT NULL,
  S_DIST_04    CHAR(24) NOT NULL,
  S_DIST_05    CHAR(24) NOT NULL,
  S_DIST_06    CHAR(24) NOT NULL,
  S_DIST_07    CHAR(24) NOT NULL,
  S_DIST_08    CHAR(24) NOT NULL,
  S_DIST_09    CHAR(24) NOT NULL,
  S_DIST_10    CHAR(24) NOT NULL,
  S_YTD        NUMERIC(8) NOT NULL,
  S_ORDER_CNT  NUMERIC(4) NOT NULL,
  S_REMOTE_CNT NUMERIC(4) NOT NULL,
  /** Make Information */
  S_DATA       VARCHAR(50) NOT NULL,
  PRIMARY KEY (S_W_ID, S_I_ID),
  FOREIGN KEY (S_W_ID) REFERENCES tpcc_warehouse (W_ID),
  FOREIGN KEY (S_I_ID) REFERENCES tpcc_item (I_ID)
);

/**
 * See Page 16
 */
DROP TABLE IF EXISTS tpcc_order_line CASCADE;
CREATE TABLE tpcc_order_line (
  OL_O_ID        INTEGER NOT NULL,
  OL_D_ID        SMALLINT NOT NULL,
  OL_W_ID        INTEGER NOT NULL,
  OL_NUMBER      SMALLINT NOT NULL,
  OL_I_ID        INTEGER NOT NULL,
  OL_SUPPLY_W_ID INTEGER NOT NULL,
  OL_DELIVERY_D  TIMESTAMP, /** Can be null*/
  OL_QUANTITY    NUMERIC(2) NOT NULL,
  OL_AMOUNT      NUMERIC(6, 2) NOT NULL,
  OL_DIST_INFO   CHAR(24) NOT NULL,
  PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
  FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES "tpcc_order" (O_W_ID, O_D_ID, O_ID),
  FOREIGN KEY (OL_SUPPLY_W_ID, OL_I_ID) REFERENCES tpcc_stock (S_W_ID, S_I_ID)
);