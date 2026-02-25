#!/usr/bin/env python3
"""
Initialize Hive transaction tables in Derby metastore.
These tables are required by Iceberg's HiveTableOperations for commit locking.
Must run AFTER the Derby metastore_db is created by Spark (first start),
but the HIVE_LOCKS table won't be there because Spark's embedded HMS
only creates the basic schema, not the TxnManager schema.
"""
import subprocess
import sys
import os

METASTORE_DB = "/dbt_project/metastore_db"
DERBY_JAR = None

# Find Derby JAR in Spark's jars
spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
for f in os.listdir(f"{spark_home}/jars"):
    if f.startswith("derby-") and f.endswith(".jar") and "derbytools" not in f and "derbyshared" not in f and "client" not in f:
        DERBY_JAR = f"{spark_home}/jars/{f}"
        break

if not DERBY_JAR:
    print("Derby JAR not found, skipping transaction schema init")
    sys.exit(0)

print(f"Using Derby JAR: {DERBY_JAR}")

# Derby ij script to create Hive transaction tables
# These DDL statements match what Hive's TxnHandler expects
IJ_SCRIPT = """
CONNECT 'jdbc:derby:{metastore_db};create=false';

-- Create HIVE_LOCKS table (required by Iceberg MetastoreLock)
CREATE TABLE APP.HIVE_LOCKS (
  HL_LOCK_EXT_ID BIGINT NOT NULL,
  HL_LOCK_INT_ID BIGINT NOT NULL,
  HL_TXNID BIGINT,
  HL_DB VARCHAR(128) NOT NULL,
  HL_TABLE VARCHAR(128),
  HL_PARTITION VARCHAR(767),
  HL_LOCK_STATE CHAR(1) NOT NULL,
  HL_LOCK_TYPE CHAR(1) NOT NULL,
  HL_LAST_HEARTBEAT BIGINT NOT NULL,
  HL_ACQUIRED_AT BIGINT,
  HL_USER VARCHAR(128) NOT NULL,
  HL_HOST VARCHAR(128) NOT NULL,
  HL_HEARTBEAT_COUNT INTEGER,
  HL_AGENT_INFO VARCHAR(128),
  HL_BLOCKEDBY_EXT_ID BIGINT,
  HL_BLOCKEDBY_INT_ID BIGINT
);

-- Create NEXT_LOCK_ID table
CREATE TABLE APP.NEXT_LOCK_ID (
  NL_NEXT BIGINT NOT NULL
);
INSERT INTO APP.NEXT_LOCK_ID VALUES (1);

-- Create TXNS table
CREATE TABLE APP.TXNS (
  TXN_ID BIGINT NOT NULL,
  TXN_STATE CHAR(1) NOT NULL,
  TXN_STARTED BIGINT NOT NULL,
  TXN_LAST_HEARTBEAT BIGINT NOT NULL,
  TXN_USER VARCHAR(128) NOT NULL,
  TXN_HOST VARCHAR(128) NOT NULL,
  TXN_AGENT_INFO VARCHAR(128),
  TXN_META_INFO VARCHAR(128),
  TXN_HEARTBEAT_COUNT INTEGER,
  TXN_TYPE INTEGER
);

-- Create NEXT_TXN_ID table
CREATE TABLE APP.NEXT_TXN_ID (
  NTXN_NEXT BIGINT NOT NULL
);
INSERT INTO APP.NEXT_TXN_ID VALUES (1);

-- Create COMPLETED_TXN_COMPONENTS table
CREATE TABLE APP.COMPLETED_TXN_COMPONENTS (
  CTC_TXNID BIGINT,
  CTC_DATABASE VARCHAR(128) NOT NULL,
  CTC_TABLE VARCHAR(128),
  CTC_PARTITION VARCHAR(767),
  CTC_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CTC_WRITEID BIGINT
);

-- Create TXN_COMPONENTS table
CREATE TABLE APP.TXN_COMPONENTS (
  TC_TXNID BIGINT NOT NULL,
  TC_DATABASE VARCHAR(128) NOT NULL,
  TC_TABLE VARCHAR(128),
  TC_PARTITION VARCHAR(767),
  TC_OPERATION_TYPE CHAR(1) NOT NULL,
  TC_WRITEID BIGINT
);

-- Create WRITE_SET table
CREATE TABLE APP.WRITE_SET (
  WS_DATABASE VARCHAR(128) NOT NULL,
  WS_TABLE VARCHAR(128) NOT NULL,
  WS_PARTITION VARCHAR(767),
  WS_TXNID BIGINT NOT NULL,
  WS_COMMIT_ID BIGINT NOT NULL,
  WS_OPERATION_TYPE CHAR(1) NOT NULL
);

-- Create COMPACTION_QUEUE table
CREATE TABLE APP.COMPACTION_QUEUE (
  CQ_ID BIGINT NOT NULL,
  CQ_DATABASE VARCHAR(128) NOT NULL,
  CQ_TABLE VARCHAR(128) NOT NULL,
  CQ_PARTITION VARCHAR(767),
  CQ_STATE CHAR(1) NOT NULL,
  CQ_TYPE CHAR(1) NOT NULL,
  CQ_TBLPROPERTIES VARCHAR(2048),
  CQ_WORKER_ID VARCHAR(128),
  CQ_START BIGINT,
  CQ_RUN_AS VARCHAR(128),
  CQ_HIGHEST_WRITE_ID BIGINT,
  CQ_META_INFO BLOB,
  CQ_HADOOP_JOB_ID VARCHAR(32),
  CQ_ERROR_MESSAGE CLOB,
  CQ_ENQUEUE_TIME BIGINT,
  CQ_WORKER_VERSION VARCHAR(128),
  CQ_INITIATOR_ID VARCHAR(128),
  CQ_INITIATOR_VERSION VARCHAR(128),
  CQ_CLEANER_START BIGINT,
  CQ_RETRY_RETENTION BIGINT DEFAULT 0,
  CQ_NEXT_TXN_ID BIGINT
);

-- Create NEXT_COMPACTION_QUEUE_ID table
CREATE TABLE APP.NEXT_COMPACTION_QUEUE_ID (
  NCQ_NEXT BIGINT NOT NULL
);
INSERT INTO APP.NEXT_COMPACTION_QUEUE_ID VALUES (1);

-- Create COMPLETED_COMPACTIONS table
CREATE TABLE APP.COMPLETED_COMPACTIONS (
  CC_ID BIGINT NOT NULL,
  CC_DATABASE VARCHAR(128) NOT NULL,
  CC_TABLE VARCHAR(128) NOT NULL,
  CC_PARTITION VARCHAR(767),
  CC_STATE CHAR(1) NOT NULL,
  CC_TYPE CHAR(1) NOT NULL,
  CC_TBLPROPERTIES VARCHAR(2048),
  CC_WORKER_ID VARCHAR(128),
  CC_START BIGINT,
  CC_END BIGINT,
  CC_RUN_AS VARCHAR(128),
  CC_HIGHEST_WRITE_ID BIGINT,
  CC_META_INFO BLOB,
  CC_HADOOP_JOB_ID VARCHAR(32),
  CC_ERROR_MESSAGE CLOB,
  CC_ENQUEUE_TIME BIGINT,
  CC_WORKER_VERSION VARCHAR(128),
  CC_INITIATOR_ID VARCHAR(128),
  CC_INITIATOR_VERSION VARCHAR(128)
);

-- Create TXN_LOCK_TBL
CREATE TABLE APP.TXN_LOCK_TBL (
  TL_LOCK BIGINT NOT NULL
);
INSERT INTO APP.TXN_LOCK_TBL VALUES (1);

DISCONNECT;
EXIT;
""".format(metastore_db=METASTORE_DB)

# Find derbytools jar for ij
derbytools_jar = None
for f in os.listdir(f"{spark_home}/jars"):
    if "derbytools" in f and f.endswith(".jar"):
        derbytools_jar = f"{spark_home}/jars/{f}"
        break

if not derbytools_jar:
    # Try derby client or just derby
    derbytools_jar = DERBY_JAR

# Write ij script to temp file
script_file = "/tmp/hive_txn_schema.sql"
with open(script_file, "w") as f:
    f.write(IJ_SCRIPT)

print(f"Running Derby ij to create Hive transaction tables...")
# Run using derby ij tool
result = subprocess.run(
    ["java", "-cp", f"{DERBY_JAR}:{derbytools_jar}", "org.apache.derby.tools.ij", script_file],
    capture_output=True, text=True, cwd="/dbt_project"
)
print("STDOUT:", result.stdout[-2000:] if result.stdout else "")
print("STDERR:", result.stderr[-2000:] if result.stderr else "")

if result.returncode != 0:
    print("WARNING: ij script had errors (tables may already exist - that's OK)")
else:
    print("Hive transaction schema initialized successfully")

sys.exit(0)
