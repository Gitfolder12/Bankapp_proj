CREATE TABLE "CR"."HWM_COPY_LOG" 
(
    "SRC_OWNER"      VARCHAR2(30 BYTE),    -- source schema
    "SRC_TABLE"        VARCHAR2(128 BYTE),   -- source table
    "OBJECT_NAME"      VARCHAR2(128 BYTE),   -- partition or subpartition
    "DST_OWNER"      VARCHAR2(30 BYTE),    -- target schema
    "DST_TABLE"        VARCHAR2(128 BYTE),   -- target table
    "ACTION_TS"        DATE,                 -- action timestamp
    "STATUS"           VARCHAR2(20 BYTE), 
    "ERROR_MSG"        VARCHAR2(4000 BYTE), 
    "SRC_ROW_COUNT"    NUMBER,               -- source row count
    "TRG_ROW_COUNT"    NUMBER                -- target row count
) SEGMENT CREATION IMMEDIATE
LOGGING
TABLESPACE "CR_DATA";

-- Index
CREATE INDEX "CR"."HWM_COPY_LOG_IX1" 
ON "CR"."HWM_COPY_LOG" ("SRC_OWNER", "SRC_TABLE", "OBJECT_NAME", "DST_OWNER", "DST_TABLE", "STATUS");
