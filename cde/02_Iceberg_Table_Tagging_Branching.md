## Iceberg Table Tagging & Branching

```
# CREATE ICEBERG TABLE
df  = spark.read.csv("/app/mount/cell_towers_1.csv", header=True, inferSchema=True)
df.writeTo("ICEBERG_CELL_TOWERS").using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
```

```
# LOAD NEW TRANSACTION BATCH
batchDf = spark.read.csv("/app/mount/cell_towers_2.csv", header=True, inferSchema=True)
batchDf.printSchema()
batchDf.createOrReplaceTempView("TEMP_VIEW_NEW_BATCH")
```

```
# CREATE TABLE BRANCH
spark.sql("ALTER TABLE ICEBERG_CELL_TOWERS CREATE BRANCH ingestion_branch")
```

```
# WRITE DATA OPERATION ON TABLE BRANCH
batchDf.write.format("iceberg").option("branch", "ingestion_branch").mode("append").save("ICEBERG_CELL_TOWERS")
```

```
# NOTICE THAT A SIMPLE SELECT AGAINST THE TABLE STILL RETURNS THE ORIGINAL DATA
spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.ICEBERG_CELL_TOWERS;").show()
```

```
# IF YOU WANT TO ACCESS BRANCH DATA YOU CAN SPECIFY THE BRANCH NAME IN THE SELECT WITH THE VERSION AS OF CLAUSE
spark.sql("SELECT COUNT(*) FROM SPARK_CATALOG.DEFAULT.ICEBERG_CELL_TOWERS VERSION AS OF 'ingestion_branch';").show()
```

```
# MONITOR BRANCHES IN REFS TABLE
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.ICEBERG_CELL_TOWERS.refs;").show()
```

```
# SAVE THE SNAPSHOT ID CORRESPONDING TO THE BRANCH
branchSnapshotId = spark.sql("SELECT snapshot_id FROM SPARK_CATALOG.DEFAULT.ICEBERG_CELL_TOWERS.refs \
                        WHERE NAME == 'ingestion_branch';").collect()[0][0]
```

```
# USE THE PROCEDURE TO CHERRY-PICK THE SNAPSHOT
# THIS IMPLICITLY SETS THE CURRENT TABLE STATE TO THE STATE DEFINED BY THE CHOSEN PRIOR SNAPSHOT ID
spark.sql("CALL spark_catalog.system.cherrypick_snapshot('SPARK_CATALOG.DEFAULT.ICEBERG_CELL_TOWERS',{})".format(branchSnapshotId))
```

```
# VALIDATE THE CHANGES
# THE TABLE ROW COUNT IN THE CURRENT TABLE STATE REFLECTS THE APPEND OPERATION - IT PREVIOSULY ONLY DID BY SELECTING THE BRANCH
spark.sql("SELECT COUNT(*) FROM ICEBERG_CELL_TOWERS;").show()
```

```
# CREATE TABLE TAG
spark.sql("ALTER TABLE SPARK_CATALOG.DEFAULT.ICEBERG_CELL_TOWERS CREATE TAG businessOrg RETAIN 365 DAYS").show()
```

```
# SELECT TABLE SNAPSHOT AS OF A PARTICULAR TAG
spark.sql("SELECT * FROM SPARK_CATALOG.DEFAULT.ICEBERG_CELL_TOWERS VERSION AS OF 'businessOrg';").show()
```
