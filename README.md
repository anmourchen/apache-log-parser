# How to run the Apache log parser


Here are the steps to compile the project and run the log parser on the acces log stored in HDFS using Spark.

## Compile the target

```
sbt package
```

## Run the unit tests

```
sbt test
```

## Run the log parser on access logs on HDFS

```
// Use Spark 2
export SPARK_MAJOR_VERSION=2
spark-submit target/scala-2.10/apache-log-parsing_2.10-0.0.1.jar logparsing.Entrypoint /data/spark/project/NASA_access_log_Aug95.gz
```