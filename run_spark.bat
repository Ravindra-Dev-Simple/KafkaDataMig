@echo off
REM ============================================================
REM  Spark Environment Setup for Windows
REM  Sets JAVA_HOME, SPARK_HOME, HADOOP_HOME and runs spark-submit
REM
REM  Usage:
REM    run_spark.bat kafka_to_iceberg_consumer.py --mode batch
REM    run_spark.bat ingest_test.py
REM    run_spark.bat kafka_to_iceberg_consumer.py --mode streaming
REM ============================================================

set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot
set SPARK_HOME=D:\Datamig\.venv\Lib\site-packages\pyspark
set HADOOP_HOME=C:\hadoop
set PATH=%JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%PATH%

REM Iceberg + Kafka packages
set PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4

echo ============================================================
echo  JAVA_HOME:   %JAVA_HOME%
echo  SPARK_HOME:  %SPARK_HOME%
echo  HADOOP_HOME: %HADOOP_HOME%
echo  Script:      %*
echo ============================================================

spark-submit --packages %PACKAGES% %*
