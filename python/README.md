# Qbeast Sharing


>### Warning: This project is an experimental extension to Delta Sharing!
> This project has been created by forking the official Delta Sharing repository in order to introduce sampling pushdown to the python client.
> 

[Qbeast Sharing](https://github.com/Qbeast-io/qbeast-sharing) extends the [Delta Sharing](https://delta.io/sharing) open protocol and adds the capabilities of sampling large datasets. Delta Sharing has been designed for secure real-time exchange of large datasets, which enables secure data sharing across different computing platforms. It lets organizations share access to existing [Delta Lake](https://delta.io) and [Apache Parquet](https://parquet.apache.org) tables with other organizations, who can then directly read the table in Pandas, Apache Spark, or any other software that implements the open protocol.

This is the Python client library for Delta Sharing, which lets you load shared tables as [pandas](https://pandas.pydata.org/) DataFrames or as [Apache Spark](http://spark.apache.org/) DataFrames if running in PySpark with the [Apache Spark Connector library](https://github.com/delta-io/delta-sharing#set-up-apache-spark).

## Installation and Usage

1. Install using `pip install qbeast-sharing`.
2. To use the Python Connector, see [the project docs](https://github.com/Qbeast-io/qbeast-sharing) for details.

## Documentation

This README only contains basic information about the Qbeast Sharing Python Connector. Please read [the project documentation](https://github.com/Qbeast-io/qbeast-sharing) for full usage details.
