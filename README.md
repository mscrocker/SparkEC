# SparkEC
SparkEC is an error correction system whose goal is to correct DNA Sequencing errors. It works making use of the Spark Big Data Framework.

This project is based off the [CloudEC](https://github.com/CSCLabTW/CloudEC) project. The underlying algorithm keeps being the same in this project, but the code architecture has been completely refactored and the Hadoop Framework has been replaced by Spark.

## Getting Started

### Prerequisites

This project requires the following software to run:
* Spark Framework
* JRE Compatible with the Spark Framework 


Also, in order to build the project you will need both Maven and a JDK Java installation.
The following versions of those tools have already been tested to build this project:
* OpenJDK 8
* Maven 3

Also, this project depends on [Hadoop Sequence Parser](https://github.com/rreye/hsp), in order to read the different sequence formats. Please, download it if you want to build this project yourself.


### Execution

The project can be run submitting it as a Spark job:

`spark-submit SparkEC.jar -in <input dataset> -out <output directory>`

## Configuration

A "config.properties.template" file is provided with this repository. Check it to see all the available configurations for this tool. Once set the configuration file, it can be used with the "-config" command line argument: 

`spark-submit SparkEC.jar -in <input dataset> -out <output directory> -config <configuration file>`

It might be also interesting to tune the Spark configuration in order to get the best results. These possible configurations are:

* **spark.hadoop.validateOutputSpecs:** this option must be set to *false* if the output of individual phases is enabled.
* **spark.serializer:** it is highly recommended to set this option to *org.apache.spark.serializer.KryoSerializer* so the Kryo serializer is used.


## Compilation

In order to build the project, simply run the required maven phase. For example:

`mvn package`

The resulting Jar will be generated at the target directory, with the name SparkEC.jar


## Testing
This project has test included, which will run spark in local mode in order to test the different phases against their CloudEC equivalent outputs. In order to run the tests, simply run the "test" phase of Maven

`mvn test`

