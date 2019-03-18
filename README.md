# EB5001 Big Data Engineering

## Instruction
[Start Kafka](https://github.com/nakamura41/EB5001-Big-Data-Engineering/blob/master/Kafka.md)

### Example build.sbt configuration file (that works on BEAD vm - SPARK2.2)  
```
name := "TestScala"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.3"
```
Command to submit spark job : spark2-submit --class HelloScala --master local ./testscala_2.11-0.1.jar
