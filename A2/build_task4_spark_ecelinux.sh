#!/bin/bash

unset JAVA_TOOL_OPTIONS
if [ -f /usr/lib/jvm/java-8-openjdk-amd64/bin/java ]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-1.8.0-openjdk-amd64/bin/java ]; then
    JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
elif [ -f /usr/lib/jvm/java-1.8.0/bin/java ]; then
    JAVA_HOME=/usr/lib/jvm/java-1.8.0
else
    echo "Unable to find java 1.8 runtime, try ecetesla[0-3]"
    exit 1
fi
$JAVA_HOME/bin/java -version
export JAVA_HOME
export SCALA_HOME=/opt/scala-2.11.6
export SPARK_HOME=/opt/spark-2.4.3-bin-hadoop2.7
export CLASSPATH=.:"$SPARK_HOME/jars/*"


echo --- Deleting
rm Task4.jar
rm Task4*.class

echo --- Compiling
$SCALA_HOME/bin/scalac Task4.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
jar -cf Task4.jar Task4*.class

echo --- Running
INPUT=sample_input/in3.txt
OUTPUT=output_spark

rm -fr $OUTPUT
time $SPARK_HOME/bin/spark-submit --master "local[2]" --class Task4 Task4.jar $INPUT $OUTPUT

cat $OUTPUT/*
