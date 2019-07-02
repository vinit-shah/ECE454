#!/bin/sh

unset JAVA_TOOL_OPTIONS
if [ -f /usr/lib/jvm/default-java/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/default-java
elif [ -f /usr/lib/jvm/java-11-openjdk-amd64/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
elif [ -f /usr/lib/jvm/java-8-openjdk-amd64/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-openjdk/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-openjdk
else
    echo "Unable to find java compiler :("
    exit 1
fi
export JAVA_HOME
export HADOOP_HOME=/opt/hadoop-3.1.2
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

echo --- Deleting
rm Task4.jar
rm Task4*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task4.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task4.jar Task4*.class

echo --- Running
INPUT=sample_input/in3.txt
OUTPUT=output_hadoop

rm -fr $OUTPUT
time $HADOOP_HOME/bin/yarn jar Task4.jar Task4 -Dmapreduce.jobtracker.staging.root.dir=/tmp/hadoop-tmp-$USER $INPUT $OUTPUT
cat $OUTPUT/*
