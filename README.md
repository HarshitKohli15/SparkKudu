# The code is used for reading the kudu data based on a certain condition, write the data to HDFS in Parquet format, check if the data is written in parquet successfully and move ahead with data deletion at kudu end.



# Install Maven 3.3.9 by using 

  wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.3.9/apache-maven-3.3.9-bin.tar.gz
  
  export M2_HOME=/root/apache-maven-3.3.9
  
  PATH="${M2_HOME}/bin:${PATH}"
  
  export PATH
  
  mvn -version
  
# Run below commands to package application
  mvn clean
  
  mvn package
  
# Run below command to trigger Spark code

  /opt/cloudera/parcels/CDH-6.3.4-1.cdh6.3.4.p0.6751098/bin/spark-submit --class org.apache.kudu.spark.kuduread.SparkReadKuduWriteHDFS --master yarn target/kudu-spark-example-1.0-SNAPSHOT.jar <kudu master server> <kudu table name> <impala table name> <hdfs location to generate parquet> <column name for where clause> <coalesce count> <limit for comparing the rows>
  
  Example :
  /opt/cloudera/parcels/CDH-6.3.4-1.cdh6.3.4.p0.6751098/bin/spark-submit --class org.apache.kudu.spark.examples.SparkExample --master yarn target/kudu-spark-example-1.0-SNAPSHOT.jar 10.65.223.153 "impala::default.customers" "customers" "/tmp/1000" "purchase_count" 1 1000

# Note: Change the variables and settings as per your environment. Also include the kinit command inside the scheduler or a shell script followed by the Spark command to run the code in Kerberized cluster.
