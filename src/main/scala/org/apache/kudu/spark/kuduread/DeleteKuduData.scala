package org.apache.kudu.spark.kuduread
import collection.JavaConverters._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.slf4j.LoggerFactory

import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{NullWritable, Text}


object DeleteKuduData {
  def delete(deletedf:DataFrame,kudtable:String,masterserver:String,spark:SparkSession ){

    val kuduContext = new KuduContext(masterserver, spark.sqlContext.sparkContext)
    kuduContext.deleteRows(deletedf, kudtable)
  }
}
