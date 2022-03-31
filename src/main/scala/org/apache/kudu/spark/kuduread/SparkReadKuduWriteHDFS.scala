// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations

package org.apache.kudu.spark.kuduread

import collection.JavaConverters._

import org.slf4j.LoggerFactory

import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{NullWritable, Text}


object SparkReadKuduWriteHDFS {

  // The list of RPC endpoints of Kudu masters in the cluster,
  // separated by comma. The default value assumes the following:
  //   * the cluster runs a single Kudu master
  //   * the Kudu master runs at the default RPC port
  //   * the spark-submit is run at the Kudu master's node
  // Replication factors of 1, 3, 5, 7 are available out-of-the-box.
  // The default value of 1 is chosen to provide maximum environment
  // compatibility, but it's good only for small toy kuduread like this.
  // For real-world scenarios the replication factor of 1 (i.e. keeping table's
  // data non-replicated) is a bad idea in general: consider the replication
  // factor of 3 and higher.

  val logger = LoggerFactory.getLogger(SparkReadKuduWriteHDFS.getClass)

  // Define a class that we'll use to insert data into the table.
  // Because we're defining a case class here, we circumvent the need to
  // explicitly define a schema later in the code, like during our RDD -> toDF()
  // calls later on.

  def main(args: Array[String]) {
    // Define our session and context variables for use throughout the program.
    // The kuduContext is a serializable container for Kudu client connections,
    // while the SparkSession is the entry point to SparkSQL and
    // the Dataset/DataFrame API.
    val masterserver=args(0)
    val kudtable=args(1)
    val imptable=args(2)
    val hdfsloc=args(3)
    val whereCol=args(4)
    val coalescecnt=args(5).toInt
    val comparecnt=args(6).toInt
    val spark = SparkSession.builder.appName("KuduSparkExample").getOrCreate()

    try {
      val sqlDF = spark.sqlContext.read.options(Map("kudu.master" -> masterserver, "kudu.table" -> kudtable)).kudu
      sqlDF.createOrReplaceTempView(imptable)
      spark.sqlContext.sql(s"SELECT * FROM $imptable  WHERE $whereCol > $comparecnt").coalesce(coalescecnt).write.parquet(hdfsloc)
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val successhdfsfile=hdfsloc.concat("/_SUCCESS")
      if (fs.exists(new org.apache.hadoop.fs.Path(successhdfsfile))){
        logger.info(s"Data copy from kudu to HDFS success, moving with kudu partition removal")
        val deletedf=spark.sql(s"SELECT * FROM $imptable  WHERE $whereCol > $comparecnt")

        DeleteKuduData.delete(deletedf,kudtable,masterserver,spark)

      }
      else{
        logger.error(s"Data copy from kudu to HDFS not succesful")
      }

    } catch {
      case unknown : Throwable => logger.error(s"got an exception: " + unknown)
        throw unknown
    } finally {
      logger.info(s"closing down the session")
      spark.close()
    }
  }
}