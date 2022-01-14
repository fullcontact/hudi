/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.examples.spark;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleSparkUtils;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;



public class HoodieSparkBootstrapExample {

  private static String tableType = HoodieTableType.MERGE_ON_READ.name();


  public static void main(String[] args) throws Exception {
//    if (args.length < 5) {
//      System.err.println("Usage: HoodieWriteClientExample <tablePath> <tableName>");
//      System.exit(1);
//    }
//    String recordKey = args[0];
//    String tableName = args[1];
//    String partitionPath = args[2];
//    String preCombineField = args[3];
//    String basePath = args[4];

    SparkConf sparkConf = HoodieExampleSparkUtils.defaultSparkConf("hoodie-client-example");

    SparkSession spark = HoodieExampleSparkUtils.defaultSparkSession("Java Spark SQL basic example");

    spark.read().parquet("file:///Users/joey.idler/Documents/hudi/")
            .withColumnRenamed("maid.value","maid_value")
            .withColumnRenamed("maid.ipAddress","maid_ip_address")
            .withColumnRenamed("maid.type","maid_type")

            .write().format("org.apache.hudi")
            .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL())
            .option("hoodie.datasource.write.storage.type", HoodieTableType.COPY_ON_WRITE.name())
            .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "fcid")
            .option(HoodieWriteConfig.TBL_NAME.key(),"maids_hudi")
            .mode(SaveMode.Append)
            .save("file:///Users/joey.idler/Documents/hudi_processed/maids_hudi");

  }
}
