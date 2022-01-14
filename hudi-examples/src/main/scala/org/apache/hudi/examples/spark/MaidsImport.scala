package org.apache.hudi.examples.spark


import com.fullcontact.spark.ArgParse.{validateArgs, _}
import com.fullcontact.spark.SparkApp
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig.{KEYGENERATOR_CLASS_NAME, _}
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

object HudiMaidsImport extends SparkApp {

  override def createSparkConfig(): SparkConf = {
    super.createSparkConfig()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  /**
   *
   * Usage: HudiMaidsImport <tablePath> <tableName>.
   * <inputPath> is the path to a dfs that will be migrated to a hudi table
   * <tablePath> and <tableName> describe root path of hudi and table name
   * for example,
   * `HudiMaidsImport s3://<...>/maids/ s3://<...>/hudi/maids_table maids_table`
   */
  override def run(): Unit = {
    validateArgs(argsMap, hasValue = Seq("input-path", "table-path", "table-name"))
    val inputPath: String = argsMap.getString("input-path")
    val tablePath: String = argsMap.getString("table-path")
    val tableName: String = argsMap.getString("table-name")

    val hudiMigrationConfigs: Map[String, String] =
      Map(
        OPERATION.key -> UPSERT_OPERATION_OPT_VAL,
        //        TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
        RECORDKEY_FIELD.key -> "fcid",
        PRECOMBINE_FIELD.key -> "timestamp",
        PARTITIONPATH_FIELD.key -> "",
        TBL_NAME.key -> tableName,
        KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getName
      )

    queryData(spark, tablePath)

    spark.read.parquet(inputPath)
      .withColumnRenamed("maid.value", "maid_value")
      .withColumnRenamed("maid.ipAddress", "maid_ip_address")
      .withColumnRenamed("maid.type", "maid_type")
      .write.format("org.apache.hudi")
      .options(hudiMigrationConfigs)
      .mode(SaveMode.Append)
      .save(tablePath)

    incrementalQuery(spark, tablePath)

  }

  def queryData(spark: SparkSession,  tablePath: String): Unit = {
    val maidsView = spark
      .read
      .format("org.apache.hudi")
      .load(tablePath)

    maidsView.createOrReplaceTempView("maids_table_view")

    spark.sql("select * from maids_table_view").show()
  }

  def incrementalQuery(spark: SparkSession, tablePath: String): Unit = {
    import spark.implicits._
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from maids_table_view order by commitTime").map(k => k.getString(0)).collect()
    val beginTime = if (commits.length > 1) commits(commits.length - 2) else commits(0) // commit time we are interested in

    println("incremental query")

    // incrementally query data
    val incViewDF = spark.
      read.
      format("org.apache.hudi").
      option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME.key, beginTime).
      load(tablePath)
    incViewDF.createOrReplaceTempView("maids_incr_table")
    spark.sql("select * from maids_incr_table").show()
  }

}
