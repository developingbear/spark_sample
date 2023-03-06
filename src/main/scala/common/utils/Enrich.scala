package com.kakao.adrec.atom.metric
package common.utils

import common.domain.{Identifier, Ranker}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, get_json_object, lit, to_timestamp}
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer


class Enrich(idType: Identifier.Value, logicalDateTimeStr: String) extends SparkBaseJob {

  private val logicalDateTime: LogicalDateTime = new LogicalDateTime(logicalDateTimeStr)

  import spark.implicits._

  // Enrich functions

  /**
   * result
   * col name | spark data type
   * ------------------------------------------
   * bid_request_id       | StringType
   * atom_raw_log         | StringType
   * ------------------------------------------
   */
  def readAtomLog(idType: Identifier.Value = idType): DataFrame = {
    val atomBasePath = idType match {
      case Identifier.ADID => "/ns2/bizrec/lookalike/atom_log/bizrec-atom-filter-result/data"
      case Identifier.ACID => "/ns2/bizrec_acid/lookalike/atom_log/bizrec-atom-filter-result-acid/data"
    }

    // ranker log 지연 가능성 존재. 1시간 이전 atom log까지 포함
    val atomLogPaths = List(
      s"${atomBasePath}/date=${logicalDateTime.getDate()}/hour=${logicalDateTime.getHour()}",
      s"${atomBasePath}/date=${logicalDateTime.getMinusHourDate(1)}/hour=${logicalDateTime.getMinusHour(1)}"
    )

    spark.read
      .option("basePath", atomBasePath)
      .text(atomLogPaths: _*)
      .select(
        (idType match {
          case Identifier.ADID => get_json_object($"value", "$.bid")
          case Identifier.ACID => get_json_object($"value", "$.acidBid")
        }) as "bid_request_id",
        $"value" as "atom_raw_log",
      )
      // atom log의 tag_count 와 survivals가 존재하는 atom log만 분석
      .filter(get_json_object($"value", "$.tagCount") > 0)
      .filter(get_json_object($"value", "$.survivals") =!= "{}")
  }


  /**
   * result
   * col name | spark data type
   * ----------------------------------------------------
   * bid_request_id                   | StringType
   * ranker_action_id                 | StringType
   * ranker_action_type               | StringType
   * ranker_raw_log                   | StringType
   * ranker_event_time                | TimestampType
   * ----------------------------------------------------
   */
  def readRankerLog(rankerLogType: Ranker.Value = rankerLogType, idType: Identifier.Value = idType): DataFrame = {
    val extractDF = spark.sql(s"select * from biz_$idType.raw_km_filtered_$rankerLogType")
      .filter($"p_dt" === logicalDateTime.getDate())
      .filter($"p_hr" === logicalDateTime.getHour())
      .select(
        get_json_object(col("log_data"), "$.rawMessage.bid.bidRequestId") as "bid_request_id",

        (rankerLogType match {
          case Ranker.WIN | Ranker.VIMP | Ranker.CLICK =>
            get_json_object(col("log_data"), "$.rawMessage.action.actionId")
          case Ranker.CONVERSION | Ranker.ACTION =>
            get_json_object(col("log_data"), "$.rawMessage.action.actionChain[0].actionId")
        }) as "ranker_action_id",

        lit(rankerLogType.toString) as "ranker_action_type",
        $"log_data" as "ranker_raw_log",
        to_timestamp(get_json_object(col("log_data"), "$.processDttm"), "yyyyMMdd HHmmss SSS") as "ranker_event_time",
      )

    // raw data size를 줄이기 위해 조건에 따른 선필터 목록

    // valid log만 분석, 내부정책
    var filteredDF = extractDF.filter(get_json_object(col("log_data"), "$.valid") === "Y")

    if(idType == Identifier.ACID){
      filteredDF = filteredDF
        .filter(get_json_object(col("ranker_raw_log"), "$.rawMessage.user.accid") =!= '0')
        .filter(get_json_object(col("ranker_raw_log"), "$.rawMessage.user.accid").isNotNull)
    }

    // WIN log의 volume이 매우 크고, 모든 종류의 goal, objectiveType에 대한 분석은 ADID log로 분석하고 있음.
    // 따라서, ACID-WIN log에는 필터링을 적용하여 data volume을 크게 줄임
    if (idType == Identifier.ACID && (rankerLogType == Ranker.WIN || rankerLogType == Ranker.VIMP)) {
      filteredDF = filteredDF
        .filter(get_json_object($"ranker_raw_log", "$.rawMessage.winner.goal") === "CONVERSION")
        .filter(get_json_object($"ranker_raw_log", "$.rawMessage.winner.objectiveType") === "PIXEL_AND_SDK")
    }

    if(rankerLogType == Ranker.ACTION)
      filteredDF = filteredDF
        .filter(get_json_object($"ranker_raw_log", "$.rawMessage.winner.objectiveDetailType") === "ADD_FRIEND")

    filteredDF
  }


  /**
   * result
   * col name | spark data type
   * ----------------------------------------------------
   * bid_request_id                   | StringType
   * atom_raw_log                     | StringType
   * atom_survivals_count             | Integer
   * atom_response_time               | TimestampType
   * ranker_action_id                 | StringType
   * ranker_action_type               | StringType
   * ranker_raw_log                   | StringType
   * ranker_event_time                | TimestampType
   * ----------------------------------------------------
   */
  def readEnrichLog(rankerLogType: Ranker.Value, startDiffHour: Int = 0, endDiffHour: Int = 0, rankerEventTimeLimitDays: Int = 0): DataFrame = {
    /**
     * startDiffHour 부터 endDiffHour 까지의 data를 path에 추가
     * ex) startDiffHour = 0, endDiffHour = 0 => executionTime의 data path(기본값)
     * ex) startDiffHour = 0, endDiffHour = 24 => executionTime의 현재 부터, 24시간 전까지의 data path
     * ex) startDiffHour = 1, endDiffHour = 10 => executionTime의 1시간 전부터, 10시간 전까지의 data path
     */
    val paths = ArrayBuffer[String]()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val enrichBasePath = idType match {
      case Identifier.ADID => s"/ns2/bizrec/lookalike/atom_log/metric/enrich/atom_${rankerLogType}/prd"
      case Identifier.ACID => s"/ns2/bizrec_acid/lookalike/atom_log/metric/enrich/atom_${rankerLogType}/prd"
    }

    for (i <- startDiffHour to endDiffHour) {
      val clickPath = s"${enrichBasePath}/date=${logicalDateTime.getMinusHourDate(i)}/hour=${logicalDateTime.getMinusHour(i)}"
      if (fs.exists(new Path(clickPath))) {
        paths += clickPath
      }
    }

    val enrichSchema = StructType(
      List(
        StructField("bid_request_id", StringType, true),
        StructField("atom_survivals_count", IntegerType, true),
        StructField("atom_raw_log", StringType, true),
        StructField("ranker_action_id", StringType, true),
        StructField("ranker_action_type", StringType, true),
        StructField("ranker_raw_log", StringType, true),
        StructField("ranker_event_time", TimestampType, true)
      )
    )

    val enrichLogDF = spark.read.option("basePath", enrichBasePath)
      .schema(enrichSchema)
      .parquet(paths: _*)
      .drop("date")
      .drop("hour")


    // enriched log의 ranker_event_time을 특정일 기준으로 filtering
    // atom_click, ranker_conv log간 join할 때 사용.
    if (rankerEventTimeLimitDays > 0) {
      val thresholdTimestamp = Timestamp.valueOf(logicalDateTime.getMinusDayDateTime(rankerEventTimeLimitDays))
      return enrichLogDF.filter($"ranker_event_time" > lit(thresholdTimestamp))
    }

    enrichLogDF
  }

  def joinAtomAndRankerLog(atomLogDF: DataFrame, rankerLogDF: DataFrame, rankerLogType: Ranker.Value): DataFrame = {

    if (rankerLogType == Ranker.CONVERSION) {
      throw new IllegalArgumentException(s"Not Allowed Ranker log type $rankerLogType")
    }

    atomLogDF.join(rankerLogDF, Seq("bid_request_id"), "right_outer")
      .select(
        $"bid_request_id",
        $"atom_raw_log",
        lit(null) cast "integer" as "atom_survivals_count",
        $"ranker_action_id",
        $"ranker_action_type",
        $"ranker_raw_log",
        $"ranker_event_time",
      )
  }

  def joinAtomClickAndAtomConvLog(atomClickForWeekDF: DataFrame, conversionLogDF: DataFrame, rankerLogType: Ranker.Value): DataFrame = {

    if (rankerLogType != Ranker.CONVERSION) {
      throw new IllegalArgumentException(s"Not Allowed Ranker log type $rankerLogType")
    }

    atomClickForWeekDF.as("atomClickDF").join(conversionLogDF.as("conversionLogDF"), Seq("ranker_action_id"), "right_outer")
      .select(
        $"atomClickDF.bid_request_id",
        $"atom_raw_log",
        lit(null) cast "integer" as "atom_survivals_count",
        $"ranker_action_id",
        $"conversionLogDF.ranker_action_type",
        $"conversionLogDF.ranker_raw_log",
        $"conversionLogDF.ranker_event_time",
      )

  }

  def write(idType: Identifier.Value, rankerLogType: Ranker.Value, resultDF: DataFrame, repartitionCount: Int = 500): Unit = {

    val saveBasePath = idType match {
      case Identifier.ADID => s"/ns2/bizrec/lookalike/atom_log/metric/enrich/atom_${rankerLogType}/prd"
      case Identifier.ACID => s"/ns2/bizrec_acid/lookalike/atom_log/metric/enrich/atom_${rankerLogType}/prd"
    }
    val savePath = s"${saveBasePath}/date=${logicalDateTime.getDate()}/hour=${logicalDateTime.getHour()}"

    resultDF
      .repartition(repartitionCount)
      .write
      .option("compression", "gzip")
      .mode("overwrite")
      .parquet(savePath)
  }
}


