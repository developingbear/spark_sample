package com.kakao.adrec.atom.metric
package common.sparkmodule.io

import common.domain.{Identifier, Ranker}
import common.utils.LogicalDateTime

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer

trait EnrichIO{
  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  private def getEnrichBasePath(idType: Identifier.Value, rankerType: Ranker.Value): String = {
    idType match {
      case Identifier.ADID => s"/ns2/bizrec/lookalike/atom_log/metric/enrich/atom_${rankerType}/prd"
      case Identifier.ACID => s"/ns2/bizrec_acid/lookalike/atom_log/metric/enrich/atom_${rankerType}/prd"
    }
  }

  def readEnrichLog(idType: Identifier.Value,
                    rankerType: Ranker.Value,
                    logicalDateTime: LogicalDateTime,
                    startDiffHour: Int = 0,
                    endDiffHour: Int = 0,
                    rankerEventTimeLimitDays: Int = 0): DataFrame = {
    /**
     * startDiffHour 부터 endDiffHour 까지의 data를 path에 추가
     * ex) startDiffHour = 0, endDiffHour = 0 => executionTime의 data path(기본값)
     * ex) startDiffHour = 0, endDiffHour = 24 => executionTime의 현재 부터, 24시간 전까지의 data path
     * ex) startDiffHour = 1, endDiffHour = 10 => executionTime의 1시간 전부터, 10시간 전까지의 data path
     */
    val paths = ArrayBuffer[String]()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val enrichBasePath = getEnrichBasePath(idType, rankerType)

    for (i <- startDiffHour to endDiffHour) {
      val enrichFullPath = s"${enrichBasePath}/date=${logicalDateTime.getMinusHourDate(i)}/hour=${logicalDateTime.getMinusHour(i)}"
      if (fs.exists(new Path(enrichFullPath))) {
        paths += enrichFullPath
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


    // enriched log의 ranker_event_time를 기준으로 필터링
    if (rankerEventTimeLimitDays > 0) {
      val thresholdTimestamp = Timestamp.valueOf(logicalDateTime.getMinusDayDateTime(rankerEventTimeLimitDays))
      return enrichLogDF.filter($"ranker_event_time" > lit(thresholdTimestamp))
    }

    enrichLogDF
  }

  def writeEnrich(idType: Identifier.Value,
                  rankerType: Ranker.Value,
                  resultDF: DataFrame,
                  repartitionCount: Int = 1000,
                  logicalDateTime: LogicalDateTime): Unit = {

    val enrichBasePath = getEnrichBasePath(idType, rankerType)
    val savePath = s"${enrichBasePath}/date=${logicalDateTime.getDate()}/hour=${logicalDateTime.getHour()}"

    resultDF
      .repartition(repartitionCount)
      .write
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(savePath)
  }
}
