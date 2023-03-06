package com.kakao.adrec.atom.metric
package common.sparkmodule.transformer

import common.domain.{Identifier, Ranker}

import org.apache.spark.sql.functions.{coalesce, col, count, lit, struct, sum, to_json, window}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait DruidTransformer {
  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def transformCoreToDruid(idType: Identifier.Value,
                           rankerType: Ranker.Value,
                           coreDF: DataFrame): DataFrame = {

    ((idType, rankerType) match {
      case (_, Ranker.CONVERSION) || (_, Ranker.ACTION) =>
        coreDF
          .groupBy(
            getDimensions() ++ List($"bconEventCode", $"convActionType", $"delayType", $"usableOnlyStats"): _*
          ).agg(
          count("*") as (idType match {
            case Identifier.ADID => "conversion"
            case Identifier.ACID => "acidConversion"
          }),
          sum("conversionValue") as (idType match {
            case Identifier.ADID => "conversionValue"
            case Identifier.ACID => "acidConversionValue"
          }),
          sum("chargeAmount") as "chargeAmount",
        )

      case (Identifier.ADID, _) =>
        coreDF
          .groupBy(getDimensions(): _*)
          .agg(
            count($"*") as "count",
            sum("tagCount") as "tagCount",

            // sum을 계산하지만, column suffix만 Avg
            // 이후 Turnilo에서 해당 값을 가지고 평균을 계산해서 보여주기 때문에 column명만 미리 Avg로 변경
            sum("survivalsCnt") as "survivalsAvg",
            sum("unknownsCnt") as "unknownsAvg",

            sum("chargeAmount") as "chargeAmount",
            sum("price") as "rs",
            sum("pctr") as "pctr",
            sum("ba") as "ba",
            sum("baFromWin") as "baFromWin",
            sum("imp") as "imp",
            sum("vimp") as "vimp",
            sum("click") as "click",
            sum("qseValue") as "qseValue",
            sum("qseClickPcvr") as "qseClickPcvr",
            sum("qseImpPcvr") as "qseImpPcvr",
            sum("qseClickAdPcvr") as "qseClickAdPcvr",
          )
    }).withColumn("eventTime", $"window.start")
      .drop($"window")
      .select(
        to_json(struct($"*")) as "result"
      )
  }

  private def getDimensions(): List[Column] = {
    List(
      col("adGroupId"),
      col("campaignId"),
      col("gender"),
      col("age"),
      col("trackId"),
      col("userDomainPolicy"),
      col("adAccountId"),
      col("rankerType"),
      col("pricing"),
      col("atomSuccess"),
      col("campaignType"),
      col("deviceType"),
      col("connectionType"),
      col("goal"),
      col("objectiveType"),
      col("objectiveDetailType"),
      col("random"),
      col("bidStrategy"),
      col("qseValueDim"),
      col("qseBucket"),
      col("qseValid"),
      col("inhouse"),
      col("activatedId"),
      coalesce(col("lmt"), lit(false)).as("lmt"),
      coalesce(col("dnt"), lit(false)).as("dnt"),
      window(col("eventTime"), "1 minute"),
      col("slot"),
      col("placement"),
      col("at"),
      col("objectiveValue"),
      col("actionLocation"),
      coalesce(col("trt"), lit("Empty")).as("trackerType")
    )
  }
}
