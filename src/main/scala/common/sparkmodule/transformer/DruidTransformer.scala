package com.kakao.adrec.atom.metric
package common.sparkmodule.transformer

import common.domain.{Identifier, Ranker}

import org.apache.spark.sql.functions.{coalesce, col, count, lit, struct, sum, to_json, window}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait DruidTransformer {
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def transformCoreToDruid(idType: Identifier.Value,
                           rankerType: Ranker.Value,
                           coreDF: DataFrame): DataFrame = {

//
    null
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
