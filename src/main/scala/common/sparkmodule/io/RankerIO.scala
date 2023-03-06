package com.kakao.adrec.atom.metric
package common.sparkmodule.io

import common.domain.{Identifier, Ranker}
import common.utils.LogicalDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}

trait RankerIO{
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def readRankerLog(idType: Identifier.Value,
                    rankerType: Ranker.Value,
                    logicalDateTime: LogicalDateTime): DataFrame = {
    spark.sql(s"select * from biz_$idType.raw_km_filtered_$rankerType")
      .filter($"p_dt" === logicalDateTime.getDate())
      .filter($"p_hr" === logicalDateTime.getHour())
  }
}
