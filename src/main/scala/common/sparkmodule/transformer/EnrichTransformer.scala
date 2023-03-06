package com.kakao.adrec.atom.metric
package common.sparkmodule.transformer

import common.domain.{Identifier, Ranker}

import org.apache.spark.sql.functions.{col, get_json_object, lit, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait EnrichTransformer {
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._


  private def transformAtom(idType: Identifier.Value, atomLogDF: DataFrame): DataFrame = {
     atomLogDF
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

  private def transformRanker(idType: Identifier.Value, rankerType: Ranker.Value, rankerDF: DataFrame): DataFrame = {
    val transformedRankerDF = rankerDF
      .select(
        get_json_object(col("log_data"), "$.rawMessage.bid.bidRequestId") as "bid_request_id",

        (rankerType match {
          case Ranker.WIN | Ranker.VIMP | Ranker.CLICK =>
            get_json_object(col("log_data"), "$.rawMessage.action.actionId")
          case Ranker.CONVERSION | Ranker.ACTION =>
            get_json_object(col("log_data"), "$.rawMessage.action.actionChain[0].actionId")
        }) as "ranker_action_id",

        lit(rankerType.toString) as "ranker_action_type",
        $"log_data" as "ranker_raw_log",
        to_timestamp(get_json_object(col("log_data"), "$.processDttm"), "yyyyMMdd HHmmss SSS") as "ranker_event_time",
      )

    // raw data size를 줄이기 위해 조건에 따른 선필터

    // valid log만 분석, 내부정책
    var filteredRankerDF = transformedRankerDF.filter(get_json_object(col("log_data"), "$.valid") === "Y")

    if (idType == Identifier.ACID) {
      filteredRankerDF = filteredRankerDF
        .filter(get_json_object(col("ranker_raw_log"), "$.rawMessage.user.accid") =!= '0')
        .filter(get_json_object(col("ranker_raw_log"), "$.rawMessage.user.accid").isNotNull)
    }

    // WIN log의 volume이 매우 크고, 모든 종류의 goal, objectiveType에 대한 분석은 ADID log로 분석하고 있음
    // 따라서, ACID-WIN log에는 선필터 적용
    if (idType == Identifier.ACID && (rankerType == Ranker.WIN || rankerType == Ranker.VIMP)) {
      filteredRankerDF = filteredRankerDF
        .filter(get_json_object($"ranker_raw_log", "$.rawMessage.winner.goal") === "CONVERSION")
        .filter(get_json_object($"ranker_raw_log", "$.rawMessage.winner.objectiveType") === "PIXEL_AND_SDK")
    }

    if (rankerType == Ranker.ACTION)
      filteredRankerDF = filteredRankerDF
        .filter(get_json_object($"ranker_raw_log", "$.rawMessage.winner.objectiveDetailType") === "ADD_FRIEND")

    filteredRankerDF
  }

  def transformAtomAndRankerToEnrich(idType: Identifier.Value,
                                     rankerType: Ranker.Value,
                                     atomDF: DataFrame,
                                     rankerDF: DataFrame): DataFrame = {
    // win, vimp, click only
    if (rankerType == Ranker.CONVERSION || rankerType == Ranker.ACTION) {
      throw new IllegalArgumentException(s"Not Allowed Ranker log type $rankerType")
    }

    val transformedAtomDF = transformAtom(idType, atomDF)
    val transformedRankerDF = transformRanker(idType, rankerType, rankerDF)

    // transform 후, join 하여 enrich format 생성
    transformedAtomDF.join(transformedRankerDF, Seq("bid_request_id"), "right_outer")
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

  def transformEnrichClickAndRankerToEnrich(idType: Identifier.Value,
                                            rankerType: Ranker.Value,
                                            enrichClickDF: DataFrame,
                                            rankerDF: DataFrame): DataFrame = {

    // conversion, action only
    if (rankerType != Ranker.CONVERSION || rankerType != Ranker.ACTION) {
      throw new IllegalArgumentException(s"Not Allowed Ranker log type $rankerType")
    }

    val transformedRankerDF = transformRanker(idType, rankerType, rankerDF)

    // transform 후, join 하여 enrich format 생성
    enrichClickDF.as("enrichClickDF").join(transformedRankerDF.as("rankerDF"), Seq("ranker_action_id"), "right_outer")
      .select(
        $"enrichClickDF.bid_request_id",
        $"atom_raw_log",
        lit(null) cast "integer" as "atom_survivals_count",
        $"ranker_action_id",
        $"rankerDF.ranker_action_type",
        $"rankerDF.ranker_raw_log",
        $"rankerDF.ranker_event_time",
      )

  }
}
