package com.kakao.adrec.atom.metric
package common.sparkmodule.transformer

import common.domain._
import common.sparkmodule.udf.CoreUDF

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

trait CoreTransformer extends CoreUDF {
  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  // Core functions

  def transformErichToCore(idType: Identifier.Value, rankerType: Ranker.Value, enrichDF: DataFrame): DataFrame = {

    var transformedDF = transformRankerCommon(idType, enrichDF)
    transformedDF = transformAtomLog(idType, transformedDF)

    if (rankerType == Ranker.CONVERSION || rankerType == Ranker.ACTION)
      transformedDF = transformConversionAndActionRanker(transformedDF)

    // drop enrich cols
    transformedDF
      .drop(
        "bid_request_id",
        "atom_raw_log",
        "atom_survivals_count",
        "ranker_action_id",
        "ranker_action_type",
        "ranker_event_time",
        "ranker_raw_log",
      )
  }

  private def transformRankerCommon(idType: Identifier.Value, enrichDF: DataFrame): DataFrame = {

    val tempDF = enrichDF
      .select(
        $"*",
        from_json($"ranker_raw_log", Encoders.product[AdLog].schema) as "ranker"
      )

    val resultDF = idType match {
      case Identifier.ADID => tempDF.select(
        $"*",
        $"ranker.rawMessage.user.id" as "aid",
        $"ranker.rawMessage.device.adid"
      )
      case Identifier.ACID => tempDF.select(
        $"*",
        $"ranker.rawMessage.user.accid"
      )
    }

    resultDF.select(
      $"*",
      $"ranker.adPurpose",
      $"ranker.rawMessage.bid.bidRequestId",
      $"ranker.adGroupId",
      $"ranker.campaignId",
      $"ranker.rawMessage.bcon",
      $"ranker.rawMessage.user.gender",
      $"ranker.rawMessage.user.ageBand" as "age",
      $"ranker.rawMessage.user.activatedId",
      $"ranker.rawMessage.user.userDomainPolicy",
      $"ranker.rawMessage.winner.trackId",
      $"ranker.rawMessage.winner.adAccountId",
      $"ranker.rawMessage.winner.rankerType",
      $"ranker.rawMessage.winner.pricing",
      $"ranker.rawMessage.winner.price",
      $"ranker.rawMessage.winner.bidAmount",
      $"ranker.rawMessage.winner.chargeAmount",
      $"ranker.rawMessage.winner.campaignType",
      $"ranker.rawMessage.winner.goal",
      $"ranker.rawMessage.winner.objectiveType",
      $"ranker.rawMessage.winner.objectiveDetailType",
      $"ranker.rawMessage.winner.bidStrategy",
      $"ranker.rawMessage.winner.ranking.ad",
      $"ranker.rawMessage.rank.metrics",
      $"ranker.rawMessage.action.actionId",
      $"ranker.rawMessage.action.actionType",
      $"ranker.rawMessage.action.actionChain",
      $"ranker.rawMessage.action.actionDttm",
      $"ranker.rawMessage.device.deviceType",
      $"ranker.rawMessage.device.connectionType",
      $"ranker.rawMessage.device.dnt",
      $"ranker.rawMessage.device.lmt",
      $"ranker.rawMessage.ssp.inhouse",
      to_timestamp($"ranker.processDttm", "yyyyMMdd HHmmss SSS") as "eventTime",
      $"ranker.rawMessage.ssp.tagId" as "slot",
      $"ranker.rawMessage.dsp.placement",
      $"ranker.rawMessage.winner.objectiveValue",
      $"ranker.rawMessage.action.click_conv_diff_days",

      when($"ranker_action_type".isin("win", "conversion", "action"), lit(1)).otherwise(0) as "imp",
      when($"ranker_action_type".isin("vimp", "conversion", "action"), lit(1)).otherwise(0) as "vimp",
      when($"ranker_action_type".isin("click", "conversion", "action"), lit(1)).otherwise(0) as "click",

      /**
       * sample: "metrics":"up=0;at=pb_talk_bizboard;"
       * at: nullable
       */
      when(
        regexp_extract(get_json_object($"ranker.rawMessage.rank.metrics", "$.ranker.rawMessage.rank.metrics"), "at=([^;]*)", 0) === "", lit(null)
      )
        .otherwise(regexp_extract(get_json_object($"ranker.rawMessage.rank.metrics", "$.ranker.rawMessage.rank.metrics"), "at=([^;]*)", 0)) as "at",

      /**
       * sample: "atom" = true
       */
      when($"ranker.rawMessage.winner.rankerType" === "danke", get_json_object($"ranker.rawMessage.rank.metrics", "$.atom") cast "String")
        .when($"ranker.rawMessage.winner.rankerType" === "ava", when(strToMapUDF($"ranker.rawMessage.rank.metrics")("atom") === "1", lit("true"))
          .otherwise(lit("false")))
        .otherwise(lit("Empty")) as "atomSuccess",

      /**
       * sample: "random":{"bucket":"rb0","random":false}
       */
      when($"ranker.rawMessage.winner.rankerType" === "danke", get_json_object($"ranker.rawMessage.rank.metrics", "$.random.random"))
        .when($"ranker.rawMessage.winner.rankerType" === "ava", when(get_json_object($"ranker.rawMessage.rank.metrics", "$.random.random") === "1", "true").otherwise(lit("false")))
        .otherwise("Empty") as "random",

      /**
       * sample: "qs_eval":{"qs":1,"valid":true,"pcvr":0.024748,"adpcvr":0.020733,"bucket":"vb2","model":"dp_pur_ftrl_cvr_v1.0.1_adid","ad_model":"dp_pur_ftrl_cvr_v1.0.0_ad"}
       * qs_eval: nullable
       */
      get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.qs_eval.bucket") as "qseBucket",
      get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.qs_eval.valid") cast "Boolean" as "qseValid",
      get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.qs_eval.qs").cast("Double") * 100 as "qseValueDim",

      // common metrics
      // BidAmount
      // CPC인 경우만 집계한다
      when($"ranker.rawMessage.winner.pricing" === "CPC",
        when($"ranker.rawMessage.action.actionType" === "click", $"ranker.rawMessage.winner.bidAmount")
          .otherwise(lit(0.0))
      ) as "ba",

      // BidAmount from win
      // CPC인 경우만 집계한다
      when($"ranker.rawMessage.winner.pricing" === "CPC",
        when($"ranker.rawMessage.action.actionType" === "win" || $"ranker.rawMessage.action.actionType" === "rimp", get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.ba"))
          .otherwise(lit(0.0))
      ) as "baFromWin", // end of pricing condition

      // win pctr metrics
      when($"ranker.rawMessage.action.actionType" === "win" || $"ranker.rawMessage.action.actionType" === "rimp",
        when($"ranker.rawMessage.winner.rankerType" === "danke", get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.pctr").cast("Double"))
          .when($"ranker.rawMessage.winner.rankerType" === "ava", strToMapUDF($"ranker.rawMessage.winner.ranking.ad")("p").cast("Double"))
          .otherwise(lit(0.0))
      ).otherwise(lit(0.0)) as "pctr",

      // imp qse value metrics
      when(($"ranker.rawMessage.action.actionType" === "win" || $"ranker.rawMessage.action.actionType" === "rimp"),
        get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.qs_eval.qs").cast("Double")
      ).otherwise(lit(0.0)) as "qseValue",

      // imp pcvr metrics
      when(($"ranker.rawMessage.action.actionType" === "win" || $"ranker.rawMessage.action.actionType" === "rimp"),
        get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.qs_eval.pcvr").cast("Double")
      ).otherwise(lit(0.0)) as "qseImpPcvr",

      // click pcvr metrics
      when($"ranker.rawMessage.action.actionType" === "click",
        get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.qs_eval.pcvr").cast("Double")
      ).otherwise(lit(0.0)) as "qseClickPcvr",

      // click adPcvr metrics
      when($"ranker.rawMessage.action.actionType" === "click",
        get_json_object($"ranker.rawMessage.winner.ranking.ad", "$.qs_eval.adpcvr").cast("Double")
      ).otherwise(lit(0.0)) as "qseClickAdPcvr",

      // actionLocation dimension
      when($"ranker.rawMessage.action.actionChain".isNull, $"ranker.rawMessage.action.actionLocation")
        .otherwise($"ranker.rawMessage.action.actionChain"(0).getItem("actionLocation")) as "actionLocation",

      //retargeting dimension & measure
      $"ranker.rawMessage.winner.creativeId",
      from_json($"ranker.rawMessage.dsp.retargetingMetrics", Encoders.product[RetargetingMetrics].schema) as "retargetingMetrics"
    )
      .select(
        $"*",
        $"retargetingMetrics.cmm"($"ranker.rawMessage.winner.creativeId") as "creativeMetricMap",
        $"retargetingMetrics.cmm"($"ranker.rawMessage.winner.creativeId")("ti") as "testId",
        $"retargetingMetrics.cmm"($"ranker.rawMessage.winner.creativeId")("tn") as "testName",
        $"retargetingMetrics.cmm"($"ranker.rawMessage.winner.creativeId")("tv") as "testVersion",
        $"retargetingMetrics.cmm"($"ranker.rawMessage.winner.creativeId")("bn") as "bucketName",
        $"retargetingMetrics.cmm"($"ranker.rawMessage.winner.creativeId")("rmn") as "recModelName",
        $"retargetingMetrics.cmm"($"ranker.rawMessage.winner.creativeId")("pms") as "pms",
      ) //measure(metric)
      .withColumn("dupProdCnt",
        when($"actionType" === "vimp",
          coalesce($"creativeMetricMap.dpc", lit(0))
        ).otherwise(lit(0)))

      .withColumn("totProdCnt",
        when($"actionType" === "vimp",
          when($"pms".isNotNull, size($"pms"))
            .otherwise(lit(0))
        ).otherwise(lit(0)))

      .withColumn("retProdCnt",
        when($"actionType" === "vimp",
          when($"pms".isNotNull, getRetargetProductCountUDF($"pms"))
            .otherwise(lit(0))
        ).otherwise(lit(0)))

      .withColumn("recProdCnt",
        when($"actionType" === "vimp",
          when($"pms".isNotNull, $"totProdCnt" - $"retProdCnt")
            .otherwise(lit(0))
        ).otherwise(lit(0)))

      //drop unused ranker cols
      .drop(
        "ranker",
        "retargetingMetrics",
        "creativeMetricMap",
        "pms"
      )
  }

  private def transformAtomLog(idType: Identifier.Value, atomClickDF: DataFrame): DataFrame = {
    val tempDF = atomClickDF
      .withColumn("atom", from_json($"atom_raw_log", Encoders.product[AtomLog].schema))

    val resultDF = idType match {
      case Identifier.ADID => tempDF.select(
        $"*",
        $"atom.bid"
      )
      case Identifier.ACID => tempDF.select(
        $"*",
        $"atom.acidBid" as "bid",
        $"atom.acidBid",
      )
    }

    resultDF.select(
      $"*",
      $"atom.ln",
      $"atom.unknowns",
      $"atom.survivals",
      $"atom.tagCount",
      $"atom.tcs",
      $"atom.tcc" divide 100 cast "Integer" as "tccg",
      $"atom.tcr" divide 100 cast "Integer" as "tcrg",
      $"atom.reqElapsed",
      $"atom.convBucket",
      $"atom.trtBucket",
      $"atom.tagCount" divide 100 cast "Integer" as "tagCountGroup",
      $"atom.version",
      $"atom.an" as "autobidBucket",
      $"atom.pm" as "populationModel",
      $"atom.hs" as "idHash",
      $"atom.stateInfoDttm",
      $"atom.tes",
      convertIdTypeBitToReadableUDF($"atom.tes") as "tagExistStatus",
      $"atom.ies",
      convertIdTypeBitToReadableUDF($"atom.ies") as "idExistStatus",
      $"atom.tcom",
      $"atom.tccm",
      $"atom.tcrm",
      $"atom.hsm",
      to_timestamp($"atom.responseTime", "yyyy-MM-dd'T'HH:mm:ssX") as "atomEventTime",
      $"atom.user.acidDnt",
      $"atom.user.potentialPopulation",

      $"atom.survivals"($"adGroupId") as "survival",
      $"atom.survivals"($"adGroupId")("sv") as "sv",
      $"atom.survivals"($"adGroupId")("cms") as "cms",
      $"atom.survivals"($"adGroupId")("trt") as "trt",
      $"atom.survivals"($"adGroupId")("customTarget") as "customTarget",
      $"atom.survivals"($"adGroupId")("cos")("v") as "cosValid",
      $"atom.survivals"($"adGroupId")("cos")("w") as "cosValue",
      $"atom.survivals"($"adGroupId")("cos")("m") as "cosModel",
      $"atom.survivals"($"adGroupId")("cos")("l") as "cosLevel",
      $"atom.survivals"($"adGroupId")("cos")("b") as "cosBucket",
      coalesce($"atom_survivals_count", size($"atom.survivals")) as "survivalsCnt",
      coalesce($"atom.unknowns", lit(-1)) as "unknownsCnt",
      mapToTagMapUDF($"atom.survivals"($"adGroupId")("matchTag")) as "matchTags",
      $"atom.survivals"($"adGroupId")("cl") as "cutoffLevel",
    )
      .select(
        $"*",

        /**
         * match_level & match_model
         *
         * sample: ae.7257844840508572198#AINN.29
         * , ae.1221914281330557110#ADNN.1, ,
         */
        when(size($"survival.matchTag") > 0,
          regexp_extract($"survival.matchTag"(0), ".*\\..*\\.([0-9]*)", 1)) as "matchLevel1",

        when(size($"survival.matchTag") > 0,
          regexp_extract($"survival.matchTag"(0), ".*\\..*#(.*)\\..*", 1)) as "matchModel1",

        when(size($"survival.matchTag") > 1,
          regexp_extract($"survival.matchTag"(1), ".*\\..*\\.([0-9]*)", 1)) as "matchLevel2",

        when(size($"survival.matchTag") > 1,
          regexp_extract($"survival.matchTag"(1), ".*\\..*#(.*)\\..*", 1)) as "matchModel2",

        when(size($"survival.matchTag") > 2,
          regexp_extract($"survival.matchTag"(2), ".*\\..*\\.([0-9]*)", 1)) as "matchLevel3",

        when(size($"survival.matchTag") > 2,
          regexp_extract($"survival.matchTag"(2), ".*\\..*#(.*)\\..*", 1)) as "matchModel3",

        when(size($"survival.matchTag") > 3,
          regexp_extract($"survival.matchTag"(3), ".*\\..*\\.([0-9]*)", 1)) as "matchLevel4",

        when(size($"survival.matchTag") > 3,
          regexp_extract($"survival.matchTag"(3), ".*\\..*#(.*)\\..*", 1)) as "matchModel4",

        when(size($"survival.matchTag") > 4,
          regexp_extract($"survival.matchTag"(4), ".*\\..*\\.([0-9]*)", 1)) as "matchLevel5",

        when(size($"survival.matchTag") > 4,
          regexp_extract($"survival.matchTag"(4), ".*\\..*#(.*)\\..*", 1)) as "matchModel5",

        when(size($"survival.matchTag") > 5,
          regexp_extract($"survival.matchTag"(5), ".*\\..*\\.([0-9]*)", 1)) as "matchLevel6",

        when(size($"survival.matchTag") > 5,
          regexp_extract($"survival.matchTag"(5), ".*\\..*#(.*)\\..*", 1)) as "matchModel6",

        when(size($"survival.customTarget") > 0, lit(true)).otherwise(lit(false)) as "useCustomTargeting",
      )
      // drop unused atom cols
      .drop(
        "atom",
        "survivals",
        "joinActionId",
      )
  }

  private def transformConversionAndActionRanker(transformedDF: DataFrame): DataFrame = {
    transformedDF
      //      .withColumn("")
      .select(
        $"*",
        $"bidRequestId" as "convBidId",
        $"adPurpose" as "convAdPurpose",
        $"campaignType" as "convCampaignType",
        $"goal" as "convGoal",
        $"objectiveType" as "convObjectiveType",
        $"objectiveDetailType" as "convObjectiveDetailType",
        $"actionDttm" as "convActionEventTime",
        $"actionType" as "convActionType",
        $"actionChain"(0).getItem("actionDttm") as "lastActionDttm",
        $"actionChain"(0).getItem("actionId") as "lastActionId",
        $"bcon.event_code" as "bconEventCode",
        $"bcon.params.krw_total_price" as "conversionValue",
        coalesce($"bcon.conv_iris_click.click_conv_diff_days", $"click_conv_diff_days") as "clickConvDiffDays",
        $"bcon.props.usable_only_stats" as "usableOnlyStats",
        when(coalesce($"bcon.conv_iris_click.click_conv_diff_days", $"click_conv_diff_days").isNotNull,
          when(coalesce($"bcon.conv_iris_click.click_conv_diff_days", $"click_conv_diff_days") === 0, "D")
            .when(coalesce($"bcon.conv_iris_click.click_conv_diff_days", $"click_conv_diff_days") < 7, "7D")
            .otherwise("ND")
        ).otherwise("Empty") as "delayType"
      )
      .drop(
        "acidBid",
        "atomEventTime",
        "convBucket",
        "ies",
        "tes",
        "survival",
        "survivalsCnt",
        "trtBucket",
        "unknowns",
        "bid",
        "customTarget",
        "matchTags",
        "tagCount",
        "unknownsCnt",
      )
  }

  def filterCoreToSave(rankerType: Ranker.Value, coreDF: DataFrame): DataFrame = {
    var filteredDF = coreDF
    if (rankerType == Ranker.WIN)
      filteredDF = filteredDF
        .filter($"goal" === "CONVERSION")
        .filter($"objectiveType" === "PIXEL_AND_SDK")

    if (rankerType == Ranker.CONVERSION)
      filteredDF = filteredDF
        .filter($"convGoal" === "CONVERSION")

    filteredDF
  }

  //  private def transformAtomLogWithConv(atomClickDF: DataFrame): DataFrame = {
  //
  //    val tempDF = atomClickDF
  //      // atom_raw_log를 AtomLog로 변환하고 제거
  //      .select(
  //        $"*",
  //        from_json($"atom_raw_log", Encoders.product[AtomLog].schema) as "atom",
  //      )
  //      .drop($"atom_raw_log")
  //      .select(
  //        $"*",
  //        $"joinActionId" as "actionId",
  //        lit("Empty") as "ln",
  //        lit("Empty") as "sv",
  //        lit(false) as "cms",
  //        $"atom.an" as "autobidBucket",
  //        $"atom.pm" as "populationModel",
  //        $"atom.survivals" as "survivals",
  //      )
  //
  //      .withColumn("cutoffLevel", $"survival.cl")
  //      .withColumn("stateInfoDttm", $"atom.stateInfoDttm")
  //      .withColumn("tcs", $"atom.tcs")
  //      .withColumn("tag_exist_status", convertIdTypeBitToReadableUDF($"atom.tes"))
  //      .withColumn("id_exist_status", convertIdTypeBitToReadableUDF($"atom.ies"))
  //      .withColumn("tccg", $"atom.tcc" divide 100 cast "Integer")
  //      .withColumn("tcrg", $"atom.tcr" divide 100 cast "Integer")
  //      .withColumn("version", coalesce($"atom.version", lit("Empty")))
  //      .withColumn("reqElapsed", coalesce($"atom.reqElapsed", lit(-1)))
  //      .withColumn("tagCountGroup", coalesce($"atom.tagCount" divide 100 cast "Integer", lit(-1)))
  //      .withColumn("useCustomTargeting",
  //        when(size($"survival.customTarget") > 0, lit(true)).otherwise(lit(false))
  //      )
  //      .withColumn("useCustomTargeting", coalesce(col("useCustomTargeting"), lit(false)))
  //      .withColumn("idHash", $"atom.hs")
  //      .withColumn("acidDnt", $"atom.user.acidDnt")
  //      .withColumn("potentialPopulation", $"atom.user.potentialPopulation")
  //      .withColumn("cosValid", $"survival.cos.v")
  //      .withColumn("cosValue", $"survival.cos.w")
  //      .withColumn("cosModel", $"survival.cos.m")
  //      .withColumn("cosLevel", $"survival.cos.l")
  //      .withColumn("cosBucket", $"survival.cos.b")
  //      .withColumn("trt", $"survival.trt")
  //
  //
  //    /**
  //     * match_level & match_model
  //     *
  //     * sample: ae.7257844840508572198#AINN.29
  //     * , ae.1221914281330557110#ADNN.1, ,
  //     */
  //
  //    var resultDF = tempDF
  //    for (i <- 1 to 6) {
  //      resultDF = resultDF
  //        .withColumn(s"match_level_${i}",
  //          when(size($"survival.matchTag") > (i - 1),
  //            regexp_extract($"survival.matchTag"(i - 1), ".*\\..*\\.([0-9]*)", 1))
  //        )
  //        .withColumn(s"match_model_${i}",
  //          when(size($"survival.matchTag") > (i - 1),
  //            regexp_extract($"survival.matchTag"(i - 1), ".*\\..*#(.*)\\..*", 1))
  //        )
  //    }
  //
  //    resultDF.drop($"atom")
  //  }

}
