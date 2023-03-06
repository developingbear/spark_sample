package com.kakao.adrec.atom.metric
package enrich.deprecated


object Example_AtomEnrichedWinClick {

  def main(args: Array[String]): Unit = {
//    //TODO: args()로 변경
//    val executionDateTimeStr = "2023-01-01 01:10:10"
//    LogicalDateTime.init(executionDateTimeStr)
//
//    val idType = Identifier.ADID
//
//    val a = RankerLog.WIN
//
//    val spark = SparkSession.builder().getOrCreate()
//    import spark.implicits._
//
//    val atomLogDF = Enrich.readAtomLog(spark, idType)
//
//    val winLogDF = Enrich.readRankerLog(spark, idType, RankerLog.WIN)
//    val clickLogDF = Enrich.readRankerLog(spark, idType, RankerLog.CLICK)
//    val winClickDF = winLogDF.union(clickLogDF)
//
//    val atomWinClickDF = atomLogDF.join(winClickDF, $"bid_id" === $"bid_request_id", "right_outer")
//      .select(
//        $"bid_request_id",
//        $"raw_atom_log",
//        $"atom_response_time",
//        $"action_id",
//        $"action_type",
//        $"raw_ranker_log",
//        $"ranker_event_time",
//      )
//
//    // join 결과를 caching 하지 않으면, spark 내부에서 atom log를 2번 read하게 DAG을 변경함
//    atomWinClickDF.persist(StorageLevel.DISK_ONLY)
//
//    val atomWinDF = atomWinClickDF.filter($"action_type" === "win")
//    val atomClickDF = atomWinClickDF.filter($"action_type" === "click")
//
//    // repartitionNum: peak 시간대 13시를 기준으로 output size를 조정
//    Enrich.writeEnrichedMetricAsParquet(atomWinDF, idType, EnrichLog.ATOM_WIN, 2700)
//    Enrich.writeEnrichedMetricAsParquet(atomClickDF, idType, EnrichLog.ATOM_CLICK, 6)
//
//    atomWinClickDF.unpersist()
//
//    spark.close()
  }
}

