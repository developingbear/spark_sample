package com.kakao.adrec.atom.metric
package enrich.batch

import common.domain.{Identifier, Ranker}
import common.utils.{LogicalDateTime, SparkJob}
import common.sparkmodule.io.{EnrichIO, RankerIO}
import common.sparkmodule.transformer.EnrichTransformer

object EnrichJob_ConversionAction extends SparkJob
  with RankerIO with EnrichIO
  with EnrichTransformer  {

  def main(args: Array[String]): Unit = {
    // init
    // ex: adid win 5000 "2023-03-03 01:10:10"
    val idType = Identifier.withName(args(0))
    val rankerType = Ranker.withName(args(1))
    val resultRepartitionCount = args(2).toInt
    val logicalDateTime = new LogicalDateTime(args(3))

    // spark job
    val rankerDF = readRankerLog(idType, rankerType, logicalDateTime)
    val enrichClickDFLast10Days = readEnrichLog(idType, Ranker.CLICK, logicalDateTime, 0, 240, 7)

    val enrichDF = transformEnrichClickAndRankerToEnrich(idType, rankerType, enrichClickDFLast10Days, rankerDF)

    writeEnrich(idType, rankerType, enrichDF, resultRepartitionCount, logicalDateTime)

    sparkStop()
  }
}
