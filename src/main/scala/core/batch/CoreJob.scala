package com.kakao.adrec.atom.metric
package core.batch

import common.domain.{Identifier, Ranker}
import common.utils.{LogicalDateTime, SparkJob}
import common.sparkmodule.io.{CoreIO, DruidIO, EnrichIO}
import common.sparkmodule.transformer.{CoreTransformer, DruidTransformer}

import org.apache.spark.storage.StorageLevel

object CoreJob extends SparkJob
  with EnrichIO
  with CoreTransformer with DruidTransformer
  with CoreIO with DruidIO{

  def main(args: Array[String]): Unit = {
    // init
    // ex: adid win 5000 "2023-03-03 01:10:10"
    val idType = Identifier.withName(args(0))
    val rankerType = Ranker.withName(args(1))
    val resultRepartitionCount = args(2).toInt
    val logicalDateTime = new LogicalDateTime(args(3))

    // spark job
    val enrichDF = readEnrichLog(idType, rankerType, logicalDateTime)
    val coreDF = transformErichToCore(idType, rankerType, enrichDF)

    coreDF.persist(StorageLevel.DISK_ONLY)

    val filteredCoreDF = filterCoreToSave(rankerType, coreDF)
    writeCore(idType, rankerType, filteredCoreDF, resultRepartitionCount, logicalDateTime)

    if(idType == Identifier.ADID || rankerType == Ranker.CONVERSION || rankerType == Ranker.ACTION){
      val druidDF = transformCoreToDruid(idType, rankerType, coreDF)
      writeDruid(idType, rankerType, druidDF, logicalDateTime)
    }

    sparkStop()
  }
}
