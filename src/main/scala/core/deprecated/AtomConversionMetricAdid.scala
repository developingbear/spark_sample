package com.kakao.adrec.atom.metric
package core.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.{Core, Enrich}

import org.apache.spark.storage.StorageLevel

object AtomConversionMetricAdid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(logicalDateTimeStr)
    val core = new Core(logicalDateTimeStr)

    val atomRankerDF = enrich.readEnrichLog(Identifier.ADID, Ranker.CONVERSION)
    val transformedAtomRankerDF = core.transformAtomRankerDF(Identifier.ADID, Ranker.CONVERSION, atomRankerDF)

    transformedAtomRankerDF.persist(StorageLevel.DISK_ONLY)

    core.writeRawMetric(Identifier.ADID, Ranker.CONVERSION, transformedAtomRankerDF, 1)
    core.writeMetric(Identifier.ADID, Ranker.CONVERSION, transformedAtomRankerDF)

    core.stop()
  }
}
