package com.kakao.adrec.atom.metric
package enrich.deprecated

import common.domain.{Identifier, Ranker}
import common.utils.Enrich

object AtomConversionEnrichAcid {

  def main(args: Array[String]): Unit = {
    val logicalDateTimeStr = args(0)

    val enrich = new Enrich(Identifier.ACID, Ranker.CONVERSION, logicalDateTimeStr)

    val convDF = enrich.readRankerLog(Identifier.ACID, Ranker.CONVERSION)
    val actionDF = enrich.readRankerLog(Identifier.ACID, Ranker.ACTION)
    val convActionDF = convDF.union(actionDF)

    val atomClickForWeekDF = enrich.readEnrichLog(Identifier.ACID, Ranker.CLICK, 0, 240, 7)
    val atomConvDF = enrich.joinAtomClickAndAtomConvLog(atomClickForWeekDF, convActionDF, Ranker.CONVERSION)

    enrich.write(Identifier.ACID, Ranker.CONVERSION, atomConvDF, 1)

    enrich.stop()
  }
}

