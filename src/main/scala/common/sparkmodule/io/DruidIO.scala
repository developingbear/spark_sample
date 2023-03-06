package com.kakao.adrec.atom.metric
package common.sparkmodule.io

import common.domain.{Identifier, Ranker}
import common.utils.LogicalDateTime
import org.apache.spark.sql.DataFrame

trait DruidIO {

  def writeDruid(idType: Identifier.Value,
                 rankerLog: Ranker.Value,
                 druidDF: DataFrame,
                 logicalDateTime: LogicalDateTime): Unit = {


    val savePath = (idType match {
      case Identifier.ADID =>
        s"/ns2/bizrec/lookalike/atom_log/metric/druid"
      case Identifier.ACID =>
        s"/ns2/bizrec_acid/lookalike/atom_log/metric/druid"
    }) + s"/atom_${rankerLog}/prd/date=${logicalDateTime.getDate()}/hour=${logicalDateTime.getHour()}"

    druidDF.write
      .mode("overwrite")
      .parquet(savePath)
  }
}
