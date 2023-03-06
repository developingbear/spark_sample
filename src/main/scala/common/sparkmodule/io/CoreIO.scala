package com.kakao.adrec.atom.metric
package common.sparkmodule.io

import common.domain.{Identifier, Ranker}

import common.utils.LogicalDateTime
import org.apache.spark.sql.DataFrame

trait CoreIO {
  def writeCore(idType: Identifier.Value,
                rankerType: Ranker.Value,
                coreDF: DataFrame,
                repartitionNum: Int,
                logicalDateTime: LogicalDateTime): Unit = {


    val savePath = (idType match {
      case Identifier.ADID => s"/ns2/bizrec/lookalike/atom_log/metric/core"
      case Identifier.ACID => s"/ns2/bizrec_acid/lookalike/atom_log/metric/core"
    }) + s"/atom_${rankerType}/prd/date=${logicalDateTime.getDate()}/hour=${logicalDateTime.getHour()}"

    coreDF
      .repartition(repartitionNum)
      .write
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(savePath)
  }
}
