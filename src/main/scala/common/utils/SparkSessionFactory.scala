package com.kakao.adrec.atom.metric
package common.utils

import common.domain.{AdLog, AtomLog}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def getOrCreate(): SparkSession = {
    val spark = SparkSession.builder()
      .config(getConfig())
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer", "1024k")
      .config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.kryo.registrationRequired", "true")
      .getOrCreate()
    spark
  }

  private def getConfig(): SparkConf ={
    new SparkConf()
      .set("spark.network.timeout", "1000s")
      .set("spark.shuffle.manager", "tungsten-sort")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(
      Array(
        classOf[AdLog],
        classOf[AtomLog],
        classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
        classOf[org.apache.spark.sql.types.StructType],
        classOf[Array[org.apache.spark.sql.types.StructType]],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.BooleanType$"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        Class.forName("[[B"),
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.types.ArrayType],
        Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
        classOf[org.apache.spark.sql.catalyst.InternalRow],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        classOf[org.apache.spark.util.collection.BitSet],
        classOf[org.apache.spark.sql.types.DataType],
        classOf[Array[org.apache.spark.sql.types.DataType]],
        Class.forName("org.apache.spark.sql.types.NullType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.TimestampType$"),
//        Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"),
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
        Class.forName("scala.collection.immutable.Set$EmptySet$"),
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        Class.forName("java.lang.Class")
      )
    )
  }
}
