package assignment

import org.apache.spark.sql.SparkSession

/**
  * Create Contexts
  */
object Contexts {

  val spark = SparkSession.builder().appName("assignment").master("local").getOrCreate()
  val sc = spark.sparkContext
  val sqlCtx = spark.sqlContext
}
