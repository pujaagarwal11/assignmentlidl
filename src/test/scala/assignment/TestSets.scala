package assignment

import org.apache.spark.sql.types.{StringType, StructField, StructType}


object TestSets {


  private val optionsMap = Map("header" -> "true", "delimiter" -> ",", "inferSchema" -> "false", "treatEmptyValuesAsNulls" -> "false", "nullValue" -> "")

  def dfWeatherInput = Contexts.sqlCtx.read.format("com.databricks.spark.csv")
    .options(optionsMap)
    .load(getClass.getResource("/input/weather_data_nyc_centralpark_2016.csv").getPath())


  val weatherSchema = StructType(List(
    StructField("date", StringType, true),
    StructField("maximum temperature", StringType, true),
    StructField("minimum temperature", StringType, true),
    StructField("average temperature", StringType, true),
    StructField("precipitation", StringType, true),
    StructField("snow fall", StringType, true),
    StructField("snow depth", StringType, true)))


}
