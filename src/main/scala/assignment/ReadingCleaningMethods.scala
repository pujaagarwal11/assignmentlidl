package assignment

import assignment.Contexts.spark.implicits._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object ReadingCleaningMethods {

  // case class for generating weather dataset

  case class Weather(date: String, maximumtemperature: String, minimumtemperature: Long, averagetemperature: String, precipitation: String, snowfall: String, snowdepth: String)

  // case class for genearting CityBike dataset

  case class CityBike(tripduration: String, starttime: String, stoptime: String, startstationid: String, startstationname: String, startstationlatitude: String, startstationlongitude: String, endstationid: String, endstationname: String, endstationlatitude: String, endstationlongitude: String, bikeid: String, usertype: String, birthyear: String, gender: String)


  /**
    *
    * @param path
    * @return Dataset[Weather]
    *         reads csv to Dataset
    */
  def readWeatherCSVtoDataset(path: String): Dataset[Weather] = {
    val WeatherDf: Dataset[Weather] = Contexts.spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("charset", "UTF8")
      .option("delimiter", ",")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .csv(path)
      .toDF("date", "maximumtemperature", "minimumtemperature", "averagetemperature", "precipitation", "snowfall", "snowdepth")
      .as[Weather]
    WeatherDf
  }

  /**
    *
    * @param path
    * @return Dataset[CityBike]
    *         reads csv to Dataset
    */
  def readBikeCSVtoDataset(path: String): Dataset[CityBike] = {
    val cityBikeDf = Contexts.spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("charset", "UTF8")
      .option("delimiter", ",")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .csv(path)
      .toDF("tripduration", "starttime", "stoptime", "startstationid", "startstationname", "startstationlatitude", "startstationlongitude", "endstationid", "endstationname", "endstationlatitude", "endstationlongitude", "bikeid", "usertype", "birthyear", "gender")
      .as[CityBike]
    cityBikeDf
  }

  /**
    * Drops one or multiple columns from a dataframe
    *
    * @param df Dataframe to be passed
    * @param columns Columns to be dropped
    * @return DataFrame without the specified columns
    */

  def dropColumns(df: DataFrame, columns: List[Column]): DataFrame = columns match {
    case Nil => df
    case _ => {
      var dataFrame = df
      for (c <- columns) {
        dataFrame = dataFrame.drop(c)
      }
      dataFrame
    }
  }

  /**
    * Use this method when getting properties from config file. allows for spaces to be trimmed
    * e.g. "hello, my,    name, is,whatever" >> List("hello", "my", "name", "is", "whatever")
    *
    * @param list
    * @return Array[String]
    */
  def getList(list: String): Array[String] = {
    list.split(",").map(_.trim)
  }

  /*
    def replaceWhiteSpace(df: Dataset[Weather]) = {
      var newDf: Dataset[Weather] = df
      for (col <- df.columns) {
       // val n: DataFrame =newDf.withColumnRenamed(col,col.replaceAll("\\s",""))
        val  n = newDf.withColumnRenamed(col, col.replaceAll("\\s", ""))
          .withColumnRenamed(col, col.replaceAll("[\\r\\n]", ""))
      }
      newDf
    }*/


}
