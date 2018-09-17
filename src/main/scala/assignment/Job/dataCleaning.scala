package assignment.Job

import assignment.ReadingCleaningMethods.{CityBike, Weather}
import assignment.{Properties, ReadingCleaningMethods}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


object dataCleaning {

  //variables

  // get the pattern for date
  val allowedDatePattern = Properties.allowedDatePattern
  val allowedDatePattern1 = Properties.allowedDatePattern1
  val inputDateFormat = Properties.inputDateFormat
  val outputDateFormat = Properties.outputDateFormat
  val weatherInputDateFormat = Properties.weatherInputDateFormat
  val weatherOutputDateFormat = Properties.weatherOutputDateFormat

  //get the columns name to drop
  val weatherColumnToDrop: List[String] = Properties.weatherColumnToDrop.toList
  val weatherDropCol = weatherColumnToDrop.map(str => col(str))

  val cityBikeColumnToDrop: List[String] = Properties.cityBikeColumnToDrop.toList
  val cityBikeDropCol = cityBikeColumnToDrop.map(str => col(str))

  //Processing/Cleaning of weather data
  def weatherDataCleaning(weatherdf: Dataset[Weather]): DataFrame = {
    weatherdf.repartition(2000)
    val df: DataFrame = weatherdf.filter(col("date").isNotNull)
      .filter(col("averagetemperature").isNotNull)
      .filter(col("date").rlike(allowedDatePattern) || col("date").rlike(allowedDatePattern1))
      .withColumn("Date", (when(col("date").rlike(allowedDatePattern1), from_unixtime(unix_timestamp(lit(col("date")), weatherInputDateFormat), weatherOutputDateFormat))
        .otherwise(col("date"))))

    df.repartition(2000)

    // get the final dataframe with required columns
    val finalWeatherDf: DataFrame = ReadingCleaningMethods.dropColumns(df, weatherDropCol)
    finalWeatherDf

  }

  //Processing/Cleaning of city bike data
  def cityBikeDataCleaning(cityBikedf: Dataset[CityBike]): DataFrame = {
    cityBikedf.repartition(2000)
    val df1: DataFrame = cityBikedf.filter(col("tripduration").isNotNull || col("tripduration") > 0)
      .filter(col("starttime").isNotNull || col("starttime").rlike(inputDateFormat))
      .filter(col("stoptime").isNotNull || col("stoptime").rlike(inputDateFormat))
      .filter(col("startstationid").isNotNull)
      .filter(col("startstationname").isNotNull)
      .filter(col("usertype") === "Customer" || col("usertype") === "Subscriber")
      .withColumn("Date", from_unixtime(unix_timestamp(lit(col("starttime")), inputDateFormat), outputDateFormat))
    df1.repartition(2000)
    // get the final dataframe with required columns
    val finalCityBikeDf: DataFrame = ReadingCleaningMethods.dropColumns(df1, cityBikeDropCol)
    finalCityBikeDf

  }


}
