package assignment

import assignment.ReadingCleaningMethods.CityBike
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.scalatest._


class ReadingCleaningMethodsTest extends FlatSpec with Matchers {
  val inputDataFrame: DataFrame = TestSets.dfWeatherInput

  val rdd = inputDataFrame.rdd
  val framedDF = Contexts.sqlCtx.createDataFrame(rdd, TestSets.weatherSchema).toDF("date", "maximumtemperature", "minimumtemperature", "averagetemperature", "precipitation", "snowfall", "snowdepth")

  val weatherColumnToDrop: List[String] = Properties.weatherColumnToDrop.toList
  val weatherDropCol = weatherColumnToDrop.map(str => col(str))


  "readWeatherCSVtoDataframe method" should "return Weather Dataset" in {
    val readWeatherFile: Dataset[ReadingCleaningMethods.Weather] = ReadingCleaningMethods.readWeatherCSVtoDataset(Properties.weatherDataPath)
    readWeatherFile.show(false)
  }

  "readBikeCSVtoDataframe method" should "return City Bike Dataset" in {
    val readCityTaxiFile: Dataset[CityBike] = ReadingCleaningMethods.readBikeCSVtoDataset(Properties.cityBikeDataPath)
    readCityTaxiFile.show(false)
  }

  "dropColumns function" should "drop the required column" in {
    val dropColumnDF = ReadingCleaningMethods.dropColumns(framedDF, weatherDropCol)
    dropColumnDF.show(false)
  }

  "list values" should "be trimmed and added as keys to map" in {
    val fromList: String = "one, two , three  ,four    "
    ReadingCleaningMethods.getList(fromList) should equal(Array("one", "two", "three", "four"))

  }
}

