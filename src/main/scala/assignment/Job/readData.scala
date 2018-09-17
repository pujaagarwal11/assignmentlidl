package assignment.Job

import assignment.ReadingCleaningMethods
import assignment.ReadingCleaningMethods.CityBike
import org.apache.spark.sql.{DataFrame, Dataset}

object readData {

  def execute(weatherInput: String, cityTaxiFileInput: String) = {

    //Read csv to dataframe

    //Weather Dataset

    val readWeatherFile: Dataset[ReadingCleaningMethods.Weather] = ReadingCleaningMethods.readWeatherCSVtoDataset(weatherInput)
    val weatherFinalDF = dataCleaning.weatherDataCleaning(readWeatherFile)

    //CityBikeDataset
    val readCityTaxiFile: Dataset[CityBike] = ReadingCleaningMethods.readBikeCSVtoDataset(cityTaxiFileInput)
    val cityBikeFinalDF = dataCleaning.cityBikeDataCleaning(readCityTaxiFile)

    //Join two dataset based on date

    val joinedDF: DataFrame = weatherFinalDF.join(cityBikeFinalDF,Seq("Date"))

    //Analysis of data

    val dataAnalysis= analyzeData.dataAnalysis(joinedDF)



  }

}
