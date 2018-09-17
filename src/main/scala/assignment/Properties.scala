package assignment


import com.typesafe.config.ConfigFactory

/**
  * Properties
  */
object Properties {

  lazy val weather_conf = ConfigFactory.load("weatherConf")

  def allowedDatePattern = weather_conf.getString("weatherConf.allowedDatePattern")

  def allowedDatePattern1 = weather_conf.getString("weatherConf.allowedDatePattern1")

  def weatherColumnToDrop: Array[String] = ReadingCleaningMethods.getList(weather_conf.getString("weatherConf.weatherColumnToDrop"))

  def cityBikeColumnToDrop = ReadingCleaningMethods.getList(weather_conf.getString("weatherConf.cityBikeColumnToDrop"))

  def inputDateFormat = weather_conf.getString("weatherConf.inputDateFormat")

  def outputDateFormat = weather_conf.getString("weatherConf.outputDateFormat")

  def weatherInputDateFormat = weather_conf.getString("weatherConf.weatherInputDateFormat")

  def weatherOutputDateFormat = weather_conf.getString("weatherConf.weatherOutputDateFormat")

  def weatherDataPath = getClass.getResource("/input/weather_data_nyc_centralpark_2016.csv").getPath

  def cityBikeDataPath = getClass.getResource("/input/201601-citibike-tripdata.csv").getPath
}
