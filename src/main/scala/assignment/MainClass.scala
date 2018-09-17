package assignment


object MainClass {

  def main(args: Array[String]): Unit = {


    // val firstJob =Job.readData.execute(args(0), args(1))

    val firstJob = Job.readData.execute(Properties.weatherDataPath, Properties.cityBikeDataPath)
  }
}
