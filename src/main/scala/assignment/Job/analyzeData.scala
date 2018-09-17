package assignment.Job

import assignment.Contexts
import org.apache.spark.sql.DataFrame
import Contexts.sqlCtx.implicits._
import co.theasi.plotly.{Plot, writer}
import co.theasi.plotly._


object analyzeData {

  /**
    *
    * @param df
    * @return
    */
  def dataAnalysis(df: DataFrame) = {

    implicit val server = new writer.Server {
      val credentials = writer.Credentials("pujaagarwal", "cyEFfXmdM967sARKIfI1")
      val url = "https://api.plot.ly/v2/"
    }


    //Number of customers per day
    //df.registerTempTable("analyseTable")
    df.createOrReplaceTempView("analyseTable")
    val firstAnalysis = Contexts.sqlCtx.sql("select max(Date) as Date,count(Date) as Number_Of_Customer from analyseTable group by Date")


    val xss: Seq[String] = firstAnalysis.select("Date").map(_.getString(0)).collect().toSeq
    val yss: Seq[Double] = firstAnalysis.select("Number_Of_Customer").map(_.getLong(0).asInstanceOf[Double]).collect().toSeq

    val plot = Plot().withScatter(xss, yss, ScatterOptions().mode(ScatterMode.Line).name("marker"))
    draw(plot, "CustomersPerDay", writer.FileOptions(overwrite = true))


    //Impact of weather on ridership

    val secondAnalysis = Contexts.sqlCtx.sql("select max(Date)as Date,max(maximumtemperature) as maximumtemperature ,max(minimumtemperature) as minimumtemperature," +
      "count(bikeid) as No_Of_bikerider from analyseTable group by Date,maximumtemperature,minimumtemperature")


    val xss1: Seq[String] = secondAnalysis.select("Date").map(_.getString(0)).collect().toSeq
    val yss0: Seq[Int] = secondAnalysis.select("maximumtemperature").map(_.getInt(0)).collect().toSeq
    val yss1: Seq[Int] = secondAnalysis.select("minimumtemperature").map(_.getInt(0)).collect().toSeq

    val yss2: Seq[Double] = secondAnalysis.select("No_Of_bikerider").map(_.getLong(0).asInstanceOf[Double]).collect().toSeq

    val plot1 = Plot().withScatter(xss1, yss0, ScatterOptions().mode(ScatterMode.Line).name("maximumtemperature"))
      .withScatter(xss1, yss1, ScatterOptions().mode(ScatterMode.Line).name("minimumtemperature"))
      .withScatter(xss1, yss2, ScatterOptions().mode(ScatterMode.Line).name("No_Of_bikerider"))
    draw(plot1, "WeatherAnalysis", writer.FileOptions(overwrite = true))


    //Average Temperature versus Number of cike riders

    val thirdAnalysis = Contexts.sqlCtx.sql("select max(averagetemperature) as averagetemperature, count(bikeid) as No_Of_bikerider from analyseTable group by averagetemperature")


    val yss3: Seq[Double] = thirdAnalysis.select("No_Of_bikerider").map(_.getLong(0).asInstanceOf[Double]).collect().toSeq
    val xss2 = thirdAnalysis.select("averagetemperature").map(_.getDouble(0)).collect().toSeq
    val plot2 = Plot().withScatter(xss2, yss3, ScatterOptions().mode(ScatterMode.Line).name("marker"))
    draw(plot2, "AverageTempVsUsers", writer.FileOptions(overwrite = true))


  }
}
