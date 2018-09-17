package assignment

import com.typesafe.config.Config
import org.scalatest.{FlatSpec, Matchers}


class PropertiesTest extends FlatSpec with Matchers{

  "weatherConf file" should "be read from resources" in {
    val file = Properties.weather_conf
    file shouldBe a [Config]
  }

  "allowedDatePattern" should "return required date pattern" in {
    val allowedDatePattern = Properties.allowedDatePattern
    allowedDatePattern shouldEqual "^[0-9]{1}[-]{1}[0-9]{1}[-]{1}[0-9]{4}$"
  }

  "allowedDatePattern1" should "return required date pattern" in {
    val allowedDatePattern1 = Properties.allowedDatePattern1
    allowedDatePattern1 shouldEqual "^[0-9]{1}[/]{1}[0-9]{1}[/]{1}[0-9]{4}$"
  }


  "inputDateFormat" should "return the date format" in {
     val inputDateFormat = Properties.inputDateFormat
    inputDateFormat shouldEqual "M/d/yyyy HH:mm"
  }


  "outputDateFormat" should "return the date format" in {
    val outputDateFormat = Properties.outputDateFormat
    outputDateFormat shouldEqual "d-M-YYYY"
  }

  "weatherInputDateFormat" should "return the date format" in {
    val weatherInputDateFormat = Properties.weatherInputDateFormat
    weatherInputDateFormat shouldEqual "d/M/yyyy"
  }


  "weatherOutputDateFormat" should "return the date format" in {
    val weatherOutputDateFormat = Properties.weatherOutputDateFormat
    weatherOutputDateFormat shouldEqual "d-M-YYYY"
  }
}
