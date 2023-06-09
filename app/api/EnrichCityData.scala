package api

import com.github.tototoshi.csv.{CSVReader, CSVWriter}

import java.io.FileOutputStream
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object EnrichCityData extends App {

  val outWrite = "newfile.csv".replace(".csv" , s"-new.csv")
  val outputWriter =  CSVWriter.open(new FileOutputStream(outWrite),"UTF-8")


  val reader = CSVReader.open("newfile.csv")
  val header = reader.iterator.next() ++ Seq("Country Name", "Country Code", "City Population", "Timezone", "Temperature", "Windspeed",
    "Winddirection", "Period Of Day", "Time Of Measurement")
  outputWriter.writeRow(header)
  val iterator = reader.iterator
  iterator.foreach { row =>
    val city = row.lift(0)
    city.foreach { city =>
      if (city.nonEmpty) {
        //        val details = Await.result(TestForCity.clientForCity(city), 10 minutes)
        //        println(details)
        Try {
          Await.result(TestForCity.clientForCity(city), 10.minutes)
        } match {
          case Success(result) => {
            result.map { obj =>
              val lat = (obj \ "latitude").asOpt[Double].getOrElse(0.0)
              val lon = (obj \ "longitude").asOpt[Double].getOrElse(0.0)
              val countryCode = (obj \ "country_code").asOpt[String].getOrElse("")
              val population = (obj \ "population").asOpt[Long].getOrElse(0)
              val countryName = (obj \ "country").asOpt[String].getOrElse("")
              val url: String = s"https://api.open-meteo.com/v1/forecast?latitude=$lat&longitude=$lon&current_weather=true"
              Try {
                Await.result(TestForCity.clientForWeather(url), 10 minutes)
              } match {
                case Success(value) => value.map { obj =>
                  val timezone = (obj \ "timezone").asOpt[String].getOrElse("")
                  val temp = ((obj \ "current_weather") \ "temperature").asOpt[Double].getOrElse(0.0)
                  val windspeed = ((obj \ "current_weather") \ "windspeed").asOpt[Double].getOrElse(0.0)
                  val winddirection = ((obj \ "current_weather") \ "winddirection").asOpt[Double].getOrElse(0.0)
                  val isDay = if (((obj \ "current_weather") \ "is_day").asOpt[Int].contains(1)) "Day" else "Night"
                  val timeOfMeasurement = ((obj \ "current_weather") \ "time").asOpt[String].getOrElse("")
                  outputWriter.writeRow(Seq(city, countryName, countryCode, population, timezone, temp, windspeed, winddirection, isDay, timeOfMeasurement))
                }
                case Failure(exception) =>
                  println(s"Error processing weather data for {$lat $lon} : ${exception.getMessage}")
                  outputWriter.writeRow(Seq(city, countryName, countryCode, population.toString, "", "", "", "", "", ""))
              }

            }
          }
          case Failure(exception) =>
            println(s"Error processing city '$city': ${exception.getMessage}")
            outputWriter.writeRow(Seq(city, "", "", "", "", "", "", "", "", ""))
        }





        //        Thread.sleep(1000)
      }


    }


  }
  WSClientN.close
  println(s"Finished writing in CSV")

}
