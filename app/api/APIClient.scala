package api

import play.api.libs.json.{JsObject, JsValue}
import play.libs.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.language.postfixOps


object APIClient {

  // CITY DETAILS

  def cityResult(city:String) = {
    // CLIENT
    def clientForCity(city: String) = {
      val apiUrl = s"https://geocoding-api.open-meteo.com/v1/search?name=$city&count=1"
      WSClientN.client
        .url(apiUrl)
        .get()
        .map { response =>
          response.status match {
            case 200 =>
              val resultFromResponse = (response.body[JsValue].as[JsObject] \ "results").as[Seq[JsObject]]
              resultFromResponse
            case _ =>
              val errorMessage = s"Error: ${response.status} ${response.statusText}"
              new Exception(errorMessage)
              Seq()
          }
        }
    }
    // RESULTS
    val cityRes = Await.result(clientForCity(city), 10 minutes)

    val cityName = cityRes.map{obj =>
      (obj \ "name").asOpt[String].getOrElse("")
    }
    val longitudeValue = cityRes.map {obj=>
      (obj \ "longitude").as[Double]
    }
    val latitudeValue = cityRes.map {obj=>
      (obj \ "latitude").as[Double]
    }
    val countryName = cityRes.map{obj=>
      (obj \ "country").asOpt[String].getOrElse("")
    }
    val population = cityRes.map{obj =>
      (obj \ "population").as[Int]
    }


    def defineUrl(lat: String, lon: String): String = {
      val url: String = s"https://api.open-meteo.com/v1/forecast?latitude=$lat&longitude=$lon&current_weather=true"
      url
    }

    val resultFromWeather = Await.result(clientForWeather(defineUrl(longitudeValue.mkString,latitudeValue.mkString)), 10 minutes)

    val temp = resultFromWeather.map { obj =>
      ((obj \ "current_weather") \ "temperature").as[Double]
    }
    val windSpeed = resultFromWeather.map { obj =>
      ((obj \ "current_weather") \ "windspeed").as[Double]
    }
    val isDay = resultFromWeather.map { obj =>
      if (((obj \ "current_weather") \ "is_day").asOpt[Int].contains(1)) "Day" else "Night"
    }
    val timezone = resultFromWeather.map{obj =>
      (obj \ "timezone").as[String]
    }
    val timeOfMeasurement = resultFromWeather.map{obj =>
      ((obj \ "current_weather") \ "time").as[String]
    }




    Json.stringify(Json.toJson(Map(
      "name" -> cityName,
      "country" -> countryName,
      "temp" -> temp,
      "windSpeed" -> windSpeed,
      "isDay" -> isDay,
      "timezone" -> timezone,
      "timeOfMeasurement" -> timeOfMeasurement,
      "population" -> population)))


  }

  // WEATHER DETAILS

//  def weatherDetails(city:String) = {
    def clientForWeather(url: String) = {
      val result = WSClientN.client
        .url(url)
        .get()
        .map { response =>
          response.status match {
            case 200 =>
              val resultati = response.body[JsValue].as[JsObject]
              Future.successful(resultati)
            case _ => {
              val errorMessage = s"Error: ${response.status} ${response.statusText}"
              Future.failed(new Exception(errorMessage))
            }
          }
        }
      result
    }

//    val longitude:String = cityResult(city).get("longitude").flatMap(_.headOption).toString
//    val latitude:String = cityResult(city).get("latitude").flatMap(_.headOption).toString
//
//    def defineUrl(lat: String, lon: String): String = {
//      val url: String = s"https://api.open-meteo.com/v1/forecast?latitude=$lat&longitude=$lon&current_weather=true"
//      url
//    }
//    val resultFromWeather = Await.result(clientForWeather(defineUrl(latitude,longitude)),10 minutes)
//    val temp = resultFromWeather.map{obj =>
//      ((obj \ "current_weather") \ "temperature").as[Double]
//    }
//    val windSpeed = resultFromWeather.map { obj =>
//      ((obj \ "current_weather") \ "windspeed").as[Double]
//    }
//    val isDay = resultFromWeather.map { obj =>
//      if (((obj \ "current_weather") \ "is_day").asOpt[Int].contains(1)) "Day" else "Night"
//    }
//
//    Map("temp" -> temp, "windSpeed" -> windSpeed, "isDay" -> isDay)

//  }

}
