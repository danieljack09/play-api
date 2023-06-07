package api

import play.api.libs.json._
import play.libs.Json

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object CountryDetailsApi {

  def getAllCountryData(country:String) = {


    def clientForCountryData(country: String) = {
      val url = s"https://restcountries.com/v3.1/name/$country"
      WSClientN.client
        .url(url)
        .get()
        .map { response =>
          response.status match {
            case 200 =>
              val resultFromClient = response.body[JsValue].as[Seq[JsObject]]
              resultFromClient

            case _ =>
              val errorMsg = s"Error: ${response.status} ${response.statusText}"
              new Exception(errorMsg)
              Seq()
          }
        }
    }

    val allCountries: Seq[JsObject] = Await.result(clientForCountryData(country), 1 minutes)


    val countryName = allCountries.map { obj => ((obj \ "name") \ "common").asOpt[String].getOrElse("") }
    val countryCurrency = allCountries.map { obj =>
      (obj \ "currencies").asOpt[JsObject].map { x =>
        x.fields.map(_._2)
      }.map { x =>
        x.map(x =>
          (x \ "name").asOpt[String].getOrElse(""))
      }.getOrElse(Seq.empty).mkString(", ")
    }
    val capital = allCountries.map { obj =>
      (obj \ "capital").asOpt[Seq[String]].map {
        _.mkString(" ")
      }.getOrElse("")
    }

    val region = allCountries.map { obj => (obj \ "region").asOpt[String].getOrElse("") }

    val area = allCountries.map { obj => (obj \ "area").asOpt[Double].getOrElse(0.0) }

    val timezone = allCountries.map { obj => (obj \ "timezones").asOpt[Seq[String]].map {
      _.mkString(" ")
    }.getOrElse("")
    }
    val population = allCountries.map { obj => (obj \ "population").asOpt[Long].getOrElse(0) }

    Json.stringify(Json.toJson(Map(
      "countryName" -> countryName,
      "capital" -> capital,
      "timezone" -> timezone,
      "countryRegion" -> region,
      "countryArea" -> area,
      "countryCurrency" -> countryCurrency,
      "population" -> population)))
  }
}
