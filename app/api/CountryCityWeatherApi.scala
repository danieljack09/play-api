package api

import play.api.libs.json.Json

import scala.language.postfixOps

object CountryCityWeatherApi {

  def fullDetailsCity(city:String) ={

      val resultFromJsonCity = Json.parse(APIClient.cityResult(city))
      val name = (resultFromJsonCity \ "name").as[Seq[String]].head
      val country = (resultFromJsonCity \ "country").as[Seq[String]].head
      val timezone = ((resultFromJsonCity \ "timezone") \ "value").as[String]
      val population = (resultFromJsonCity \ "population").as[Seq[Long]].head
      val timeOfMeasurement = ((resultFromJsonCity \ "timeOfMeasurement") \ "value").as[String]
      val isDay = ((resultFromJsonCity \ "isDay") \ "value").as[String]
      val temp = ((resultFromJsonCity \ "temp") \ "value").as[Double]
      val windSpeed = ((resultFromJsonCity \ "windSpeed") \ "value").as[Double]



      val resultFromJsonCountry = Json.parse(CountryDetailsApi.getAllCountryData(country))
      val capital = (resultFromJsonCountry \ "capital").as[Seq[String]].head
      val countryRegion = (resultFromJsonCountry \ "countryRegion").as[Seq[String]].head
      val countryArea = (resultFromJsonCountry \ "countryArea").as[Seq[Double]].head
      val countryCurrency = (resultFromJsonCountry \ "countryCurrency").as[Seq[String]].head
      val countryPopulation = (resultFromJsonCountry \ "population").as[Seq[Long]].head

      val responseResult = Json.obj(
        "city" -> name,
        "country" -> country,
        "capital" -> capital,
        "timezone" -> timezone,
        "countryRegion" -> countryRegion,
        "cityPopulation" -> population,
        "countryPopulation" -> countryPopulation,
        "countryArea" -> countryArea,
        "countryCurrency" -> countryCurrency,
        "weatherDetails" -> Json.obj(
          "temperature" -> temp,
          "windspeed" -> windSpeed,
          "timeOfMeasurement" -> timeOfMeasurement,
          "periodOfDay" -> isDay
        )
      )
      Json.stringify(responseResult)
    }
}
