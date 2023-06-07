package controllers

import api.{APIClient, CountryCityWeatherApi, CountryDetailsApi}
import play.api.libs.json._
import play.api.mvc.{Action, AnyContent, BaseController, ControllerComponents}

import javax.inject.{Inject, Singleton}
import scala.concurrent.ExecutionContext
import scala.language.postfixOps

@Singleton
class Application @Inject()(val controllerComponents: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {

  // LINE PARAMETER
  def getCityData(city:String): Action[AnyContent] = Action { implicit request =>
    val resultFromJson = Json.parse(APIClient.cityResult(city))
    val name = (resultFromJson \ "name").as[Seq[String]].head
    val country  = (resultFromJson \ "country").as[Seq[String]].head
    val timezone = ((resultFromJson \ "timezone") \ "value").as[String]
    val population = (resultFromJson \ "population").as[Seq[Long]].head

    val responseResult = Json.obj(
      "cityName" -> name,
      "countryName" -> country,
      "timezone"-> timezone,
      "cityPopulation" -> population
    )
    Ok(responseResult)
  }

  def getWeatherData(city:String) = Action { implicit request =>
    val resultFromJson = Json.parse(APIClient.cityResult(city))
    val name = (resultFromJson \ "name").as[Seq[String]].head
    val country  = (resultFromJson \ "country").as[Seq[String]].head
    val timezone = ((resultFromJson \ "timezone") \ "value").as[String]
    val timeOfMeasurement = ((resultFromJson \ "timeOfMeasurement") \ "value").as[String]
    val isDay = ((resultFromJson \ "isDay") \ "value").as[String]
    val temp = ((resultFromJson \ "temp") \ "value").as[Double]
    val windSpeed = ((resultFromJson \ "windSpeed") \ "value").as[Double]

    val responseResult = Json.obj(
      "cityName" -> name,
      "countryName" -> country,
      "timezone" -> timezone,
      "timeOfMeasurement" -> timeOfMeasurement,
      "dayOrNight" -> isDay,
      "temp" -> temp,
      "windSpeed" -> windSpeed
    )
    Ok(responseResult)
  }

  def getCountryData(country: String) = Action { implicit request =>
    val resultFromApi = Json.parse(CountryDetailsApi.getAllCountryData(country))
    val capital = (resultFromApi \ "capital" ).as[Seq[String]].head
    val timezone = (resultFromApi \ "timezone" ).as[Seq[String]].head
    val countryRegion = (resultFromApi \ "countryRegion" ).as[Seq[String]].head
    val countryArea = (resultFromApi \ "countryArea" ).as[Seq[Double]].head
    val countryCurrency = (resultFromApi \ "countryCurrency" ).as[Seq[String]].head

    val responseResult = Json.obj(
      "capital" -> capital,
      "timezone" -> timezone,
      "countryRegion" -> countryRegion,
      "countryArea" -> countryArea,
      "countryCurrency" -> countryCurrency
    )
    Ok(responseResult)
  }

  def fullDetails(city:String) = Action { implicit request =>
    Ok(CountryCityWeatherApi.fullDetailsCity(city))
  }

  // QUERY PARAMETER
  def getCityDataQuery(): Action[AnyContent] = Action { implicit request =>
    val cityOption = request.getQueryString("city")
    cityOption match {
      case Some(city) =>
        val resultFromJson = Json.parse(APIClient.cityResult(city))
        val name = (resultFromJson \ "name").as[Seq[String]].head
        val country = (resultFromJson \ "country").as[Seq[String]].head
        val timezone = ((resultFromJson \ "timezone") \ "value").as[String]
        val population = (resultFromJson \ "population").as[Seq[Long]].head

        val responseResult = Json.obj(
          "cityName" -> name,
          "countryName" -> country,
          "timezone" -> timezone,
          "cityPopulation" -> population
        )
        Ok(responseResult)
      case None =>
        BadRequest("Missing 'city' query parameter")
    }
  }

  def getWeatherDataQuery() = Action { implicit request =>
    val cityOption = request.getQueryString("city")
    cityOption match {
      case Some(city) =>
        val resultFromJson = Json.parse(APIClient.cityResult(city))
        val name = (resultFromJson \ "name").as[Seq[String]].head
        val country = (resultFromJson \ "country").as[Seq[String]].head
        val timezone = ((resultFromJson \ "timezone") \ "value").as[String]
        val timeOfMeasurement = ((resultFromJson \ "timeOfMeasurement") \ "value").as[String]
        val isDay = ((resultFromJson \ "isDay") \ "value").as[String]
        val temp = ((resultFromJson \ "temp") \ "value").as[Double]
        val windSpeed = ((resultFromJson \ "windSpeed") \ "value").as[Double]

        val responseResult = Json.obj(
          "cityName" -> name,
          "countryName" -> country,
          "timezone" -> timezone,
          "timeOfMeasurement" -> timeOfMeasurement,
          "dayOrNight" -> isDay,
          "temp" -> temp,
          "windSpeed" -> windSpeed
        )
        Ok(responseResult)
      case None =>
        BadRequest("Missing 'city' query parameter")
    }
  }

  def getCountryDataQuery() = Action { implicit request =>
    val countryOption = request.getQueryString("country")
    countryOption match {
      case Some(country) =>
    val resultFromApi = Json.parse(CountryDetailsApi.getAllCountryData(country))
    val capital = (resultFromApi \ "capital").as[Seq[String]].head
    val timezone = (resultFromApi \ "timezone").as[Seq[String]].head
    val countryRegion = (resultFromApi \ "countryRegion").as[Seq[String]].head
    val countryArea = (resultFromApi \ "countryArea").as[Seq[Double]].head
    val countryCurrency = (resultFromApi \ "countryCurrency").as[Seq[String]].head

    val responseResult = Json.obj(
      "capital" -> capital,
      "timezone" -> timezone,
      "countryRegion" -> countryRegion,
      "countryArea" -> countryArea,
      "countryCurrency" -> countryCurrency
    )
      Ok(responseResult)
      case None =>
        BadRequest("Missing 'country' query parameter")
  }}

  def fullDetailsQuery() = Action { implicit request =>
    val cityOption = request.getQueryString("city")
    cityOption match {
      case Some(city) => Ok(CountryCityWeatherApi.fullDetailsCity(city))
      case None =>
        BadRequest("Missing 'city' query parameter")
  }}


}

