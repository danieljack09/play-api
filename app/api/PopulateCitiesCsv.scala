package api

import com.github.tototoshi.csv.CSVWriter
import play.api.libs.json.{JsObject, JsValue}

import java.io.FileOutputStream
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object PopulateCitiesCsv extends App {

  def getAllCities() = {
    val url = "https://restcountries.com/v3.1/all"

    WSClientN.client
      .url(url)
      .get()
      .map{response =>
        response.status match {
          case 200 =>
            val responseFromApi = (response.body[JsValue]).as[Seq[JsObject]]
            responseFromApi
          case _ =>
            val errorMsg = s"Error message ${response.status} <- status with ${response.statusText}"
            new Exception(errorMsg)
            Seq()
        }
      }
  }
  val results = Await.result(getAllCities(),10 minutes)

  val capital = results.map{obj =>
    (obj \ "capital").asOpt[Seq[String]].map(_.mkString(" ")).getOrElse("")
  }



  val outputWriter =  CSVWriter.open(new FileOutputStream("newfile.csv"),"UTF-8")
  outputWriter.writeRow(Seq("Name"))

  capital.foreach{x =>
    outputWriter.writeRow(Seq(x))
  }
  WSClientN.close
  println(s"Finished writing in CSV")














}
