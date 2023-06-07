package api

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.ws.ahc.StandaloneAhcWSClient



object WSClientN  {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  system.registerOnTermination {
    System.exit(0)
  }
  val client: StandaloneAhcWSClient = StandaloneAhcWSClient()

  def close: Unit = {
    materializer.shutdown()
    system.terminate()
    client.close()
  }
}


