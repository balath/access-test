import io.circe.Json
import io.circe.parser.parse
import Decoders.dataFlowsDecoder
import Models.Dataflows


object Pipeline extends App {

  val json = scala.io.Source.fromFile("doc/metadata.json").getLines().mkString("")


  val j = parse(json)
  println(j)

  val decoded = j.getOrElse(Json.Null).as[Dataflows].toOption.get
  println(decoded)
}
