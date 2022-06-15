import io.circe.Json
import io.circe.parser.parse
import Decoders.dataFlowsDecoder
import Models.Dataflows
import org.apache.spark.sql.SparkSession


object Application extends App {

  val sparkSession: SparkSession = SparkSession
    .builder
    .appName("sdg-access-test")
    .config("spark.master", "local")
    .getOrCreate()

  val metadata: String = scala.io.Source.fromFile("metadata/metadata.json").getLines().mkString("")
  val parsedMetadata: Json = parse(metadata).getOrElse(Json.Null)
  val decodedMetadata: Dataflows = parsedMetadata.as[Dataflows].toOption.get

  decodedMetadata.map(_.run(sparkSession))

  sparkSession.close()
}