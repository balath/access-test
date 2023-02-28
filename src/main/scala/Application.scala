package accessTest
import io.circe.Json
import io.circe.parser.parse
import Decoders.dataFlowsDecoder
import Models.Dataflows
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}


object Application extends App {
  Try(scala.io.Source.fromFile(args(0)).getLines().mkString("")) match {
    case Success(metadata) => {
      val sparkSession: SparkSession = SparkSession
        .builder
        .appName("access-test")
        .master("local")
//        .master("spark://da1370c6cc35:7077 ")
        .getOrCreate()
      val parsedMetadata: Json = parse(metadata).getOrElse(Json.Null)
      val dataflows: Dataflows = parsedMetadata.as[Dataflows].toOption.get

      dataflows.map(dataflow => dataflow.run(sparkSession))

      sparkSession.close()
    }
    case Failure(exception) => println(s"Illegal or invalid arguments caused an exception: ${exception.getMessage}")
  }
}
