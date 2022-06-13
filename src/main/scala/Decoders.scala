import Models._
import io.circe.{Decoder, ACursor}

object Decoders {
  implicit val transformationDecoder: Decoder[Transformation] = (cursor: ACursor) => {
    val transformationType = cursor.get[String]("type").getOrElse("")
    transformationType match {
      case "validate_fields" => for {
        name <- cursor.get[String]("name")
        input <- cursor.downField("params").get[String]("input")
        validations <- cursor.downField("params").get[Vector[Validation]]("validations")
      } yield ValidateFields(name, input, validations)
      case "add_fields" => for {
        name <- cursor.get[String]("name")
        input <- cursor.downField("params").get[String]("input")
        additions <- cursor.downField("params").get[Vector[FieldAddition]]("addFields")
      } yield AddFields(name, input, additions)
    }
  }

  implicit val fieldAdditionDecoder: Decoder[FieldAddition] = (cursor: ACursor) => for {
    name <- cursor.get[String]("name")
    function <- cursor.get[String]("function")
  } yield FieldAddition(name, function)


  implicit val validationDecoder: Decoder[Validation] = (cursor: ACursor) => for {
    field <- cursor.get[String]("field")
    validations <- cursor.get[Vector[String]]("validations")
  } yield Validation(field, validations)


  implicit val sinkDecoder: Decoder[Sink] = (cursor: ACursor) => for {
    input <- cursor.get[String]("input")
    name <- cursor.get[String]("name")
    paths <- cursor.get[Vector[String]]("paths")
    format <- cursor.get[String]("format")
    saveMode <- cursor.get[String]("saveMode")
  } yield Sink(input, name, paths, format, saveMode)

  implicit val sourceDecoder: Decoder[Source] = (cursor: ACursor) => for {
    name <- cursor.get[String]("name")
    path <- cursor.get[String]("path")
    format <- cursor.get[String]("format")
  } yield Source(name, path, format)

  implicit val dataFlowDecoder: Decoder[Dataflow] = (cursor: ACursor) => for {
    name <- cursor.get[String]("name")
    sources <- cursor.get[Vector[Source]]("sources")
    transformations <- cursor.get[Vector[Transformation]]("transformations")
    sinks <- cursor.get[Vector[Sink]]("sinks")
  } yield Dataflow(name, sources, transformations, sinks)

  implicit val dataFlowsDecoder: Decoder[Dataflows] = (cursor: ACursor) => for {
    dataflows <- cursor.get[Vector[Dataflow]]("dataflows")
  } yield Dataflows(dataflows)

}
