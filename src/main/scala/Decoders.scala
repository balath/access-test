package sdgTest

import Models._
import io.circe.{ACursor, Decoder}
import org.apache.spark.sql.SaveMode

object Decoders {

  //Decoder for the metadata json file, as a vector of dataflows.
  implicit val dataFlowsDecoder: Decoder[Dataflows] = (cursor: ACursor) => for {
    dataflows <- cursor.get[Vector[Dataflow]]("dataflows")
  } yield Dataflows(dataflows)

  //Decoder for a dataflow metadata, composed by sources, transformations and sinks.
  implicit val dataFlowDecoder: Decoder[Dataflow] = (cursor: ACursor) => for {
    name <- cursor.get[String]("name")
    sources <- cursor.get[Vector[Source]]("sources")
    transformations <- cursor.get[Vector[Transformation]]("transformations")
    sinks <- cursor.get[Vector[Sink]]("sinks")
  } yield Dataflow(name, sources, transformations, sinks)

  //Decoder for transformations. It uses decoders inside for the different kinds of transformations.
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

  //Decoder for "FielAddition" transformation.
  implicit val fieldAdditionDecoder: Decoder[FieldAddition] = (cursor: ACursor) => for {
    name <- cursor.get[String]("name")
    function <- cursor.get[String]("function")
  } yield FieldAddition(name, function)

  //Decoder for "Validation" transformation.
  implicit val validationDecoder: Decoder[Validation] = (cursor: ACursor) => for {
    field <- cursor.get[String]("field")
    validations <- cursor.get[Vector[String]]("validations")
    constraints: Vector[Constraints] = validations.map {
      case "notEmpty" => NotEmpty
      case "notNull" => NotNull
      case _ => InvalidConstraint
    }
  } yield Validation(field, constraints)

  implicit val sinkDecoder: Decoder[Sink] = (cursor: ACursor) => for {
    input <- cursor.get[String]("input")
    name <- cursor.get[String]("name")
    paths <- cursor.get[Vector[String]]("paths")
    format <- cursor.get[String]("format")
    saveModeString <- cursor.get[String]("saveMode")
    saveMode = saveModeString.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case _ => SaveMode.ErrorIfExists
    }
  } yield Sink(input, name, paths, format, saveMode)

  implicit val sourceDecoder: Decoder[Source] = (cursor: ACursor) => for {
    name <- cursor.get[String]("name")
    path <- cursor.get[String]("path")
    format <- cursor.get[String]("format")
  } yield Source(name, path, format)
}
