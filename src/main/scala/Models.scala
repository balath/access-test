import org.apache.spark.sql.{Column, SaveMode}


object Models {

  case class Dataflows(dataflows: Vector[Dataflow]) {
    def apply(n: Int):Dataflow = dataflows(n)
    def head: Dataflow = dataflows.head
  }

  case class Dataflow(name: String, sources: Vector[Source], transformations: Vector[Transformation], sinks: Vector[Sink]){
    def run: Unit = {
      applyTransformations
      executeSinks
    }
    def applyTransformations: Unit = sources.flatMap(source => transformations.map(_ over source))
    def executeSinks: Unit = sinks.map(_.execute)
  }

  case class Source(name: String, path: String, format: String)

  sealed trait Transformation {
    def over(source: Source): Unit = {}
  }
  case class ValidateFields(name: String, input: String, validations: Vector[Validation]) extends Transformation
  case class AddFields(name: String, input: String, additions: Vector[FieldAddition]) extends Transformation

  case class Validation(field: String, validations: Vector[String])
  case class FieldAddition(name: String, function: String)

  case class Sink(input: String, name: String, paths: Vector[String], format: String, saveMode: String) {
    def execute: Unit = {}
  }


}
