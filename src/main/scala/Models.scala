package sdgTest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, _}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.annotation.tailrec

object Models {

  //Models a sequence of dataflows. Provides functions that operate directly in the sequence.
  case class Dataflows(dataflows: Vector[Dataflow]) {
    def apply(n: Int): Dataflow = dataflows(n)
    def map[B](f: Dataflow => B): Vector[B] = dataflows.map(f)
    def head: Dataflow = dataflows.head
  }

  //Models the dataflow sequence elements: sources, transformations applied to them, and sinks for the resulting data.
  case class Dataflow(name: String, sources: Vector[Source], transformations: Vector[Transformation], sinks: Vector[Sink]) {
    def run(session: SparkSession): Unit = {
      sources.map(_.createViewFromSource(session))
      transformations.map(_.apply(session))
      sinks.map(_.execute(session))
    }
  }

  //Models the sources and provides a function to create view from these sources.
  case class Source(name: String, path: String, format: String) {
    def createViewFromSource(session: SparkSession): Unit = {
      val tempPath = "data/input/events/person/example.json"
      val dataframe = session.read.format(format).load(tempPath) //TO-DO let parameter path (outside windows)
      dataframe.createOrReplaceTempView(name)
    }
  }

  //Models the algebraic data types for transformations, providing a function for apply them in a Spark Session.
  sealed trait Transformation {
    def apply(session: SparkSession): Unit
  }

  //Transformation to validate rows by field constraints in a view or table. Creates views both for validated and
  //not-validated rows.
  case class ValidateFields(name: String, input: String, validations: Vector[Validation]) extends Transformation {
    override def apply(session: SparkSession): Unit = {
      val dataframe = session.table(input)
      val (validationOk, validationKo, _) = validations.zipWithIndex.map { case (validation, index) =>
        val okValidationString = validation.constraintsToString(true)
        val koValidationString = validation.constraintsToString(false)
        val tempOK = dataframe.selectExpr("*").where(okValidationString)
        val tempKO = dataframe.selectExpr("*").where(koValidationString)
          .withColumn(s"error$index", lit(s"$name: $okValidationString on ${validation.field}"))
        (tempOK, tempKO, index)
      }.reduce((pair1, pair2) => {
        val (errorField1, errorField2) = (s"error${pair1._3}", s"error${pair2._3}")
        val okJoined = pair1._1.join(pair2._1, dataframe.columns)
        val koJoined = pair1._2.join(pair2._2, dataframe.columns, "outer")
          .withColumn("arraycoderrorbyfield", filter(array(errorField1, errorField2), _.isNotNull))
          .drop(errorField1, errorField2)
        (okJoined, koJoined, 0)
      })
      validationOk.createOrReplaceTempView("validation_ok")
      validationKo.createOrReplaceTempView("validation_ko")
    }
  }

  //Model a validation as a conjunction of constraints, providing a function to return the statement as String
  case class Validation(field: String, validations: Vector[Constraints]) {
    def constraintsToString(checkValidity: Boolean): String = {
      val prefix = if (checkValidity) "" else "!"
      validations.map {
        case NotNull => s"$prefix$field IS NOT NULL"
        case NotEmpty => s"${prefix}LENGTH($field) > 0"
        case _ => InvalidConstraint
      }.mkString(" AND ")
    }
  }

  //Constraints applied to validations. New constraints must be considered in the Decoder[Constraints]
  sealed trait Constraints
  case object NotNull extends Constraints
  case object NotEmpty extends Constraints
  case object InvalidConstraint extends Constraints

  //Transformation to add a new field to a dataframe.
  case class AddFields(name: String, input: String, additions: Vector[FieldAddition]) extends Transformation {
    override def apply(session: SparkSession): Unit = {
      @tailrec
      def recAddition(additions: Vector[FieldAddition], partialDataframe: DataFrame): DataFrame = additions match {
        case Vector(ad, _, _*) => recAddition(additions.tail, partialDataframe.withColumn(ad.name, col(ad.function)))
        case Vector(ad) => partialDataframe.withColumn(ad.name, col(ad.function))
      }

      recAddition(additions, session.table(input)).createOrReplaceTempView(name)
    }
  }

  case class FieldAddition(name: String, function: String)

  //Models a sink for a dataframe with several paths but same format and saveMode
  case class Sink(input: String, name: String, paths: Vector[String], format: String, saveMode: SaveMode) {
    def execute(session: SparkSession): Unit = {
      val df = session.table(input)
      val writer = df.write.format(format.toLowerCase).mode(saveMode)
      df.show(5, 200)
      paths.map(path => writer.save(s"$path/$name"))
    }
  }

}
