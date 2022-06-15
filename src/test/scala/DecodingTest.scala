import Models.{AddFields, Dataflow, Dataflows, FieldAddition, NotEmpty, NotNull, Sink, Source, ValidateFields, Validation}
import Decoders.dataFlowsDecoder
import io.circe.Json
import io.circe.parser.parse
import munit.FunSuite
import org.apache.spark.sql.SaveMode

class DecodingTest extends FunSuite {

  test("Empty Json dataflow should return empty Dataflows object"){
    val expected = Dataflows(Vector.empty)
    val input ="""
        |{
        |  "dataflows": []
        |}
        |""".stripMargin
    val actual = parse(input).getOrElse(Json.Null).as[Dataflows].toOption.get
    assertEquals(actual, expected)
  }

  test("Metadata in Json should keep the same info as in Dataflow object"){
    val expected = Dataflows(Vector(
      Dataflow(
        "prueba-acceso",
        Vector(
          Source("person_inputs", "/data/input/events/person/*", "JSON")
        ),
        Vector(
          ValidateFields("validation", "person_inputs", Vector(
            Validation("office", Vector(NotEmpty)),
            Validation("age", Vector(NotNull)))
          ),
          AddFields("ok_with_date", "validation_ok", Vector(
            FieldAddition("dt", "current_timestamp"))
          )
        ),
        Vector(
          Sink("ok_with_date", "raw-ok", Vector("/data/output/events/person"), "JSON", SaveMode.Overwrite),
          Sink("validation_ko", "raw-ko", Vector("/data/output/discards/person"), "JSON", SaveMode.Overwrite)
        )
      )
    ))

    val input ="""
        |{
        |  "dataflows": [
        |    {
        |      "name": "prueba-acceso",
        |      "sources": [
        |        {
        |          "name": "person_inputs",
        |          "path": "/data/input/events/person/*",
        |          "format": "JSON"
        |        }
        |      ],
        |      "transformations": [
        |        {
        |          "name": "validation",
        |          "type": "validate_fields",
        |          "params": {
        |            "input": "person_inputs",
        |            "validations": [
        |              {
        |                "field": "office",
        |                "validations": [
        |                  "notEmpty"
        |                ]
        |              },
        |              {
        |                "field": "age",
        |                "validations": [
        |                  "notNull"
        |                ]
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "name": "ok_with_date",
        |          "type": "add_fields",
        |          "params": {
        |            "input": "validation_ok",
        |            "addFields": [
        |              {
        |                "name": "dt",
        |                "function": "current_timestamp"
        |              }
        |            ]
        |          }
        |        }
        |      ],
        |      "sinks": [
        |        {
        |          "input": "ok_with_date",
        |          "name": "raw-ok",
        |          "paths": [
        |            "/data/output/events/person"
        |          ],
        |          "format": "JSON",
        |          "saveMode": "OVERWRITE"
        |        },
        |        {
        |          "input": "validation_ko",
        |          "name": "raw-ko",
        |          "paths": [
        |            "/data/output/discards/person"
        |          ],
        |          "format": "JSON",
        |          "saveMode": "OVERWRITE"
        |        }
        |      ]
        |    }
        |  ]
        |}
        |""".stripMargin
    val actual = parse(input).getOrElse(Json.Null).as[Dataflows].toOption.get
    assertEquals(actual, expected)
  }

}
