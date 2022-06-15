import Models.{InvalidConstraint, NotEmpty, NotNull, Source, ValidateFields, Validation}
import munit.FunSuite
import org.apache.spark.sql.SparkSession

class ModelsTest extends FunSuite {

  val spark = FunFixture[SparkSession](
    setup = _ => SparkSession.builder.appName("testSession").config("spark.master", "local").getOrCreate(),
    teardown = _ => SparkSession.clearActiveSession()
  )
  val exampleJsonPath = "data/input/events/person/example.json"
//  exampleJson:
//  {"name":"Xabier","age":39,"office":""}
//  {"name":"Miguel","office":"RIO"}
//  {"name":"Fran","age":31,"office":"RIO"}

  spark.test("After read a given a Source, a temporary view should be created and accesible") { spark =>
    //Creating and executing Source
    Source("test_source", exampleJsonPath, "JSON").createViewFromSource(spark)
    //Obtained results and assertions
    val records = spark.sql("SELECT count(*) as c FROM test_source").first().getAs[Long]("c")
    val older = spark.sql("SELECT name FROM test_source ORDER BY age desc").first().getAs[String](0)
    assertEquals(records, 3.toLong)
    assertEquals("Xabier",older)
  }
/*
  spark.test("Validate fields should create views both for validated and not-validated rows from a dataframe") { spark =>
    //Creating and executing Source
    Source("validate_test_source", exampleJsonPath, "JSON").createViewFromSource(spark)
    //Creating validations
    val validation = ValidateFields("test", "validate_test_source", Vector(Validation("fieldA", Vector(NotNull, NotEmpty))))
    val naiveValidation = ValidateFields("test", "validate_test_source", Vector(Validation("fieldA", Vector(InvalidConstraint))))
    //Obtained results and assertions
    validation.apply(spark)
    val recordsOk = spark.sql("SELECT count(*) as c FROM validated_ok").first().getAs[Long]("c")
    val recordsKo = spark.sql("SELECT count(*) as c FROM validated_ko").first().getAs[Long]("c")
    assertEquals(recordsOk,1.toLong)
    assertEquals(recordsKo,2.toLong)
    naiveValidation.apply(spark)
    val naiveRecordsOk = spark.sql("SELECT count(*) as c FROM validated_ok").first().getAs[Long]("c")
    val naiveRecordsKo = spark.sql("SELECT count(*) as c FROM validated_ko").first().getAs[Long]("c")
    assertEquals(naiveRecordsOk,3.toLong)
    assertEquals(naiveRecordsKo,0.toLong)
  }

 */

}
