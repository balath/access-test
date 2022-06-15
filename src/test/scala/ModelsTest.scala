import Models.Source
import munit.FunSuite
import org.apache.spark.sql.SparkSession

class SourceTest extends FunSuite {

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

}
