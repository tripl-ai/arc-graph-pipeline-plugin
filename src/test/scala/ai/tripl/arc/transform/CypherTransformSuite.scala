package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.opencypher.morpheus.api.MorpheusSession

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory 

import ai.tripl.arc.util._

class CypherTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputFile =getClass.getResource("/query.cypher").toString 
  val outputGraph = "graph"
  val outputView = "result"

  val inputNodes0 = "inputNodes0"
  val inputRelationships0 = "inputRelationships0"
  val nodesType0 = "Person"
  val relationshipsType0 = "FRIEND"


  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("CypherTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val nodesDF0 = spark.createDataset(Seq(
      ("customer0", "Alice", 42L),
      ("customer1", "Bob", 23L),
      ("customer2", "Eve", 84L)
    )).toDF("id", "name", "age")
    nodesDF0.createOrReplaceTempView(inputNodes0)

    val relsDF0 = spark.createDataset(Seq(
      (0L, "customer0", "customer1", "23/01/1987"),
      (1L, "customer1", "customer2", "12/12/2009")
    )).toDF("id", "source", "target", "since")
    relsDF0.createOrReplaceTempView(inputRelationships0)


    val conf = s"""{
      "stages": [
        {
          "type": "GraphTransform",
          "name": "test graph",
          "environments": [
            "production",
            "test"
          ],
          "nodes": [
            {"label": "${nodesType0}", "view": "${inputNodes0}"},
          ],
          "relationships": [
            {"type": "${relationshipsType0}", "view": "${inputRelationships0}"},
          ],
          "outputGraph": "${outputGraph}"
        },
        {
          "type": "CypherTransform",
          "name": "test graph",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${inputFile}",
          "cypherParams": {
            "graphName": "${outputGraph}"
          },
          "outputView": "${outputView}"
        }        
      ]
    }"""
    
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), arcContext)

    // assert graph created
    pipelineEither match {
      case Left(err) => {
        println(err)
        assert(false)
      }
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
        assert(df.get.count == 2)
      }
    }
  }
  
}
