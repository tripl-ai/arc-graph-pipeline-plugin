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

class GraphTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val outputGraph = "graph"

  val inputNodes0 = "inputNodes0"
  val inputRelationships0 = "inputRelationships0"
  val nodesType0 = "Person"
  val relationshipsType0 = "FRIEND"

  val inputNodes1 = "inputNodes1"
  val inputRelationships1 = "inputRelationships1"
  val nodesType1 = "Book"
  val relationshipsType1 = "BOUGHT"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("GraphTransform") {
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

    val nodesDF1 = spark.createDataset(Seq(
      ("book0", "1984"),
      ("book1", "Cryptonomicon"),
      ("book2", "The Eye of the World")
    )).toDF("id", "name")
    nodesDF1.createOrReplaceTempView(inputNodes1)

    val relsDF1 = spark.createDataset(Seq(
      (0L, "customer0", "book0"),
      (1L, "customer0", "book2"),
      (2L, "customer1", "book0"),
      (3L, "customer1", "book2"),
      (4L, "customer1", "book1"),
      (5L, "customer2", "book2"),            
    )).toDF("id", "source", "target")
    relsDF1.createOrReplaceTempView(inputRelationships1)

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
            {"label": "${nodesType1}", "view": "${inputNodes1}"}
          ],
          "relationships": [
            {"type": "${relationshipsType0}", "view": "${inputRelationships0}"},
            {"type": "${relationshipsType1}", "view": "${inputRelationships1}"}
          ],
          "outputGraph": "${outputGraph}"
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
        assert(df.get.count == 8)
      }
    }

    // assert can carry session via arcContext.userData
    // assert graphName is registered
    arcContext.userData.get("morpheusSession") match {
      case Some(morpheus: MorpheusSession) => {
        val catalogGraphs = morpheus.catalog.graphNames.filter(graph => graph.graphName.toString == outputGraph)
        assert(catalogGraphs.nonEmpty)
      }
      case _ => assert(false)
    }
  }
  
}
