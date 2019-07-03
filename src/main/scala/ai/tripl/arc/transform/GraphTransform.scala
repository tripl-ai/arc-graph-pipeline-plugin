package ai.tripl.arc.transform

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.QueryExecutionUtils
import ai.tripl.arc.util.Utils

class GraphTransform extends PipelineStagePlugin {

  val version = ai.tripl.arc.graph.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "persist" :: "sqlParams" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")  
    val inputCypher = if (config.hasPath("inputURI")) getValue[String]("inputURI") |> parseURI("inputURI") _ |> textContentForURI("inputURI", authentication) _
    val inputViews = readGraph("inputViews") 
    val outputGraph = getValue[String]("outputGraph")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, parsedURI, inputSQL, validSQL, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(parsedURI), Right(inputSQL), Right(validSQL), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) =>    

      val stage = GraphTransformStage(
          plugin=this,
          name=name,
          description=description,

        )

        stage.stageDetail.put("inputURI", parsedURI.toString)  
        stage.stageDetail.put("outputView", outputView)   
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("sql", inputSQL)   
        stage.stageDetail.put("sqlParams", sqlParams.asJava)  
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputSQL, validSQL, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  def readGraph(path: String)(implicit c: Config): Either[Errors, Option[MorpheusGraph]] = {
  
    def err(lineNumber: Option[Int], msg: String): Either[Errors, Option[MorpheusGraph]] = Left(ConfigError(path, lineNumber, msg) :: Nil)


{
  "type": "GraphTransform",
  "name": "create a graph to be able to execute queries against",
  "environments": [
    "production",
    "test"
  ],
  "inputViews": {
    "nodes": [
      {"type": "PERSON", "inputView": "view0"}
    ],
    "relationships": [
      {"type": "FRIEND", "inputView": "view0"}
    ]
  },
  "outputGraph": "customer"
}


    try {
      if (c.hasPath(path)) {

        val inputViews = c.getObject(path)

        if (!c.hasPath("nodes")) {
          return err(Some(c.getValue(path).origin.lineNumber()), s"${path} requires child attrbute 'nodes'")
        } else {
          val nodes = c.getObjectList("nodes").asScala.toList
        }



        if (authentication.isEmpty) {
          Right(None)
        } else {
          authentication.get("method") match {
            case Some("AzureSharedKey") => {
              val accountName = authentication.get("accountName") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureSharedKey' requires 'accountName' parameter.")
              } 
              if (accountName.contains("fs.azure")) {
                throw new Exception(s"Authentication method 'AzureSharedKey' 'accountName' should be just the account name not 'fs.azure.account.key...''.")
              }
              val signature = authentication.get("signature") match {
                case Some(v) => v
                case None => throw new Exception(s"Authentication method 'AzureSharedKey' requires 'signature' parameter.")
              }            
              Right(Some(Authentication.AzureSharedKey(accountName, signature)))
            }                              
            case _ =>  throw new Exception(s"""Unable to parse authentication method: '${authentication.get("method").getOrElse("")}'""")
          }
        }
      } else {
        Right(None)
      }
    } catch {
      case e: Exception => err(Some(c.getValue(path).origin.lineNumber()), s"Unable to read config value: ${e.getMessage}")
    }
  }  
}

case class GraphNodes (
  nodesType: String, 
  nodesView: String
)

case class GraphRelationships (
  relationshipsType: String, 
  relationshipsView: String
)

case class MorpheusGraph(
  nodes: List[GraphNodes],
  relationships: List[GraphRelationships]
)

case class GraphTransformStage(
    plugin: GraphTransform,
    name: String, 
    description: Option[String], 
    source: Either[MorpheusGraph, String], 
    outputGraph: String, 
    params: Map[String, String], 
    persist: Boolean,
    numPartitions: Option[Int], 
    partitionBy: List[String]    
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    GraphTransformStage.execute(this)
  }
}

object GraphTransformStage {
  
  def execute(stage: GraphTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // val startTime = System.currentTimeMillis() 
    // val stageDetail = new java.util.HashMap[String, Object]()
    // stageDetail.put("type", transform.getType)
    // stageDetail.put("name", transform.name)
    // for (description <- transform.description) {
    //   stageDetail.put("description", description)    
    // }    

    // val nodeSignature = "GraphTransform requires nodesView to have a column named 'id'."
    // val relsSignature = "GraphTransform requires relationshipsView to have a columns named 'id', 'source' and 'target'."
    // val relsTypeSignature = "GraphTransform requires relationshipsView to have 'source' and 'target' the same data type as nodesView 'id'."

    // logger.info()
    //   .field("event", "enter")
    //   .map("stage", stageDetail)      
    //   .log()   

    // val morpheus = arcContext.morpheusSession match {
    //   case Some(m) => m
    //   case None => {
    //     arcContext.morpheusSession = Option(MorpheusSession.create(spark))
    //     arcContext.morpheusSession.get
    //   }
    // }         

    // val graph = try {
    //   transform.source match {
    //     case Left(graphList) => {
    //       val subGraphs = graphList.map { case graph =>
    //         val nodesDF = spark.table(graph.nodesView)
    //         val relsDF = spark.table(graph.relationshipsView)

    //         // validation is basically to override error messages for clarity
    //         // validate nodesDF
    //         val nodesIds = nodesDF.schema.fields.filter(_.name == "id")
    //         if (nodesIds.length != 1) {
    //           throw new Exception(s"${nodeSignature} nodesView '${graph.nodesView}' has ${nodesDF.schema.length} columns named ${nodesDF.columns.mkString("[", ", ", "]")}.")
    //         }
    //         val nodesId = nodesIds(0)

    //         // validate relsDF
    //         val relsIds = relsDF.schema.fields.filter(_.name == "id")
    //         val relsSources = relsDF.schema.fields.filter(_.name == "source")
    //         val relsTargets = relsDF.schema.fields.filter(_.name == "target")

    //         // ensure required columns exist
    //         if (relsIds.length != 1 || relsSources.length != 1 || relsTargets.length != 1) {
    //           throw new Exception(s"${relsSignature} relationshipsView '${graph.relationshipsView}' has ${relsDF.schema.length} columns named ${relsDF.columns.mkString("[", ", ", "]")}.")
    //         }

    //         // ensure source and target have same dataType as nodesDF.id
    //         val relsSource = relsSources(0)
    //         val relsTarget = relsTargets(0)          
    //         if (relsSource.dataType != nodesId.dataType || relsTarget.dataType != nodesId.dataType) {
    //           throw new Exception(s"${relsTypeSignature} nodesView 'id' is of type '${nodesId.dataType.simpleString}' and relationshipsView ['source','target'] are of types ['${relsSource.dataType.simpleString}','${relsTarget.dataType.simpleString}'].")
    //         }

    //         // create the tables
    //         val nodesTable = MorpheusNodeTable(Set(graph.nodesType), nodesDF)
    //         val relationshipsTable = MorpheusRelationshipTable(graph.relationshipsType, relsDF)

    //         (nodesTable, relationshipsTable)
    //       }
    //       val subGraph = subGraphs.flatMap{ case (a,b) => List(a,b) }

    //       // create the graph
    //       morpheus.readFrom(subGraph(0), subGraph.drop(1):_*)
    //     }
    //     case Right(cypher) => {
    //       morpheus.cypher(cypher).graph
    //     }
    //   }
    // } catch {
    //   case e: Exception => throw new Exception(e) with DetailException {
    //     override val detail = stageDetail          
    //   }
    // }

    // // drop any existing graph with same name (like createOrReplaceTempView)
    // // will not throw error if does not exist
    // morpheus.catalog.dropGraph(transform.outputGraph)
    // // put the graph into the catalog. accessible via 'session.[outputGraph]'
    // morpheus.catalog.store(transform.outputGraph, graph)
    
    // if (transform.persist) {
    //   graph.cache
    // }      

    // val outputDF = morpheus.cypher(s"""
    // FROM session.${transform.outputGraph}
    // MATCH (n)-[r]->()
    // RETURN n, r
    // """).records.table.df   

    // logger.info()
    //   .field("event", "exit")
    //   .field("duration", System.currentTimeMillis() - startTime)
    //   .map("stage", stageDetail)      
    //   .log()  

    // Option(outputDF)
    None
  }

}
