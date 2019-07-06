package ai.tripl.arc.transform

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable}

import com.typesafe.config._

import ai.tripl.arc.api.API._
import ai.tripl.arc.api._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.config._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class GraphTransform extends PipelineStagePlugin {

  val version = ai.tripl.arc.graph.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "cypherParams" :: "outputGraph" :: "nodes" :: "relationships" :: "authentication" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")  
    val inputCypher = if (config.hasPath("inputURI")) getValue[String]("inputURI") |> parseURI("inputURI") _ |> textContentForURI("inputURI", authentication) _ else Right("")
    val cypherParams = readMap("cypherParams", c)
    val nodes = if (!config.hasPath("inputURI")) readNodes("nodes") else Right(List(GraphNode("","")))
    val relationships = if (!config.hasPath("inputURI")) readRelationships("relationships") else Right(List(GraphRelationship("","")))
    val outputGraph = getValue[String]("outputGraph")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputCypher, nodes, relationships, outputGraph, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(inputCypher), Right(nodes), Right(relationships), Right(outputGraph), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) =>    
        val source = if(c.hasPath("inputURI")) Right(inputCypher) else Left(MorpheusGraph(nodes, relationships))

        val stage = GraphTransformStage(
          plugin=this,
          name=name,
          description=description,
          source=source,
          cypherParams=cypherParams,
          outputGraph=outputGraph,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )        

        stage.stageDetail.put("outputGraph", outputGraph)   
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputCypher, nodes, relationships, outputGraph, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

  // readNodes reads a list of either GraphNodes
  def readNodes(path: String)(implicit c: Config, spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ConfigError], List[GraphNode]] = {
    // check valid type
    val objectList = try {
      c.getObjectList(path)
    } catch {
      case e: com.typesafe.config.ConfigException.WrongType => return Left(ConfigError(path, Some(c.origin.lineNumber), s"Expected ${path} to be a List of Objects.") :: Nil)
      case e: Exception => return Left(ConfigError(path, Some(c.origin.lineNumber), e.getMessage) :: Nil)
    }

    val (errors, instances) = objectList.asScala.zipWithIndex.foldLeft[(List[ConfigError], List[GraphNode])]( (Nil, Nil) ) { case ( (errors, instances), (instance, index) ) =>
      import ConfigReader._
      implicit val c = instance.toConfig

      val label = getValue[String]("label")
      val view = getValue[String]("view")

      val instanceOrError: Either[List[ConfigError], GraphNode] = (label, view) match {
        case (Right(label), Right(view)) =>    
          Right(
            GraphNode(
              label=label,
              view=view
            )
          )
        case _ =>
          Left(List(label, view).collect{ case Left(errs) => errs }.flatten)
      }

      instanceOrError match {
        case Left(error) => (error ::: errors, instances)
        case Right(instance) => (errors, instance :: instances)
      }
    }

    errors match {
      case Nil => Right(instances.reverse)
      case _ => Left(errors.reverse)
    }
  }

  // readRelationships reads a list of either GraphRelationship
  def readRelationships(path: String)(implicit c: Config, spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ConfigError], List[GraphRelationship]] = {
    // check valid type
    val objectList = try {
      c.getObjectList(path)
    } catch {
      case e: com.typesafe.config.ConfigException.WrongType => return Left(ConfigError(path, Some(c.origin.lineNumber), s"Expected ${path} to be a List of Objects.") :: Nil)
      case e: Exception => return Left(ConfigError(path, Some(c.origin.lineNumber), e.getMessage) :: Nil)
    }

    val (errors, instances) = objectList.asScala.zipWithIndex.foldLeft[(List[ConfigError], List[GraphRelationship])]( (Nil, Nil) ) { case ( (errors, instances), (instance, index) ) =>
      import ConfigReader._
      implicit val c = instance.toConfig

      val relationshipType = getValue[String]("type")
      val view = getValue[String]("view")

      val instanceOrError: Either[List[ConfigError], GraphRelationship] = (relationshipType, view) match {
        case (Right(relationshipType), Right(view)) =>    
          Right(
            GraphRelationship(
              relationshipType=relationshipType,
              view=view
            )
          )
        case _ =>
          Left(List(relationshipType, view).collect{ case Left(errs) => errs }.flatten)
      }

      instanceOrError match {
        case Left(error) => (error ::: errors, instances)
        case Right(instance) => (errors, instance :: instances)
      }
    }

    errors match {
      case Nil => Right(instances.reverse)
      case _ => Left(errors.reverse)
    }
  }  

}

case class GraphNode (
  label: String, 
  view: String
)

case class GraphRelationship (
  relationshipType: String, 
  view: String
)

case class MorpheusGraph(
  nodes: List[GraphNode],
  relationships: List[GraphRelationship]
)

case class GraphTransformStage(
    plugin: GraphTransform,
    name: String, 
    description: Option[String], 
    source: Either[MorpheusGraph, String], 
    cypherParams: Map[String, String],
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

    // get the morpheus session from the arcContext
    // this is a bit hacky but fits with the arc plugin model
    val morpheus = arcContext.userData.get("morpheusSession") match {
      case Some(m: MorpheusSession) => m
      case _ => {
        val morpheusSession = MorpheusSession.create(spark)
        arcContext.userData += ("morpheusSession" -> morpheusSession)
        morpheusSession
      }
    }   

    val nodeSignature = "GraphTransform requires all Nodes views to have a column named 'id'."
    val relsSignature = "GraphTransform requires all Relationships views to have a columns named 'id', 'source' and 'target'."

    val graph = try {
      stage.source match {
        case Left(morpheusGraph) => {
          val nodes = morpheusGraph.nodes.map { case graphNode =>
            val df = spark.table(graphNode.view)

            // validation is basically to override error messages for clarity
            val idColumns = df.schema.fields.filter(_.name == "id")
            if (idColumns.length != 1) {
              throw new Exception(s"${nodeSignature} Nodes view '${graphNode.view}' has columns named ${df.columns.mkString("[", ", ", "]")}.")
            }

            MorpheusNodeTable(Set(graphNode.label), df)
          }

          val relationships = morpheusGraph.relationships.map { case graphRelationship =>
            val df = spark.table(graphRelationship.view)

            // validation is basically to override error messages for clarity
            val idColumns = df.schema.fields.filter(_.name == "id")
            val sourceColumns = df.schema.fields.filter(_.name == "source")
            val targetColumns = df.schema.fields.filter(_.name == "target")

            // ensure required columns exist
            if (idColumns.length != 1 || sourceColumns.length != 1 || targetColumns.length != 1) {
              throw new Exception(s"${relsSignature} Relationships view '${graphRelationship.view}' has columns named ${df.columns.mkString("[", ", ", "]")}.")
            }

            MorpheusRelationshipTable(graphRelationship.relationshipType, df)
          }

          // create the graph
          // note this is using a strange api where it requires at least one argument and the rest are variadic
          val merged = nodes ::: relationships
          morpheus.readFrom(merged(0), merged.drop(1):_*)
        }
        case Right(cypher) => {
          // inject parameters
          val stmt = SQLUtils.injectParameters(cypher, stage.cypherParams, false)
          stage.stageDetail.put("cypher", stmt)
          
          morpheus.cypher(stmt).graph
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }
    }

    // if not in immutable mode
    // drop any existing graph with same name (like createOrReplaceTempView)
    // will not throw error if does not exist
    if (!arcContext.immutableViews) {
      morpheus.catalog.dropGraph(stage.outputGraph)
    }
    // put the graph into the catalog. accessible via 'session.[outputGraph]'
    morpheus.catalog.store(stage.outputGraph, graph)
    
    if (stage.persist) {
      graph.cache
    }      

    val outputDF = morpheus.cypher(s"""
    FROM session.${stage.outputGraph}
    MATCH (n0)-[r]->(n1)
    RETURN n0, r, n1
    """).records.table.df   

    Option(outputDF)
  }

}