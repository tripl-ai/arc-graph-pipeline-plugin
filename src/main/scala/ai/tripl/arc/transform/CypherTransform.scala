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

class CypherTransform extends PipelineStagePlugin {

  val version = ai.tripl.arc.graph.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "cypherParams" :: "outputView" :: "authentication" :: "persist" :: "params" :: "numPartitions" :: "partitionBy" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")  
    val cypher = getValue[String]("inputURI") |> parseURI("inputURI") _ |> textContentForURI("inputURI", authentication) _
    val cypherParams = readMap("cypherParams", c)
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))        
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, cypher, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(cypher), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) =>    

        val stage = CypherTransformStage(
          plugin=this,
          name=name,
          description=description,
          cypher=cypher,
          cypherParams=cypherParams,
          outputView=outputView,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )        

        stage.stageDetail.put("cypher", cypher)   
        stage.stageDetail.put("cypherParams", cypherParams.asJava)
        stage.stageDetail.put("outputView", outputView)   
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, cypher, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class CypherTransformStage(
    plugin: CypherTransform,
    name: String, 
    description: Option[String], 
    cypher: String,
    cypherParams: Map[String, String], 
    outputView: String,
    params: Map[String, String], 
    persist: Boolean,
    numPartitions: Option[Int], 
    partitionBy: List[String]    
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    CypherTransformStage.execute(this)
  }
}

object CypherTransformStage {
  
  def execute(stage: CypherTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // get the morpheus session from the arcContext
    // this is a bit hacky but fits with the arc plugin model
    val morpheus = arcContext.userData.get("morpheusSession") match {
      case Some(m: MorpheusSession) => m
      case _ => {
        throw new Exception(s"CypherTransform executes an existing graph created with GraphTransform but no session exists to execute CypherTransform against.")
      }
    }   

    // inject parameters
    val stmt = SQLUtils.injectParameters(stage.cypher, stage.cypherParams, false)
    stage.stageDetail.put("cypher", stmt)

    // execute query and get dataframe
    val transformedDF = try {
      val result = morpheus.cypher(stmt)
      result.records.table.df
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => { 
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    }

    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count)) 
      }      
    }

    Option(repartitionedDF)
  }

}