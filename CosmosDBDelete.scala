package com.microsoft.azure.cosmosdb.spark.delete

import com.microsoft.azure.cosmosdb.spark.CosmosDBConnection
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.{Try, Success, Failure}
import com.microsoft.azure.cosmosdb.spark.util.HdfsUtils
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.config.CosmosDBConfig
import scala.collection.mutable
import org.apache.spark.{Partition, SparkContext}
import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import scala.collection.mutable.ListBuffer
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.AsyncCosmosDBConnection
import java.util.concurrent.TimeUnit
import com.microsoft.azure.cosmosdb.spark.DefaultSource
import java.util.Random
import com.microsoft.azure.documentdb.bulkexecutor.DocumentBulkExecutor
import com.microsoft.azure.documentdb.bulkexecutor.BulkUpdateResponse
import com.microsoft.azure.documentdb.bulkexecutor.UpdateItem
import com.microsoft.azure.documentdb.Document
import org.apache.spark.sql.Row
import com.microsoft.azure.cosmosdb.spark.schema.CosmosDBRowConverter
import java.util.Collections
import java.util.Arrays
import com.microsoft.azure.cosmosdb.FeedOptions
import collection.JavaConversions._
import com.microsoft.azure.documentdb.bulkexecutor.BulkDeleteResponse
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.Log


object CosmosDBDelete extends CosmosDBLoggingTrait {

  var lastWritingBatchSize: Option[Int] = _

  val random = new Random(System.nanoTime())

  def save[D: ClassTag](dataset: Dataset[D], writeConfig: Config): Unit = {
    var numPartitions = 0
    val rdd = dataset.rdd
    try {
      numPartitions = rdd.getNumPartitions
    } catch {
      case _: Throwable => // no op
    }

    var partitionMap = mutable.Map[Int, Partition]()
    try {
      // The .partitions call can throw NullRef for non-parallel RDD
      val partitions = rdd.partitions
    } catch {
      case _: Throwable => // no op
    }

    val hadoopConfig = HdfsUtils.getConfigurationMap(rdd.sparkContext.hadoopConfiguration).toMap

    // Use min(writeThroughputBudget, collectionThroughput) - utilized only in bulk import
    val connection: CosmosDBConnection = new CosmosDBConnection(writeConfig)
    var collectionThroughput: Int = 0
    collectionThroughput = connection.getCollectionThroughput

    val writeThroughputBudget = writeConfig.get[String](CosmosDBConfig.WriteThroughputBudget)
    var offerThroughput: Int = collectionThroughput
    if (writeThroughputBudget.isDefined) {
      offerThroughput = Math.min(writeThroughputBudget.get.toInt, collectionThroughput)
    }

    logInfo("Write config: " + writeConfig.toString)

    val mapRdd = rdd.mapPartitionsWithIndex((partitionId, iter) => {
      val connection = new CosmosDBConnection(writeConfig)
      savePartition(connection, iter, writeConfig, numPartitions, offerThroughput)
    }, preservesPartitioning = true)
    mapRdd.collect()
  }

  private def savePartition[D: ClassTag](connection: CosmosDBConnection,
                                         iter: Iterator[D],
                                         config: Config,
                                         partitionCount: Int,
                                         offerThroughput: Int): Iterator[D] = {

    val connection: CosmosDBConnection = new CosmosDBConnection(config)
    val asyncConnection: AsyncCosmosDBConnection = new AsyncCosmosDBConnection(config)

    val isBulkDeleting = config.get[String]("bulkDelete").
      getOrElse("true").
      toBoolean

    val writingBatchSize = if (isBulkDeleting) {
      config.getOrElse(CosmosDBConfig.WritingBatchSize, String.valueOf(CosmosDBConfig.DefaultWritingBatchSize_BulkInsert))
        .toInt
    } else {
      config.getOrElse(CosmosDBConfig.WritingBatchSize, String.valueOf(CosmosDBConfig.DefaultWritingBatchSize_PointInsert))
        .toInt
    }

    val writingBatchDelayMs = config
      .getOrElse(CosmosDBConfig.WritingBatchDelayMs, String.valueOf(CosmosDBConfig.DefaultWritingBatchDelayMs))
      .toInt
    val rootPropertyToSave = config
      .get[String](CosmosDBConfig.RootPropertyToSave)

    val clientInitDelay = config.get[String](CosmosDBConfig.ClientInitDelay).
      getOrElse(CosmosDBConfig.DefaultClientInitDelay.toString).
      toInt
    val partitionKeyDefinition = config
      .get[String](CosmosDBConfig.PartitionKeyDefinition)

    val maxConcurrencyPerPartitionRangeStr = config.get[String](CosmosDBConfig.BulkImportMaxConcurrencyPerPartitionRange)
    val maxConcurrencyPerPartitionRange = if (maxConcurrencyPerPartitionRangeStr.nonEmpty)
      Integer.valueOf(maxConcurrencyPerPartitionRangeStr.get) else null

    // Delay the start as the number of tasks grow to avoid throttling at initialization
    val maxDelaySec: Int = (partitionCount / clientInitDelay) + (if (partitionCount % clientInitDelay > 0) 1 else 0)
    if (maxDelaySec > 0)
      TimeUnit.SECONDS.sleep(random.nextInt(maxDelaySec))

    CosmosDBSpark.lastWritingBatchSize = Some(writingBatchSize)

    if (iter.nonEmpty) {
      if (isBulkDeleting) {
        logDebug(s"Writing partition with bulk delete")
        bulkDelete(iter, connection, offerThroughput, writingBatchSize, partitionKeyDefinition)
      }
    }
    new ListBuffer[D]().iterator
  }

  private def bulkDelete[D: ClassTag](iter: Iterator[D],
                                      connection: CosmosDBConnection,
                                      offerThroughput: Int,
                                      writingBatchSize: Int,
                                      partitionKeyDefinition: Option[String])(implicit ev: ClassTag[D]): Unit = {

    // Set retry options high for initialization (default values)
    connection.setDefaultClientRetryPolicy

    // Initialize BulkExecutor
    val deleter: DocumentBulkExecutor = connection.getDocumentBulkImporter(offerThroughput, partitionKeyDefinition)

    val deleteIdPathPairs = new java.util.ArrayList[org.apache.commons.lang3.tuple.Pair[String, String]](writingBatchSize)

    var bulkDeleteResponse: BulkDeleteResponse = null
    iter.foreach(item => {
      item match {
        case updateItem: UpdateItem =>
        case doc: Document =>
        case row: Row =>
          val doc = new Document(CosmosDBRowConverter.rowToJSONObject(row).toString())
          val propertyNames: java.util.Collection[String] = partitionKeyDefinition.get.split("/").drop(1).toSeq
          val partition = Option(doc.getObjectByPath(propertyNames)).map(_.toString()).getOrElse(null)
          val id = Option(doc.getObjectByPath(Array("id").toSeq)).map(_.toString()).getOrElse(null)
          deleteIdPathPairs.add(org.apache.commons.lang3.tuple.ImmutablePair.of(partition, id))
        case _ => throw new Exception("Unsupported delete item types")
      }
      if (deleteIdPathPairs.size() >= writingBatchSize) {
        bulkDeleteResponse = deleter.deleteAll(deleteIdPathPairs)
        if (!bulkDeleteResponse.getErrors.isEmpty) {
          throw new Exception("Errors encountered in bulk delete API execution. Exceptions observed:\n" + bulkDeleteResponse.getErrors.toString)
        }
        deleteIdPathPairs.clear()
      }
    })
    if (deleteIdPathPairs.size() > 0) {
        bulkDeleteResponse = deleter.deleteAll(deleteIdPathPairs)
        if (!bulkDeleteResponse.getErrors.isEmpty) {
          throw new Exception("Errors encountered in bulk delete API execution. Exceptions observed:\n" + bulkDeleteResponse.getErrors.toString)
        }
    }
  }

  def truncate(config: Config): Unit = {
    val connection: CosmosDBConnection = new CosmosDBConnection(config)
    var partitionKeyDefinition = Some("")
    val documentCollection = connection.getCollection
    if(documentCollection == null) {
      throw new Exception("Errors encountered in truncate collection because the collection does not exist.")
    }
    else {
      Try(documentCollection.getPartitionKey().getPaths().iterator().next()) match {
        case Success(pathValue) => {
          partitionKeyDefinition = Some(pathValue)
        }
        case Failure(exception) =>
      }
    }

    var offerThroughput: Int = connection.getCollectionThroughput
    val writeThroughputBudget = config.get[String](CosmosDBConfig.WriteThroughputBudget)
    if (writeThroughputBudget.isDefined) {
      offerThroughput = Math.min(writeThroughputBudget.get.toInt, offerThroughput)
    }

    val writingBatchSize = config.getOrElse(CosmosDBConfig.WritingBatchSize, String.valueOf(CosmosDBConfig.DefaultWritingBatchSize_BulkInsert)).toInt

    // Beginning bulk delete within each partition range
    connection.getAllPartitions.foreach(partitionKeyRange =>
      truncateInternal(new CosmosDBConnection(config), partitionKeyRange.getId(), offerThroughput, writingBatchSize, partitionKeyDefinition)
    )
  }

  private def truncateInternal(connection: CosmosDBConnection,
                               partitionKeyRangeId: String,
                               offerThroughput: Int,
                               writingBatchSize: Int,
                               partitionKeyDefinition: Option[String]): Unit = {

    val deleter: DocumentBulkExecutor = connection.getDocumentBulkImporter(offerThroughput, partitionKeyDefinition)
    val options = new com.microsoft.azure.documentdb.FeedOptions()
    options.setPartitionKeyRangeIdInternal(partitionKeyRangeId)

    val iterator = connection.readDocuments(options)
    var pkIdPairsToDelete = new java.util.ArrayList[org.apache.commons.lang3.tuple.Pair[String, String]](writingBatchSize)
    var bulkDeleteResponse: BulkDeleteResponse = null

    while (iterator.hasNext) {
      var doc = iterator.next()
      val propertyNames: java.util.Collection[String] = partitionKeyDefinition.get.split("/").drop(1).toSeq
      val partition = Option(doc.getObjectByPath(propertyNames)).map(_.toString()).getOrElse(null)
      val id = Option(doc.getObjectByPath(Array("id").toSeq)).map(_.toString()).getOrElse(null)
      pkIdPairsToDelete.add(org.apache.commons.lang3.tuple.ImmutablePair.of(partition, id))

      if (pkIdPairsToDelete.size() >= writingBatchSize) {
        bulkDeleteResponse = deleter.deleteAll(pkIdPairsToDelete)
        if (!bulkDeleteResponse.getErrors.isEmpty) {
          throw new Exception("Errors encountered in truncate collection. Exceptions observed:\n" + bulkDeleteResponse.getErrors.toString)
        }
        pkIdPairsToDelete.clear()
      }
    }
    if (pkIdPairsToDelete.size() > 0) {
      bulkDeleteResponse = deleter.deleteAll(pkIdPairsToDelete)
      if (!bulkDeleteResponse.getErrors.isEmpty) {
        throw new Exception("Errors encountered in truncate collection. Exceptions observed:\n" + bulkDeleteResponse.getErrors.toString)
      }
    }
  }
}