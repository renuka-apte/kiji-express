/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.chopsticks

import scala.collection.JavaConverters._

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import java.util
import org.apache.avro.specific.{SpecificRecordBase, SpecificFixed}
import org.apache.avro.generic.{GenericData, GenericFixed, IndexedRecord}
import java.io.InvalidClassException
import org.kiji.schema.layout.{InvalidLayoutException, KijiTableLayout}
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.Schema

/**
 * A scheme that can source and sink data from a Kiji table. This scheme is responsible for
 * converting rows from a Kiji table that are input to a Cascading flow into Cascading tuples (see
 * [[#source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)]]) and writing output
 * data from a Cascading flow to a Kiji table
 * (see [[#sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)]]).
 *
 * @param columns mapping tuple field names to Kiji column names.
 */
@ApiAudience.Framework
@ApiStability.Unstable
class KijiScheme(
    private val timeRange: TimeRange,
    private val columns: Map[String, ColumnRequest])
    extends Scheme[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _],
        KijiSourceContext, KijiSinkContext] {
  import KijiScheme._

  /** Fields expected to be in any tuples processed by this scheme. */
  private val fields: Fields = {
    val fieldSpec: Fields = buildFields(columns.keys)

    // Set the fields for this scheme.
    setSourceFields(fieldSpec)
    setSinkFields(fieldSpec)

    fieldSpec
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  override def sourceConfInit(
      process: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]],
      conf: JobConf) {
    // Build a data request.
    val request: KijiDataRequest = buildRequest(timeRange, columns.values)

    // Write all the required values to the job's configuration object.
    conf.setInputFormat(classOf[KijiInputFormat])
    conf.set(
        KijiConfKeys.KIJI_INPUT_DATA_REQUEST,
        Base64.encodeBase64String(SerializationUtils.serialize(request)))
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourcePrepare(
      process: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]) {
    val tableUriProperty = process.getStringProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI)
    if (null != tableUriProperty) {
      val tableUri:KijiURI = KijiURI.newBuilder(tableUriProperty).build()
      doAndRelease(Kiji.Factory.open(tableUri)) { kiji: Kiji =>
        doAndRelease(kiji.openTable(tableUri.getTable())) { table: KijiTable =>
        // Set the sink context to the table layout.
          sourceCall.setContext(KijiSourceContext(sourceCall.getInput().createValue(), table.getLayout))
        }
      }
    } else
      sourceCall.setContext(KijiSourceContext(sourceCall.getInput.createValue(), null))
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   * @return True always. This is used to indicate if there are more rows to read.
   */
  override def source(
      process: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]): Boolean = {
    // Get the current key/value pair.
    val KijiSourceContext(value, layout) = sourceCall.getContext()

    // Get the first row where all the requested columns are present,
    // and use that to set the result tuple.
    while (sourceCall.getInput().next(null, value)) {
      val row: KijiRowData = value.get()
      if (allColumnsPresent(row, columns)) {
        val result: Tuple = rowToTuple(columns, getSourceFields, row)
        sourceCall.getIncomingEntry().setTuple(result)
        return true // We set a result tuple, return true for success.
      }
      // If we didn't return true because a column wasn't present, continue the loop.
      // TODO(CHOP-47): log skipped rows.
    }
    return false // We reached the end of the RecordReader.
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourceCleanup(
      process: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]) {
    sourceCall.setContext(null)
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  override def sinkConfInit(
      process: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]],
      conf: JobConf) {
    // No-op since no configuration parameters need to be set to encode data for Kiji.
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkPrepare(
      process: FlowProcess[JobConf],
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]) {
    // Open a table writer.
    val uriString: String = process.getConfigCopy().get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    // TODO: Check and see if Kiji.Factory.open should be passed the configuration object in
    //     process.
    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(uri.getTable())) { table: KijiTable =>
        // Set the sink context to an opened KijiTableWriter.
        sinkCall.setContext(KijiSinkContext(table.openTableWriter(), table.getLayout))
      }
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table. This method is called once
   * for each row on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sink(
      process: FlowProcess[JobConf],
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]) {
    // Retrieve writer from the scheme's context.
    val KijiSinkContext(writer, layout) = sinkCall.getContext()

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry()
    putTuple(columns, getSinkFields(), output, writer, layout)
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkCleanup(
      process: FlowProcess[JobConf],
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]) {
    // Close the writer.
    sinkCall.getContext().kijiTableWriter.close()
    sinkCall.setContext(null)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case scheme: KijiScheme => columns == scheme.columns
      case _ => false
    }
  }

  override def hashCode(): Int = columns.hashCode()
}

/** Companion object for KijiScheme. Contains helper methods and constants. */
object KijiScheme {
  /** Field name containing a row's [[EntityId]]. */
  private[chopsticks] val entityIdField: String = "entityId"

  /**
   * Determines whether all of the columns requested are present in the given row.
   *
   * @param row The Kiji row to inspect.
   * @param columnsRequested The columns requested in this Scheme.
   * @return If all the columns requested are in the row.
   */
  private def allColumnsPresent(
      row: KijiRowData,
      columnsRequested: Map[String, ColumnRequest]): Boolean = {
    !columnsRequested.values.exists {
      case ColumnFamily(family, _) => {
        ! row.containsColumn(family)
      }
      case QualifiedColumn(family, qualifier, _) => {
        ! row.containsColumn(family, qualifier)
      }
    }
  }

  /**
   * Converts a KijiRowData to a Cascading tuple.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of desired tuple elements.
   * @param row The row data.
   * @return A tuple containing the values contained in the specified row.
   */
  private[chopsticks] def rowToTuple(
      columns: Map[String, ColumnRequest],
      fields: Fields,
      row: KijiRowData): Tuple = {
    val result: Tuple = new Tuple()
    val iterator = fields.iterator().asScala

    // Add the row's EntityId to the tuple.
    result.add(row.getEntityId())
    // TODO(CHOP-???): Validate that the first field is 'entityId'.
    iterator.next()

    // Add the rest.
    // Get the column request associated with each field.
    iterator.map { field => columns(field.toString) }
        // Build the tuple, by adding each requested value into result.
        .foreach {
          case ColumnFamily(family, _) => {
            val familyValues: java.util.NavigableMap[String,
              java.util.NavigableMap[java.lang.Long, AnyRef]] = row.getValues[AnyRef](family)
            val scalaFamilyValues: collection.mutable.Map[String, java.util.NavigableMap[java.lang.Long, AnyRef]] = familyValues.asScala
            result.add(scalaFamilyValues.map{ kv:(String, util.NavigableMap[java.lang.Long, AnyRef]) =>
              ((kv._1), convertKijiValuesToScalaTypes(kv._2))
            })
          }
          case QualifiedColumn(family, qualifier, _) => {
            result.add(convertKijiValuesToScalaTypes(row.getValues(family, qualifier)))
          }
        }
    return result
  }

  private[chopsticks] def convertKijiValuesToScalaTypes[T](map: java.util.NavigableMap[java.lang.Long, T]):Map[Long, Any] = {
    (map.asScala.map(kv => (kv._1.longValue(), convertType(kv._2)))).toMap
  }

  private def convertType[T](columnValue: T): Any = {
    if (null == columnValue) null
    else
      columnValue match {
        case i: java.lang.Integer => i
        case b: java.lang.Boolean => b
        case l: java.lang.Long => l
        case f: java.lang.Float => f
        case d: java.lang.Double => d
        // bytes
        case bb: java.nio.ByteBuffer => bb.array()
        // string
        case s: java.lang.CharSequence => s.toString()
        // array
        case l: java.util.List[Any] => {l.asScala.toList.map(elem => convertType(elem))}
        // map
        case m: java.util.Map[Any, Any] => m.asScala.toMap.map(kv =>
          (convertType(kv._1), convertType(kv._2)))
        // fixed
        case f: SpecificFixed => f.bytes().array
        // null field
        case n: java.lang.Void => null
        // enum
        case e: java.lang.Enum[Any] => e
        // avro record or object
        case a: IndexedRecord => a
        // any other type we don't understand
        case _ => throw new InvalidClassException("Unable to convert type")
      }
  }

  // TODO(CHOP-35): Use an output format that writes to HFiles.
  /**
   * Writes a Cascading tuple to a Kiji table.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of incoming tuple elements.
   * @param output Tuple to write out.
   * @param writer KijiTableWriter to use to write.
   */
  private[chopsticks] def putTuple [T](
      columns: Map[String, ColumnRequest],
      fields: Fields,
      output: TupleEntry,
      writer: KijiTableWriter,
      layout: KijiTableLayout) {
    val iterator = fields.iterator().asScala

    // Get the entityId.
    val entityId: EntityId = output.getObject(entityIdField).asInstanceOf[EntityId]
    iterator.next()

    // Store the retrieved columns in the tuple.
    iterator.foreach { fieldName =>
      columns(fieldName.toString()) match {
        case ColumnFamily(family, _) => {
          val kijiCol = new KijiColumnName(family)
          writer.put(
              entityId,
              family,
              null,
              {
                val familyMap =
                  output.getObject(fieldName.toString()).asInstanceOf[Map[String, Map[Long, T]]]
                val resMap: util.NavigableMap[String, util.NavigableMap[java.lang.Long, java.lang.Object]] =
                  new java.util.TreeMap(familyMap.map(kv =>(
                    new java.lang.String(kv._1),
                    convertScalaTypesToKijiValues(kv._2, layout, kijiCol)
                    )).asJava)
              })
        }
        case QualifiedColumn(family, qualifier, _) => {
          val kijiCol = new KijiColumnName(family, qualifier)
          writer.put(
              entityId,
              family,
              qualifier,
              convertScalaTypes(output.getObject(
                fieldName.toString()), layout, kijiCol))
        }
      }
    }
  }

  def convertScalaTypesToKijiValues[T](map:Map[Long, T], layout: KijiTableLayout, columnName: KijiColumnName)
    :java.util.NavigableMap[java.lang.Long, java.lang.Object] = {
    new java.util.TreeMap(map.map(kv => (java.lang.Long.valueOf(kv._1), convertScalaTypes(kv._2, layout, columnName))).asJava)
  }

  private def convertScalaTypes[T](columnValue: T, layout: KijiTableLayout, columnName: KijiColumnName): java.lang.Object = {
    if (null == columnValue) null
    else
      columnValue match {
        case i: Int => i.asInstanceOf[java.lang.Integer]
        case b: Boolean => b.asInstanceOf[java.lang.Boolean]
        case l: Long => l.asInstanceOf[java.lang.Long]
        case f: Float => f.asInstanceOf[java.lang.Float]
        case d: Double => d.asInstanceOf[java.lang.Double]
        // bytes or fixed
        case bb: Array[Byte] => {
          // this case will have an inline schema with its description
          val schemaDesc = layout.getCellSchema(columnName).getValue()
          if (schemaDesc == "\"bytes\"")
            java.nio.ByteBuffer.wrap(bb)
          else if (schemaDesc.contains("\"fixed\""))
            new Fixed(new Schema.Parser().parse(schemaDesc), bb)
          else
            throw new InvalidLayoutException("Invalid schema for column")
        }
        // string
        case s: String => s
        // array
        // we don't pass column name down while converting individual elements to avoid confusion
        case l: List[Any] => (l.map(elem => convertScalaTypes(elem, layout, null))).asJava
        // map
        case m: Map[Any, Any] => new java.util.TreeMap[Any, Any](
          // we don't pass column name down while converting individual elements to avoid confusion
          m.map(kv => (convertScalaTypes(kv._1, layout, null), convertScalaTypes(kv._2, layout, null))).asJava)
        // enum
        case e: java.lang.Enum[Any] => e
        // avro record or object
        case a: IndexedRecord => a
        // any other type we don't understand
        case _ => throw new InvalidClassException("Unable to convert type")
      }
  }

  private[chopsticks] def buildRequest(
      timeRange: TimeRange,
      columns: Iterable[ColumnRequest]): KijiDataRequest = {
    def addColumn(builder: KijiDataRequestBuilder, column: ColumnRequest) {
      column match {
        case ColumnFamily(family, inputOptions) => {
          builder.newColumnsDef()
              .withMaxVersions(inputOptions.maxVersions)
              .withFilter(inputOptions.filter)
              .add(new KijiColumnName(family))
        }
        case QualifiedColumn(family, qualifier, inputOptions) => {
          builder.newColumnsDef()
              .withMaxVersions(inputOptions.maxVersions)
              .withFilter(inputOptions.filter)
              .add(new KijiColumnName(family, qualifier))
        }
      }
    }

    val requestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
        .withTimeRange(timeRange.begin, timeRange.end)

    columns
        .foldLeft(requestBuilder) { (builder, column) =>
          addColumn(builder, column)
          builder
        }
        .build()
  }

  private[chopsticks] def buildFields(fieldNames: Iterable[String]): Fields = {
    val fieldArray: Array[Fields] = (Seq(entityIdField) ++ fieldNames)
        .map { name: String => new Fields(name) }
        .toArray

    Fields.join(fieldArray: _*)
  }
}
