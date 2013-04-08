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
import scala.collection.mutable.Buffer
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.scheme.ConcreteCall
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import com.twitter.scalding.AccessMode
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Test
import com.twitter.scalding.Write
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources._
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI

/**
 * A read or write view of a Kiji table.
 *
 * A Scalding `Source` provides a view of a data source that can be read as Scalding tuples. A
 * `Source` is also a `Pipe`, which can be used (via functional operators) to process a data set.
 *
 * When reading from a Kiji table, a `KijiSource` will provide a view of a Kiji table as a
 * collection of tuples that correspond to rows from the Kiji table. Which columns will be read
 * and how they are associated with tuple fields can be configured,
 * as well as the time span that cells retrieved must belong to.
 *
 * When writing to a Kiji table, a `KijiSource` views a Kiji table as a collection of tuples that
 * correspond to cells from the Kiji table. Each tuple to be written must provide a cell address
 * by specifying a Kiji `EntityID` in the tuple field `entityId`, a value to be written in a
 * configurable field, and (optionally) a timestamp in a configurable field.
 *
 * End-users cannot directly obtain instances of `KijiSource`. Instead,
 * they should use the factory methods in [[org.kiji.chopsticks.DSL]].
 *
 * @param tableAddress is a Kiji URI addressing the Kiji table to read or write to.
 * @param timeRange that cells read must belong to. Ignored when the source is used to write.
 * @param timestampField is the name of a tuple field that will contain cell timestamp when the
 *                       source is used for writing. Specify `None` to write all
 *                       cells at the current time.
 * @param loggingInterval The interval at which to log skipped rows.
 * @param columns is a one-to-one mapping from field names to Kiji columns. When reading,
 *                the columns in the map will be read into their associated tuple fields. When
 *                writing, values from the tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class KijiSource private[chopsticks] (
    val tableAddress: String,
    val timeRange: TimeRange,
    val timestampField: Option[Symbol],
    val loggingInterval: Long,
    val columns: Map[Symbol, ColumnRequest])
    extends Source {
  import KijiSource._

  private type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  /** The URI of the target Kiji table. */
  private val tableUri: KijiURI = KijiURI.newBuilder(tableAddress).build()
  /** A Kiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  private val kijiScheme: KijiScheme =
      new KijiScheme(timeRange, timestampField, loggingInterval, convertColumnMap(columns))
  /** A Kiji scheme intended to be used with Scalding/Cascading's local mode. */
  private val localKijiScheme: LocalKijiScheme = {
    new LocalKijiScheme(timeRange, timestampField, convertColumnMap(columns))
  }

  /**
   * Convert scala columns definition into its corresponding java variety.
   *
   * @param columnMap Mapping from field name to Kiji column name.
   * @return Java map from field name to column definition.
   */
  private def convertColumnMap(columnMap: Map[Symbol, ColumnRequest])
      : Map[String, ColumnRequest] = {
    columnMap.map { case (symbol, column) => (symbol.name, column) }
  }

  /**
   * Takes a buffer containing rows and writes them to the table at the specified uri.
   *
   * @param rows Tuples to write to populate the table with.
   * @param fields Field names for elements in the tuple.
   */
  private def populateTestTable(rows: Buffer[Tuple], fields: Fields) {
    // Open a table writer.
    val writer =
        doAndRelease(Kiji.Factory.open(tableUri)) { kiji =>
          doAndRelease(kiji.openTable(tableUri.getTable())) { table =>
            table.openTableWriter()
          }
        }

    // Write the desired rows to the table.
    try {
      rows.foreach { row: Tuple =>
        val tupleEntry = new TupleEntry(fields, row)
        val iterator = fields.iterator()

        // Get the entity id field.
        val entityIdField = iterator.next().toString()
        val entityId = tupleEntry
            .getObject(entityIdField)
            .asInstanceOf[EntityId]

        // Iterate through fields in the tuple, adding each one.
        while (iterator.hasNext()) {
          val field = iterator.next().toString()

          // Get the timeline to be written.
          val cells: Seq[Cell[Any]] = tupleEntry.getObject(field)
              .asInstanceOf[KijiSlice[Any]].cells

          // Write the timeline to the table.
              cells.map { cell: Cell[Any] =>
                // scalastyle:off null
                writer.put(
                    entityId,
                    cell.family,
                    cell.qualifier,
                    cell.version,
                    cell.datum)
                // scalastyle:on null
              }
        }
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the hadoop runner.
   */
  override val hdfsScheme: HadoopScheme = kijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[HadoopScheme]

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the local runner.
   */
  override val localScheme: LocalScheme = localKijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[LocalScheme]

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[org.kiji.schema.KijiTable]].
   *
   * @param readOrWrite Specifies if this source is to be used for reading or writing.
   * @param mode Specifies which job runner/flow planner is being used.
   * @return A tap to use for this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tap: Tap[_, _, _] = mode match {
      // Production taps.
      case Hdfs(_,_) => new KijiTap(tableUri, kijiScheme).asInstanceOf[Tap[_, _, _]]
      case Local(_) => new LocalKijiTap(tableUri, localKijiScheme).asInstanceOf[Tap[_, _, _]]

      // Test taps.
      case HadoopTest(_, buffers) => {
        readOrWrite match {
          case Read => {
            val scheme = kijiScheme
            populateTestTable(buffers(this), scheme.getSourceFields())

            new KijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
          case Write => {
            val scheme = new TestKijiScheme(buffers(this), timeRange,
                timestampField, loggingInterval, convertColumnMap(columns))

            new KijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
        }
      }
      case Test(buffers) => {
        readOrWrite match {
          // Use Kiji's local tap and scheme when reading.
          case Read => {
            val scheme = localKijiScheme
            populateTestTable(buffers(this), scheme.getSourceFields())

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }

          // After performing a write, use TestLocalKijiScheme to populate the output buffer.
          case Write => {
            val scheme = new TestLocalKijiScheme(buffers(this), timeRange, timestampField,
                convertColumnMap(columns))

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
        }
      }

      // Delegate any other tap types to Source's default behaviour.
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }

  override def equals(other: Any): Boolean = {
    other match {
      case source: KijiSource =>
        Objects.equal(tableAddress, source.tableAddress) &&
        Objects.equal(columns, source.columns) &&
        Objects.equal(timestampField, source.timestampField) &&
        Objects.equal(timeRange, source.timeRange)
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableAddress, columns, timestampField, timeRange)
}

/**
 * Contains a private, inner class used by [[org.kiji.chopsticks.KijiSource]] when working with
 * tests.
 */
object KijiSource {
  /**
   * A LocalKijiScheme that loads rows in a table into the provided buffer. This class
   * should only be used during tests.
   *
   * @param buffer Buffer to fill with post-job table rows for tests.
   * @param timeRange Range of timestamps to read from each column.
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *                       source is used for writing. Specify the empty field name to write all
   *                       cells at the current time.
   * @param columns Scalding field name to Kiji column name mapping.
   */
  private class TestLocalKijiScheme(
      val buffer: Buffer[Tuple],
      timeRange: TimeRange,
      timestampField: Option[Symbol],
      columns: Map[String, ColumnRequest])
      extends LocalKijiScheme(timeRange, timestampField, columns) {
    override def sinkConfInit(
        process: FlowProcess[Properties],
        tap: Tap[Properties, InputStream, OutputStream],
        conf: Properties) {
      super.sinkConfInit(process, tap, conf)

      // Store output uri as input uri.
      tap.sourceConfInit(process, conf)
    }

    override def sinkCleanup(
        process: FlowProcess[Properties],
        sinkCall: SinkCall[OutputContext, OutputStream]) {
      super.sink(process, sinkCall)

      // Read table into buffer.
      val sourceCall: ConcreteCall[InputContext, InputStream] = new ConcreteCall()
      sourceCall.setIncomingEntry(new TupleEntry())
      sourcePrepare(process, sourceCall)
      while (source(process, sourceCall)) {
        buffer += sourceCall.getIncomingEntry().getTuple()
      }
      sourceCleanup(process, sourceCall)

      super.sinkCleanup(process, sinkCall)
    }
  }

  /**
   * A KijiScheme that loads rows in a table into the provided buffer. This class should only be
   * used during tests.
   *
   * @param buffer Buffer to fill with post-job table rows for tests.
   * @param timeRange Range of timestamps to read from each column.
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *                       source is used for writing. Specify the empty field name to write all
   *                       cells at the current time.
   * @param columns Scalding field name to Kiji column name mapping.
   */
  private class TestKijiScheme(
      val buffer: Buffer[Tuple],
      timeRange: TimeRange,
      timestampField: Option[Symbol],
      loggingInterval: Long,
      columns: Map[String, ColumnRequest])
      extends KijiScheme(timeRange, timestampField, loggingInterval, columns) {
    override def sinkCleanup(
        process: FlowProcess[JobConf],
        sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]) {
      super.sink(process, sinkCall)

      // Store the output table.
      val uri: KijiURI = KijiURI
          .newBuilder(process.getConfigCopy().get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI))
          .build()

      // Read table into buffer.
      doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
        doAndRelease(kiji.openTable(uri.getTable())) { table: KijiTable =>
          doAndClose(table.openTableReader()) { reader: KijiTableReader =>
            val request: KijiDataRequest =
                KijiScheme.buildRequest(timeRange, columns.values)
            doAndClose(reader.getScanner(request)) { scanner: KijiRowScanner =>
              val rows: Iterator[KijiRowData] = scanner.iterator().asScala

              rows.foreach { row: KijiRowData =>
                KijiScheme
                    .rowToTuple(columns, getSinkFields(), timestampField, row)
                    .foreach { tuple => buffer += tuple }
              }
            }
          }
        }
      }

      super.sinkCleanup(process, sinkCall)
    }
  }
}
