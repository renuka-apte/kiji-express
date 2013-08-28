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
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.modeling.impl

import com.twitter.scalding.Source
import org.kiji.schema.{KijiColumnName, KijiURI, KijiDataRequest}
import org.kiji.express.modeling.config._
import scala.Some
import org.kiji.mapreduce.KijiContext
import org.kiji.express.modeling.KeyValueStore
import org.kiji.express.avro.KvStoreType
import org.apache.hadoop.conf.Configuration
import org.kiji.mapreduce.kvstore.{ KeyValueStore => JKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroRecordKeyValueStore => JAvroRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ KijiTableKeyValueStore => JKijiTableKeyValueStore }
import org.apache.hadoop.fs.Path
import org.kiji.express.flow._
import org.kiji.express.flow.Between
import scala.Some

object ModelJobUtils {

  sealed trait PhaseType
  object PhaseType {
    object PREPARE extends PhaseType
    object TRAIN extends PhaseType
    object SCORE extends PhaseType
  }

  /**
   * Returns a KijiDataRequest that describes which input columns need to be available to the
   * producer.
   *
   * This method reads the Extract phase's data request configuration from this model's run profile
   * and builds a KijiDataRequest from it.
   *
   * @return a kiji data request if the phase exists or None.
   */
  def getDataRequest(modelEnvironment: ModelEnvironment,
      phase: PhaseType): Option[KijiDataRequest] = {
    val inputConfig: Option[InputSpec] = phase match {
      case PhaseType.PREPARE => modelEnvironment.prepareEnvironment.map {
        _.inputConfig
      }
      case PhaseType.TRAIN => modelEnvironment.trainEnvironment.map {
        _.inputConfig
      }
      case PhaseType.SCORE => modelEnvironment.scoreEnvironment.map {
        _.inputConfig
      }
    }
    inputConfig match {
      case Some(inputConfig) => inputConfig match {
        case kijiInputSpec :KijiInputSpec => Some(kijiInputSpec.dataRequest.toKijiDataRequest())
        case _ => throw new RuntimeException("Input Specification is not of type KijiInputSpec")
      }
      case _ => None
    }
  }

  /**
   * Returns the name of the Kiji column this phase will write to.
   *
   * This method reads the Score phase's output column from this model's run profile and returns it.
   *
   * @return the output column name.
   */
  def getOutputColumn(modelEnvironment: ModelEnvironment): String = modelEnvironment
      .scoreEnvironment
      .get
      .outputConfig
      .asInstanceOf[KijiSingleColumnOutputSpec]
      .outputColumn

  /**
   * Wrap the provided kvstores in their scala counterparts.
   *
   * @param kvstores to open.
   * @param context providing access to the opened kvstores.
   * @return a mapping from the kvstore's name to the wrapped kvstore.
   */
  def wrapKvstoreReaders(
      kvstores: Seq[KVStore],
      context: KijiContext): Map[String, KeyValueStore[_, _]] = {
    return kvstores
        .map { kvstore: KVStore =>
          val jkvstoreReader = context.getStore(kvstore.name)
          val wrapped: KeyValueStore[_, _] = KvStoreType.valueOf(kvstore.storeType) match {
            case KvStoreType.AVRO_KV => new AvroKVRecordKeyValueStore(jkvstoreReader)
            case KvStoreType.AVRO_RECORD => new AvroRecordKeyValueStore(jkvstoreReader)
            case KvStoreType.KIJI_TABLE => new KijiTableKeyValueStore(jkvstoreReader)
          }
          (kvstore.name, wrapped)
        }
        .toMap
  }

  /**
   * Open the provided kvstore definitions.
   *
   * @param kvstores to open.
   * @param conf containing settings pertaining to the specified kvstores.
   * @return a mapping from the kvstore's name to the opened kvstore.
   */
  def openJKvstores(
      kvstores: Seq[KVStore],
      conf: Configuration): Map[String, JKeyValueStore[_, _]] = {
    kvstores
        // Open the kvstores defined for the extract phase.
        .map { kvstore: KVStore =>
          val properties = kvstore.properties

          // Handle each type of kvstore differently.
          val jkvstore: JKeyValueStore[_, _] = KvStoreType.valueOf(kvstore.storeType) match {
            case KvStoreType.AVRO_KV => {

              // Open AvroKV.
              val builder = JAvroKVRecordKeyValueStore
                  .builder()
                  .withConfiguration(conf)
                  .withInputPath(new Path(properties("path")))
              if (properties.contains("use_dcache")) {
                builder
                    .withDistributedCache(properties("use_dcache") == "true")
                    .build()
              } else {
                builder.build()
              }
            }
            case KvStoreType.AVRO_RECORD => {
              // Open AvroRecord.
              val builder = JAvroRecordKeyValueStore
                  .builder()
                  .withConfiguration(conf)
                  .withKeyFieldName(properties("key_field"))
                  .withInputPath(new Path(properties("path")))
              if (properties.contains("use_dcache")) {
                builder
                    .withDistributedCache(properties("use_dcache") == "true")
                    .build()
              } else {
                builder.build()
              }
            }
            case KvStoreType.KIJI_TABLE => {
              // Kiji table.
              val uri: KijiURI = KijiURI.newBuilder(properties("uri")).build()
              val columnName: KijiColumnName = new KijiColumnName(properties("column"))
              JKijiTableKeyValueStore
                  .builder()
                  .withConfiguration(conf)
                  .withTable(uri)
                  .withColumn(columnName.getFamily(), columnName.getQualifier())
                  .build()
            }
            case kvstoreType => throw new UnsupportedOperationException(
                "KeyValueStores of type \"%s\" are not supported".format(kvstoreType.toString))
          }

          // Pack the kvstore into a tuple with its name.
          (kvstore.name, jkvstore)
        }
        .toMap
  }

  private def getTimeRange(inputSpec: InputSpec): TimeRange = {
    inputSpec match {
      case kijiInputSpec: KijiInputSpec =>
        Between(kijiInputSpec.dataRequest.minTimeStamp,
            kijiInputSpec.dataRequest.maxTimeStamp)
      case _ => throw new IllegalStateException("Unsupported Input Specification")
    }
  }

  private def getInputColumnMap(inputSpec: KijiInputSpec): Map[ColumnRequest, Symbol] = {
    val columnMap: Map[ColumnRequest, String] = inputSpec.dataRequest.columnRequests.map (
        (columnReq: ExpressColumnRequest) => {
          val options = new ColumnRequestOptions(columnReq.maxVersions, columnReq.filter.map {
            _.getKijiColumnFilter()
          })
          val kijiColName = new KijiColumnName(columnReq.name)
          val colReq: ColumnRequest = if (kijiColName.isFullyQualified) {
            QualifiedColumn(kijiColName.getFamily, kijiColName.getQualifier, options)
          } else {
            // TODO specify regex matching for qualifier
            ColumnFamily(kijiColName.getFamily, None, options)
          }
          (colReq -> columnReq.name)
        }
    ).toMap
    val bindingMap: Map[String, String] = inputSpec.fieldBindings.seq.map(
        fieldBinding => {
          fieldBinding.storeFieldName -> fieldBinding.tupleFieldName
        }
    ).toMap
    columnMap.map{ case(k,v) => k -> Symbol(bindingMap(v)) }
  }

  def inputSpecToSource(modelEnvironment: ModelEnvironment, phase: PhaseType): Source = {
    val inputConfig: InputSpec = phase match {
      case PhaseType.PREPARE => modelEnvironment
          .prepareEnvironment
          .getOrElse {
            throw new IllegalArgumentException("Prepare environment does not exist")
          }
          .inputConfig
      case PhaseType.TRAIN => modelEnvironment
          .trainEnvironment
          .getOrElse {
            throw new IllegalArgumentException("Prepare environment does not exist")
          }
          .inputConfig
      case PhaseType.SCORE => modelEnvironment
          .scoreEnvironment
          .getOrElse {
            throw new IllegalArgumentException("Prepare environment does not exist")
          }
          .inputConfig
    }
    inputConfig match {
      case kijiInputSpec: KijiInputSpec => {
        KijiInput(kijiInputSpec.tableUri,
            getTimeRange(kijiInputSpec))
            .apply(getInputColumnMap(kijiInputSpec))
      }
      case _ => throw new IllegalArgumentException("Prepare environment does not exist")
    }
  }

  private def getOutputColumnMap(kijiOutputSpec: KijiOutputSpec): Seq[(Symbol, String)] = {
    kijiOutputSpec.fieldBindings.map(fieldBinding => {
      (Symbol(fieldBinding.tupleFieldName), fieldBinding.storeFieldName)
    })
  }

  def outputSpecToSource(modelEnvironment: ModelEnvironment, phase: PhaseType): Source = {
    val outputConfig: OutputSpec = phase match {
      case PhaseType.PREPARE => modelEnvironment
        .prepareEnvironment
        .getOrElse {
          throw new IllegalArgumentException("Prepare environment does not exist")
        }
        .outputConfig
      case PhaseType.TRAIN => modelEnvironment
        .trainEnvironment
        .getOrElse {
          throw new IllegalArgumentException("Prepare environment does not exist")
        }
        .outputConfig
      case PhaseType.SCORE => modelEnvironment
        .scoreEnvironment
        .getOrElse {
          throw new IllegalArgumentException("Prepare environment does not exist")
        }
        .outputConfig
    }
    outputConfig match {
      case kijiOutputSpec: KijiOutputSpec => {
        val kijiOutput: KijiOutput = if (kijiOutputSpec.timeStampField.isDefined) {
          KijiOutput(kijiOutputSpec.tableUri,
            Symbol(kijiOutputSpec.timeStampField.get))
        } else {
          KijiOutput(kijiOutputSpec.tableUri)
        }
        kijiOutput.apply(getOutputColumnMap(kijiOutputSpec):_*)
      }
      case _ => throw new IllegalArgumentException("Prepare environment does not exist")
    }
  }
}
