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

package org.kiji.express.modeling.framework

import scala.collection.JavaConverters._
import scala.Some

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.KijiSlice
import org.kiji.express.avro.KvStoreType
import org.kiji.express.modeling._
import org.kiji.express.modeling.config.KijiInputSpec
import org.kiji.express.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.express.modeling.config.KVStore
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.impl.AvroKVRecordKeyValueStore
import org.kiji.express.modeling.impl.AvroRecordKeyValueStore
import org.kiji.express.modeling.impl.KijiTableKeyValueStore
import org.kiji.express.util.GenericRowDataConverter
import org.kiji.express.util.Tuples
import org.kiji.mapreduce.KijiContext
import org.kiji.mapreduce.kvstore.{ KeyValueStore => JKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroRecordKeyValueStore => JAvroRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ KijiTableKeyValueStore => JKijiTableKeyValueStore }
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.mapreduce.produce.ProducerContext
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI
import org.kiji.express.modeling.ExtractFn
import scala.Some
import org.kiji.express.modeling.ScoreFn
import org.kiji.schema.shell.DDLException
import org.apache.hadoop.hbase.HBaseConfiguration

@ApiAudience.Framework
@ApiStability.Experimental
final class ModelExecutor (_modelDefinition: ModelDefinition,
    _modelEnvironment: ModelEnvironment,
    _hadoopConfiguration: Configuration) {
  /** PrepareExtractor to use for this model definition. Optional. */
  private[this] var prepareExtractor: Option[Extractor] = None
  /** Preparer to use for this model definition. Optional. */
  private[this] var preparer: Option[Preparer] = None

  /** TrainExtractor to use for this model definition. Optional. */
  private[this] var trainExtractor: Option[Extractor] = None
  /** Trainer to use for this model definition. Optional. */
  private[this] var trainer: Option[Trainer] = None

  /** ScoreExtractor to use for this model definition. This variable must be initialized. */
  private[this] var scoreExtractor: Option[Extractor] = None
  /** Scorer to use for this model definition. This variable must be initialized. */
  private[this] var scorer: Option[Scorer] = None

  def getInstanceForPhaseClass[T](classForPhase: Option[java.lang.Class[_ <: T]]) : Option[T] = {
    classForPhase
      .map {
      cname: Class[_ <: T] => cname.newInstance()
    }
  }

  // Make an instance from the class of each phase.
  prepareExtractor = getInstanceForPhaseClass[Extractor](_modelDefinition.prepareExtractor)
  preparer = getInstanceForPhaseClass[Preparer](_modelDefinition.preparerClass)

  trainExtractor = getInstanceForPhaseClass[Extractor](_modelDefinition.trainExtractor)
  trainer = getInstanceForPhaseClass[Trainer](_modelDefinition.trainerClass)

  scoreExtractor = getInstanceForPhaseClass[Extractor](_modelDefinition.scoreExtractor)
  scorer = getInstanceForPhaseClass[Scorer](_modelDefinition.scorerClass)

  def runScore(): Boolean = {
    ScoreProducerJobBuilder
        .buildJob(_modelDefinition, _modelEnvironment, _hadoopConfiguration)
        .run()
  }
}

object ModelExecutor {
  def apply(modelDefinition: ModelDefinition,
      modelEnvironment: ModelEnvironment,
      hadoopConfiguration: Configuration = HBaseConfiguration.create()): ModelExecutor = {
    new ModelExecutor(modelDefinition, modelEnvironment, hadoopConfiguration)
  }
}
