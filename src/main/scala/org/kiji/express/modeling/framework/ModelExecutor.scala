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

import org.apache.hadoop.conf.Configuration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.Extractor
import org.kiji.express.modeling.impl.ModelJobUtils
import org.kiji.express.modeling.ScoreProducerJobBuilder
import org.kiji.express.modeling.Preparer
import org.kiji.express.modeling.Scorer
import org.kiji.express.modeling.Trainer
import org.apache.hadoop.hbase.HBaseConfiguration
import com.twitter.scalding.Source
import org.kiji.express.modeling.impl.ModelJobUtils.PhaseType

/**
 * The ModelExecutor can be used to run valid combinations of the model lifecycle.
 * Build the ModelExecutor by providing it with the appropriate [[org.kiji.express.modeling
 * .config.ModelDefinition]] and [[org.kiji.express.modeling.config.ModelEnvironment]] as follows:
 *
 * val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)
 *
 * You can then run all the defined phases as:
 * {{{
 * modelExecutor.run()
 * }}}
 * You can also run individual phases as:
 * {{{
 * modelExecutor.runPreparer()
 * modelExecutor.runTrainer()
 * modelExecutor.runScorer()
 * }}}
 *
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class ModelExecutor (
    _modelDefinition: ModelDefinition,
    _modelEnvironment: ModelEnvironment,
    _hadoopConfiguration: Configuration) {
  /** Preparer to use for this model definition. Optional. */
  private[this] var preparer: Option[Preparer] = None

  /** Trainer to use for this model definition. Optional. */
  private[this] var trainer: Option[Trainer] = None

  /** ScoreExtractor to use for this model definition. This variable must be initialized. */
  private[this] var scoreExtractor: Option[Extractor] = None

  /** Scorer to use for this model definition. This variable must be initialized. */
  private[this] var scorer: Option[Scorer] = None

  private def getInstanceForPhaseClass[T](classForPhase: Option[java.lang.Class[_ <: T]])
      : Option[T] = {
    classForPhase
      .map {
      cname: Class[_ <: T] => cname.newInstance()
    }
  }

  // Make an instance from the class of each phase.
  preparer = getInstanceForPhaseClass[Preparer](_modelDefinition.preparerClass)
  trainer = getInstanceForPhaseClass[Trainer](_modelDefinition.trainerClass)

  scoreExtractor = getInstanceForPhaseClass[Extractor](_modelDefinition.scoreExtractor)
  scorer = getInstanceForPhaseClass[Scorer](_modelDefinition.scorerClass)

  /**
   * Runs the prepare phase of the [[org.kiji.express.modeling.config.ModelDefinition]] provided
   * to this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call this
   * when the prepare phase is not defined.
   *
   * @return true if prepare phase succeeds, false otherwise.
   */
  def runPreparer(): Boolean = {
    if (preparer.isEmpty) {
      throw new IllegalArgumentException("A preparer has not been provided in the Model " +
        "Definition")
    }
    val input: Source = ModelJobUtils.inputSpecToSource(_modelEnvironment, PhaseType.PREPARE)
    val output: Source = ModelJobUtils.outputSpecToSource(_modelEnvironment, PhaseType.PREPARE)
    preparer.get.prepare(input, output)
  }


  /**
   * Runs the train phase of the [[org.kiji.express.modeling.config.ModelDefinition]] provided to
   * this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call this when the
   * train phase is not defined.
   *
   * @return true if the train phase succeeds, false otherwise.
   */
  def runTrainer(): Boolean = {
    if (trainer.isEmpty) {
      throw new IllegalArgumentException("A trainer has not been provided in the Model " +
        "Definition")
    }
    val input: Source = ModelJobUtils.inputSpecToSource(_modelEnvironment, PhaseType.TRAIN)
    val output: Source = ModelJobUtils.outputSpecToSource(_modelEnvironment, PhaseType.TRAIN)
    trainer.get.train(input, output)
  }

  /**
   * Runs the extract-score phase of the [[org.kiji.express.modeling.config.ModelDefinition]]
   * provided to this [[org.kiji.express.modeling.framework.ModelExecutor]]. It is illegal to call
   * this when the score phase is not defined.
   *
   * @return true if the score phase succeeds, false otherwise.
   */
  def runScorer(): Boolean = {
    ScoreProducerJobBuilder
        .buildJob(_modelDefinition, _modelEnvironment, _hadoopConfiguration)
        .run()
  }

  /**
   * Runs all the phases defined by the [[org.kiji.express.modeling.config.ModelDefinition]].
   *
   * @return true if all the phases succeed, false otherwise.
   */
  def run(): Boolean = {
    (preparer.isEmpty || runPreparer()) &&
        (trainer.isEmpty || runTrainer()) &&
        (scorer.isEmpty || runScorer())
  }
}

/**
 * The companion object to [[org.kiji.express.modeling.framework.ModelExecutor]].
 *
 */
object ModelExecutor {
  /**
   * Factory method for constructing a ModelExecutor.
   *
   * @param modelDefinition which defines the phase classes for this executor.
   * @param modelEnvironment which specifies how to run this executor.
   * @param hadoopConfiguration for this executor. Optional.
   * @return a [[org.kiji.express.modeling.framework.ModelExecutor]] that can run the phases.
   */
  def apply(modelDefinition: ModelDefinition,
      modelEnvironment: ModelEnvironment,
      hadoopConfiguration: Configuration = HBaseConfiguration.create()): ModelExecutor = {
    new ModelExecutor(modelDefinition, modelEnvironment, hadoopConfiguration)
  }
}
