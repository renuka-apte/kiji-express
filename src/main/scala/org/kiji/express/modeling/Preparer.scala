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

package org.kiji.express.modeling

import cascading.pipe.Pipe
import com.twitter.scalding._

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.modeling.impl.KeyValueStores
import org.kiji.express.repl.Implicits
import org.kiji.express.util.PipeConversions
import org.kiji.express.flow.KijiJob

/**
 * Represents the Prepare phase of a model lifecycle. Users should extend this trait when defining a
 * Prepare phase. The Prepare phase of a model lifecycle is responsible for performing computations
 * required by one or more other lifecycle phases.
 *
 * To define a custom prepare phase, the prepare method of the must be overridden. This method takes
 * as parameters already configured input and output data sources. Both input and output
 * configurations are stored in a [[org.kiji.express.modeling.config.ModelEnvironment]]. For
 * example:
 * {{{
 *   class MyPreparer extends Preparer {
 *     override def prepare(input: Source, output: Source) {
 *       new TrainerJob {
 *         input
 *             .map('inputField -> 'intermediateField) { inputColumn: KijiSlice[String] =>
 *               inputColumn.getFirstValue
 *             }
 *             .groupBy('intermediateField) { _.count('count) }
 *             .write(output)
 *       }.run.finish
 *     }
 *   }
 * }}}
 *
 * This trait also provides access to outside data sources required for the Prepare phase through
 * the `kvstores` property.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
trait Preparer
    extends KeyValueStores {

  /**
   * Override this class to implement a MapReduce flow for the prepare phase.
   */
  abstract class PreparerJob extends KijiJob(Args(Nil))

  /**
   * Used to define the computation required for the Prepare phase of the model lifecycle.
   *
   * @param input data source used during the prepare phase.
   * @param output data source used during the prepare phase.
   */
  def prepare(
      input: Source,
      output: Source): Unit
}