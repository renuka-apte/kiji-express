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

package org.kiji.express.tool

import com.twitter.scalding.Args
import com.twitter.scalding.Mode
import com.twitter.scalding.Tool

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.framework.ModelExecutor

/**
 * <p>
 * Provides the ability to run the various phases of the model lifecycle via the command line.
 * This tool expects two flags: model-def and model-env.
 * The value of the flag "model-def" is expected to be the path in the local filesystem to the
 * JSON file that specifies the model definition. The value of the flag "model-env" is expected
 * to be the path in the local filesystem to the JSON file that specifies the model environment.
 * </p>
 * <p>
 * You can decide to run in local or hdfs mode via flags: --local or --hdfs
 * </p>
 * <p>
 * You can optionally specify the flags --skip-prepare, --skip-train, --skip-score.
 * </p>
 */
@ApiAudience.Private
@ApiStability.Experimental
class ModelLifecycleTool extends Tool {
  // parse arguments, create trainer and run it.
  override def run(args: Array[String]): Int = {
    // TODO EXP-204, pass the mode argument to the model executor
    val (mode: Mode, jobArgs: Args) = parseModeArgs(args)
    Mode.mode = mode

    val modelDefinitionPath: String = jobArgs("model-def")
    val modelEnvironmentPath: String = jobArgs("model-env")

    val modelExecutor = ModelExecutor(
        ModelDefinition.fromJsonFile(modelDefinitionPath),
        ModelEnvironment.fromJsonFile(modelEnvironmentPath),
        jobArgs,
        getConf)

    val skipPrepare: Boolean = jobArgs.boolean("skip-prepare")
    val skipTrain: Boolean = jobArgs.boolean("skip-train")
    val skipScore: Boolean = jobArgs.boolean("skip-score")

    (skipPrepare || modelExecutor.runPreparer()) &&
        (skipTrain || modelExecutor.runTrainer()) &&
        (skipScore || modelExecutor.runScorer()) match {
      case true => 0
      case false => 1
    }
  }
}

/*
 * The companion object to ModelLifecycleTool that only contains a main method.
 */
object ModelLifecycleTool {
  /**
   * The entry point into the tool.
   *
   * @param args from the command line.
   * @return a return code that signals the success of the specified job.
   */
  def main(args: Array[String]) {
    ToolRunner.run(HBaseConfiguration.create(), new ModelLifecycleTool, args)
  }
}