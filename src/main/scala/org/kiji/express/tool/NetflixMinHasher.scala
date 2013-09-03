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

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.kiji.express.modeling.ScoreProducerJobBuilder
import org.kiji.express.modeling.config._
import org.kiji.express.modeling.lib.LSHMoviePreparer
import scala.Some
import scala.Some
import org.kiji.express.util.Resources._
import scala.Some
import org.kiji.schema.{KijiURI, KijiTable}
import org.kiji.express.modeling.framework.ModelExecutor

@ApiAudience.Private
@ApiStability.Experimental
final class NetflixMinHasher extends Configured with Tool {

  override def run(args: Array[String]): Int = {
    val modelDefinition: ModelDefinition = ModelDefinition(
      name = "iterative-prepare-model-def",
      version = "1.0",
      preparer = Some(classOf[LSHMoviePreparer]))

    val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
      new ExpressColumnRequest("rating", 1, None) :: Nil)

    val modelEnvironment: ModelEnvironment = ModelEnvironment(
      name = "movie-minhash-model-environment",
      version = "1.0",
      prepareEnvironment = Some(PrepareEnvironment(
        inputConfig = KijiInputSpec(
          "kiji://.env/default/movies",
          dataRequest = request,
          fieldBindings = Seq(
            FieldBinding(tupleFieldName = "movieVector", storeFieldName = "rating")
          )
        ),
        outputConfig = KijiOutputSpec(
          tableUri = "kiji://.env/default/movies",
          fieldBindings = Seq(
            FieldBinding(tupleFieldName = "movieBuckets", storeFieldName = "info:buckets"))
        ),
        kvstores = Seq()
      )),
      trainEnvironment = None,
      scoreEnvironment = None
    )
    // Build the produce job.
    val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)
    modelExecutor.runPreparer() match {
      case true => 0
      case false => 1
    }
  }
}

/*
 * The companion object to ScoreJobTool that only contains a main method.
 */
object NetflixMinHasher {
  val LOGGER: Logger = LoggerFactory.getLogger(NetflixMinHasher.getClass)
  /**
   * The entry point into the tool.
   *
   * @param args from the command line.
   * @return a return code that signals the success of the specified job.
   */
  def main(args: Array[String]) {
    ToolRunner.run(HBaseConfiguration.create(), new NetflixMinHasher, args)
  }
}
