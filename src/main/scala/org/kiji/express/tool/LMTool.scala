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

import org.kiji.annotations.{ApiAudience, ApiStability}
import com.twitter.scalding.Tool
import org.kiji.express.modeling.config._
import org.kiji.express.modeling.lib.LMTrainer
import org.kiji.express.modeling.config.ExpressColumnRequest
import org.kiji.express.modeling.config.ExpressDataRequest
import scala.Some
import org.kiji.express.modeling.framework.ModelExecutor
import org.slf4j.Logger
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory


@ApiAudience.Private
@ApiStability.Experimental
final class LMTool extends Tool {

  override def run(args: Array[String]): Int = {
    val datasetTableURI: String = args(0)
    val paramsFilePath: String = args(1)

    val modelDefinition: ModelDefinition = ModelDefinition(
      name = "lm-model-def",
      version = "1.0",
      trainerClass = Some(classOf[LMTrainer]))

    val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue, Seq(
      new ExpressColumnRequest("attributes", 1, None),
      new ExpressColumnRequest("target", 1, None)
    ))

    val modelEnvironment: ModelEnvironment = ModelEnvironment(
      name = "lm-model-environment",
      version = "1.0",
      prepareEnvironment = None,
      trainEnvironment = Some(TrainEnvironment(
        inputSpec = Map(
          "dataset" ->
            KijiInputSpec(datasetTableURI,
                          dataRequest = request,
                          fieldBindings = Seq(
                            FieldBinding(tupleFieldName = "attributes",
                                          storeFieldName = "attributes"),
                            FieldBinding(tupleFieldName = "target",
                                          storeFieldName = "target")))),
          /*"parameters" -> None
            /*TextFileInputSpec*/),*/
        outputSpec = Map(
          /*"parameters" ->
          TextFileOutputSpec*/),
        keyValueStoreSpecs = Seq()
      )),
      scoreEnvironment = None
    )
    // Build the produce job.
    val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)
    modelExecutor.runTrainer() match {
      case true => 0
      case false => 1
    }
  }
}

/*
 * The companion object to ScoreJobTool that only contains a main method.
 */
object LMTool {
  val LOGGER: Logger = LoggerFactory.getLogger(LMTool.getClass)
  /**
   * The entry point into the tool.
   *
   * @param args from the command line.
   * @return a return code that signals the success of the specified job.
   */
  def main(args: Array[String]) {

    if (args.length < 2){
      println("Usage: LMTool <dataset-table-URI> <parameters-file-path>")
      System.exit(1)
    }

    ToolRunner.run(HBaseConfiguration.create(), new LMTool, args)
  }

}
