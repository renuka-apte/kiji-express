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

package org.kiji.express.modeling.lib

import java.io.File

import com.twitter.scalding.Hdfs

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.KijiSuite
import org.kiji.express.modeling.config.ExpressColumnRequest
import org.kiji.express.modeling.config.ExpressDataRequest
import org.kiji.express.modeling.config.FieldBinding
import org.kiji.express.modeling.config.KijiInputSpec
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.express.modeling.config.TextSourceSpec
import org.kiji.express.modeling.config.TrainEnvironment
import org.kiji.express.modeling.framework.ModelExecutor
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.util.InstanceBuilder

/**
 * Tests the Linear Regression Trainer.
 */
@ApiAudience.Private
@ApiStability.Experimental
class LMTrainerSuite extends KijiSuite {
  /**
   * Tests that LMTrainer can calculate the equation for line y = x.
   */
  test("A Linear Regression trainer works correctly") {
    val inputTable: String = "two-double-columns.json"
    val paramsFile: String = "src/test/resources/sources/LRparams"
    val outputParams: String = "src/test/resources/sources/LROutput"

    val testLayoutDesc: TableLayoutDesc = layout(inputTable).getDesc
    testLayoutDesc.setName("lr_table")

    val kiji: Kiji = new InstanceBuilder("default")
      .withTable(testLayoutDesc)
      .withRow("row1")
      .withFamily("family")
      .withQualifier("column1").withValue(0.0)
      .withQualifier("column2").withValue(0.0)
      .withRow("row2")
      .withFamily("family")
      .withQualifier("column1").withValue(1.0)
      .withQualifier("column2").withValue(1.0)
      .withRow("row3")
      .withFamily("family")
      .withQualifier("column1").withValue(2.0)
      .withQualifier("column2").withValue(2.0)
      .build()

    val modelDefinition: ModelDefinition = ModelDefinition(
      name = "lr-model-def",
      version = "1.0",
      trainerClass = Some(classOf[LMTrainer]))

    val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
        Seq(new ExpressColumnRequest("family:column1", 1, None),
            new ExpressColumnRequest("family:column2", 1, None)))

    doAndRelease(kiji.openTable("lr_table")) { table: KijiTable =>
      val tableUri: KijiURI = table.getURI()

      val modelEnvironment: ModelEnvironment = ModelEnvironment(
          name = "lr-train-model-environment",
          version = "1.0",
          trainEnvironment = Some(TrainEnvironment(
              inputSpec = Map(
                  "dataset" -> KijiInputSpec(
                      tableUri.toString,
                      dataRequest = request,
                      fieldBindings = Seq(
                          FieldBinding(tupleFieldName = "attributes",
                              storeFieldName = "family:column1"),
                          FieldBinding(tupleFieldName = "target",
                              storeFieldName = "family:column2"))
                  ),
                  "parameters" -> TextSourceSpec(
                      path = paramsFile
                  )
              ),
              outputSpec = Map(
                  "parameters" -> TextSourceSpec(
                      path = outputParams
              )),
              keyValueStoreSpecs = Seq()
          ))
      )

      // Build the produce job.
      val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment)

      // Hack to set the mode correctly. Scalding sets the mode in JobTest
      // which creates a problem for running the prepare/train phases, which run
      // their own jobs. This makes the test below run in HadoopTest mode instead
      // of Hadoop mode whenever it is run after another test that uses JobTest.
      // Remove this after the bug in Scalding is fixed.
      com.twitter.scalding.Mode.mode = Hdfs(false, HBaseConfiguration.create())

      // Verify that everything went as expected.
      assert(modelExecutor.runTrainer())
    }
    kiji.release()
    val lines = scala.io.Source.fromFile(outputParams + "/part-00000").mkString
    // Theta values after a single iteration.
    assert(lines.split("""\s+""").map(_.toDouble).deep === Array(0.0, 0.75, 1.0, 1.25).deep)
    FileUtils.deleteDirectory(new File(outputParams))
  }
}
