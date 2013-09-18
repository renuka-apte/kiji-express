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

import java.io.File

import org.apache.commons.io.FileUtils

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.KijiSuite
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.util.InstanceBuilder

/**
 * Test out LMTool.
 */
@ApiAudience.Public
@ApiStability.Experimental
class LMToolSuite extends KijiSuite {
  test("LMTool works properly") {
    val inputTable: String = "two-double-columns.json"
    val paramsFile: String = "src/test/resources/sources/LRparams"
    val outputParams: String = "src/test/resources/sources/LROutput"
    val attrCol: String = "family:column1"
    val targetCol: String = "family:column2"

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

    val tableUri: KijiURI = doAndRelease(kiji.openTable("lr_table")) {
      table: KijiTable => table.getURI()
    }

    LMTool.main(Array("--dataset", tableUri.toString,
        "--parameters", paramsFile,
        "--attribute-column", attrCol,
        "--target-column", targetCol,
        "--output", outputParams,
        "--hdfs"))

    kiji.release()
    val lines = scala.io.Source.fromFile(outputParams + "/part-00000").mkString
    // Theta values after a single iteration.
    assert(lines.split("""\s+""").map(_.toDouble).deep === Array(0.0, 0.75, 1.0, 1.25).deep)
    FileUtils.deleteDirectory(new File(outputParams))
  }
}
