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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.kiji.express.{KijiSlice, KijiSuite}
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.{KijiClientTest, KijiURI, KijiTable, Kiji}
import org.kiji.schema.util.InstanceBuilder
import com.twitter.scalding.{TextLine, Args}
import org.kiji.express.flow.{KijiInput, KijiJob}
import org.kiji.express.util.Resources._
import org.kiji.schema.layout.KijiTableLayouts
import java.io.File
import org.apache.commons.io.FileUtils

@RunWith(classOf[JUnitRunner])
class RecommendationPipeSuite extends KijiSuite {
  val testLayoutDesc: TableLayoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE)
  testLayoutDesc.setName("OrdersTable")

  val kiji: Kiji = new InstanceBuilder("default")
      .withTable(testLayoutDesc)
      .withRow("row1")
      .withFamily("family")
      .withQualifier("column").withValue("milk, bread, coke")
      .withRow("row2")
      .withFamily("family")
      .withQualifier("column").withValue("milk, bread, butter, flour")
      .withRow("row3")
      .withFamily("family")
      .withQualifier("column").withValue("butter, flour")
      .build()

  val inputUri: KijiURI = doAndRelease(kiji.openTable("OrdersTable")) {
    table: KijiTable => table.getURI
  }

  test("Prepare Itemsets Job runs properly") {
    class PrepareItemsetsJob(args: Args) extends KijiJob(args) {
      KijiInput(args("input"), "family:column" -> 'slice)
        .map('slice -> 'order) {
        slice: KijiSlice[String] => slice.getFirstValue().split(",").map(_.trim).toList
      }
      .prepareItemSets[String]('order -> 'itemset, 2, 2)
      .write(TextLine(args("output")))
    }
    val outputFile: File = File.createTempFile("/tmp", ".txt")
    new PrepareItemsetsJob(Args("--input " + inputUri.toString + " --output " +
        outputFile.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputFile.getAbsolutePath)) {
      source: scala.io.Source => source.mkString
    }
    val expectedOutput = Array(
        "butter,flour",
        "milk,bread",
        "milk,butter",
        "milk,flour",
        "bread,butter",
        "bread,flour",
        "butter,flour",
        "milk,bread",
        "milk,coke",
        "bread,coke"
    )
    assert(expectedOutput.sameElements(lines.split("\n")))
    FileUtils.deleteQuietly(outputFile)
  }
}
