package org.kiji.express.modeling.config

import scala.tools.nsc.io.Path
import org.scalatest.FunSuite
import net.liftweb.json._
import org.kiji.express.util.Resources._
import scala.Some

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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class JsonWriterSuite extends FunSuite {
  /*val validDefinitionLocation: String =
    "src/test/resources/modelEnvironments/valid-model-environment.json"

  val filter: ExpressColumnFilter = new RegexQualifierFilter("foo")
  val dataRequest: ExpressDataRequest = new ExpressDataRequest(0, 38475687,
    new ExpressColumnRequest("info:in", 3, Some(filter)) :: Nil)

  val kvstores: Seq[KVStore] = Seq(
    KVStore(
      name="side_data",
      storeType="AVRO_KV",
      properties=Map("path" -> "/usr/src/and/so/on")
    )
  )

  val scoreOutputSpec = KijiSingleColumnOutputSpec("kiji://myuri", "info:out")

  val outputSpec = KijiOutputSpec("kiji://myuri",
      Seq(FieldBinding("outputtuple", "info:out"))
  )

  val inputSpec = KijiInputSpec(
      "kiji://.env/default/table",
      dataRequest,
    Seq(FieldBinding("tuplename", "info:storefieldname"))
  )

  val prepareEnvironment = PrepareEnvironment(
      inputSpec,
      outputSpec,
      kvstores)

  val trainEnvironment = TrainEnvironment(
      inputSpec,
      outputSpec,
      kvstores
  )

  val scoreEnvironment = ScoreEnvironment(
      inputSpec,
      scoreOutputSpec,
      kvstores
  )

  val modelEnvironment = ModelEnvironment(
      name = "myRunProfile",
      version = "2.0.0",
      prepareEnvironment = Some(prepareEnvironment),
      trainEnvironment = Some(trainEnvironment),
      scoreEnvironment = scoreEnvironment
  )

  test("Write classes to file") {
    val parsedJSON = parse(modelEnvironment.toJson())
    val jsonString: String = Printer.pretty(render(parsedJSON))
    Path(validDefinitionLocation).toFile.writeAll(jsonString)

  } */

  val validDefinitionLocation: String =
      "src/test/resources/modelDefinitions/valid-model-definition.json"

  val modelDefinition = ModelDefinition(
    name = "name",
    version = "1.0.0",
    prepareExtractor = Some(classOf[ModelDefinitionSuite.MyExtractor]),
    preparer = Some(classOf[ModelDefinitionSuite.MyPreparer]),
    trainer = Some(classOf[ModelDefinitionSuite.MyTrainer]),
    scorer = classOf[ModelDefinitionSuite.MyScorer]
  )

  test("Write valid model definition") {
    val parsedJSON = parse(modelDefinition.toJson())
    val jsonString: String = Printer.pretty(render(parsedJSON))
    Path(validDefinitionLocation).toFile.writeAll(jsonString)
  }

}
