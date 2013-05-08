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

import scala.io.Source

import org.kiji.schema.util.FromJson
import org.kiji.schema.util.ToJson

case class ModelPipeline private(private val avroModelPipeline: AvroModelPipeline) {
  def toJson(): String = {
    ToJson.toJsonString(avroModelPipeline)
  }
}

object ModelPipeline {
  def isValidModelPipeline(avroModelPipeline: AvroModelPipeline): Boolean = {
    return (!avroModelPipeline.getModelPipelineName.isEmpty
        && avroModelPipeline.getModelPipelineVersion.matches("[0-9]+(.[0-9]+)*")
        && avroModelPipeline.getProtocolVersion.matches("model-pipeline-0.1.0")
        && Class.forName(avroModelPipeline.getExtractClass).isInstanceOf[Extractor]
        && Class.forName(avroModelPipeline.getScoreClass).isInstanceOf[Scorer[Any]])
  }

  def apply(jsonString: String): ModelPipeline = {
    val avroModelPipeline: AvroModelPipeline = FromJson.fromJsonString(
      jsonString,
      AvroModelPipeline.SCHEMA$)
      .asInstanceOf[AvroModelPipeline]
    require(isValidModelPipeline(avroModelPipeline))
    ModelPipeline(avroModelPipeline)
  }

  def apply(jsonSource: Source): ModelPipeline = {
    ModelPipeline(jsonSource.mkString)
  }
}
