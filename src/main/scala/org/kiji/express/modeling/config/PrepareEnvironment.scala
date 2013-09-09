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

package org.kiji.express.modeling.config

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * A specification of the runtime bindings needed in the prepare phase of a model.
 *
 * @param inputSpec defining a mapping from input data source names to their configurations.
 * @param outputSpec defining a mapping from output data sink names to their configurations.
 * @param keyValueStoreSpecs for usage during the prepare phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class PrepareEnvironment(
    inputSpec: Map[String, InputSpec],
    outputSpec: Map[String, OutputSpec],
    keyValueStoreSpecs: Seq[KeyValueStoreSpec])
