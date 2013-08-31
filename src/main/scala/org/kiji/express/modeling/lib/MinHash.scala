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

import com.twitter.scalding.Source
import org.kiji.express.{EntityId, KijiSlice}
import org.kiji.express.modeling.Preparer

/**
 * The MinHash Preparer calculates the MinHash for a
 */
class MinHash extends Preparer {
  class MinHashJob(input: Source, output: Source) extends PreparerJob {
    input
  }
  override def prepare(input: Source, output: Source): Boolean = {
    new MinHashJob(input, output).run
    true
  }
}