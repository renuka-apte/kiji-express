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

import com.twitter.scalding.RichPipe
import cascading.pipe.Pipe
import cascading.tuple.Fields
import org.kiji.express.repl.Implicits.pipeToRichPipe

class RecommendationPipe(private val pipe: Pipe) extends RichPipe(pipe) {
  /**
   * This function takes profile information (e.g. a history of purchases) or order data and outputs
   * smaller subsets of co-occuring items. If the minSetSize and maxSetSize is 2, it will create 
   * tuples of items that are found within the same history/order. This will typically be used to 
   * calculate the numerator (raw count) for Jaccard similarity, or the number of N-grams.
   * Mathematically, if N = number of orders, m = minSetSize and M = maxSetSize, the number of
   * resulting subsets will be = N choose m + N choose (m+1) + N choose (m+2) ... N choose M
   * 
   * @param fieldSpec mapping from the field(s) which represent the order/purchase history to the
   *     field that will hold the resulting N-grams.
   * @param minSetSize is the minimum size of the subset or N-gram. Optional.
   * @param maxSetSize is the maximum size of the subset or N-gram. Optional. Care must be taken
   *     while choosing this value as the number of resulting tuples might be too large to hold in
   *     memory at a single mapper or may take a while to generate.
   * @tparam T is the data type of the element in the order/purchase history.
   * @return a RichPipe with the specified output field which holds the resulting tuples.
   */
  def prepareItemSets[T](fieldSpec: (Fields, Fields),
      minSetSize: Int = 2,
      maxSetSize: Int = 5): RichPipe = {
    val (fromFields, toFields) = fieldSpec
    this
        .flatMapTo(fromFields -> toFields) {
          basket: List[T] => (minSetSize to maxSetSize).flatMap(basket.combinations)
        }
        .map(toFields -> toFields) { itemSets: List[T] => itemSets.mkString(",") }
  }

  def filterBySupport[T](fieldSpec: (Fields, Fields),
      supportThreshold: Double): RichPipe = {

  }
}
