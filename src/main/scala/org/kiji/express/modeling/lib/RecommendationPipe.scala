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

import com.twitter.scalding._
import cascading.pipe.Pipe
import cascading.tuple.Fields
import  org.kiji.express.repl.Implicits._

class RecommendationPipe(val pipe: Pipe) {
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
    pipe
        .flatMapTo(fromFields -> toFields) {
          basket: List[T] => (minSetSize to maxSetSize).flatMap(basket.combinations)
        }
        .map(toFields -> toFields) { itemSets: List[T] => itemSets.mkString(",") }
  }

  /**
   * This function joins every row in the pipe with the size of the group it belongs to. The
   * parameter fromFields determines the criteria for the group. If fromFields is set to ALL,
   * then it will join with the total number of rows.
   *
   * @param fieldSpec a mapping from the fields on which to group to the output field name.
   * @return a pipe containing the groups, along with a field for their size.
   */
  def joinWithGroupCount(fieldSpec : (Fields, Fields)) : RichPipe = {
    val (fromFields, toField) = fieldSpec
    val total = pipe.groupBy(fromFields) { _.size(toField) }
    pipe.crossWithTiny(total)
  }

  /**
   * Filter itemsets (which are N-grams of entities that occur together in some context, e.g.
   * products in an order) by a threshold value called supportThreshold. This is usually used to
   * filter only "important" occurrences. This is typically expressed as a fraction, rather than
   * an absolute value. The denominator for this fraction needs to be provided either as a constant
   * or a pipe and a field within it.
   *
   * @param itemsetField is the field in this pipe that contains the co-occurring N-grams.
   * @param normalizingPipe is a pipe that may contain the normalizing constant. Optional.
   * @param normalizingConstant is a normalizing constant.
   * @param normalizingField is either the name of the field in the normalizing pipe that contains
   *     the normalizing constant or is the name of the field to insert into the pipe if you have
   *     provided a normalizingConstant.
   * @param supportThreshold is the cut-off value for filtering the itemsets of importance.
   * @param numReducers is used if you have more than 1 reducer available to run this function.
   * @return the pipe containing itemsets that satisfy the supportThreshold.
   */
  def filterBySupport(itemsetField: Fields,
      normalizingPipe: Option[Pipe],
      normalizingConstant: Option[Double],
      normalizingField: Fields,
      supportThreshold: Double,
      numReducers: Int = 1): RichPipe = {
    // Exactly one of normalizingConstant and normalizingPipe must be supplied to this function
    require (normalizingConstant.isDefined ^ normalizingPipe.isDefined)
    pipe.groupBy(itemsetField) { _.reducers(numReducers).size('itemSetFrequency) }
    if (normalizingConstant.isDefined) {
      pipe.insert(normalizingField, normalizingConstant.get)
    } else {
      pipe.crossWithTiny(normalizingPipe.get)
    }
    pipe
        .filter('itemSetFrequency, normalizingField) {
          fields: (Long, Long) => {
            val (frequency, normalizer) = fields
            frequency/normalizer > supportThreshold
          }
        }
  }
}