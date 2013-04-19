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

package org.kiji.express

import scala.collection.mutable.HashMap

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.Resources._
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI

/**
 * In order to unambiguously create scala EntityIds, we require the row key format, or in
 * other words, the table corresponding to this EntityId.
 * If we were to open a Kiji connection for each entityId we created in the flow, it would
 * cause a significant performance hit. Instead, we cache the mapping from a table to its
 * EntityIdFactory, once per JVM using a RowKeyFormatCache.
 */
@ApiAudience.Private
@ApiStability.Experimental
object RowKeyFormatCache {
  val rowKeyFormatCache = new HashMap[KijiURI, EntityIdFactory]()

  /**
   *  Get an EntityIdFactory, either from the cache or by opening a Kiji table, if
   *  it is not cached.
   *
   * @param tableUriString is the string representing a [[org.kiji.schema.KijiURI]].
   * @return the EntityIdFactory to generate an EntityId for this table.
   */
  def getFactory(tableUriString: String) : EntityIdFactory = {
    val inputTableURI: KijiURI = KijiURI.newBuilder(tableUriString).build()
    getFactory(inputTableURI)
  }

  /**
   * Get an EntityIdFactory, either from the cache or by opening a Kiji table, if
   * it is not cached.
   *
   * @param tableUri is the unique [[org.kiji.schema.KijiURI]] for the table.
   * @return the EntityIdFactory to generate an EntityId for this table.
   */
  def getFactory(tableUri: KijiURI): EntityIdFactory = {
    if (rowKeyFormatCache.contains(tableUri)) {
      rowKeyFormatCache(tableUri)
    } else {
      val tableLayout =
          doAndRelease(Kiji.Factory.open(tableUri)) { kiji: Kiji =>
            doAndRelease(kiji.openTable(tableUri.getTable())) { table: KijiTable =>
              table.getLayout()
            }
          }
      val factory = EntityIdFactory.getFactory(tableLayout)
      rowKeyFormatCache(tableUri) = factory
      factory
    }
  }
}
