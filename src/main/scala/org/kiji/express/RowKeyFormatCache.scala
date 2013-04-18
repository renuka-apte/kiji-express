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

import org.kiji.schema.{KijiTable, Kiji, EntityIdFactory, KijiURI}
import scala.collection.mutable
import org.kiji.express.Resources._

object RowKeyFormatCache {
  val rowKeyFormatCache = new mutable.HashMap[KijiURI, EntityIdFactory]()

  def getFactory(tableUriString: String) : EntityIdFactory = {
    val inputTableURI: KijiURI = KijiURI.newBuilder(tableUriString).build()
    getFactory(inputTableURI)
  }

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
      rowKeyFormatCache(tableUri) = EntityIdFactory.getFactory(tableLayout)
      rowKeyFormatCache(tableUri)
    }
  }
}
