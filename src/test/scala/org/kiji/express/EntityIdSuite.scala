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

import scala.collection.JavaConverters._

import org.scalatest.FunSuite
import org.kiji.schema.avro._
import org.kiji.schema.{EntityId => JEntityId, KijiURI, KijiTable, EntityIdFactory}
import org.kiji.schema.layout.{KijiTableLayouts, KijiTableLayout}
import org.kiji.express.Resources._


class EntityIdSuite extends KijiSuite {
  /** Table layout to use for tests. */
  val tableLayoutFormatted = KijiTableLayout.newLayout(
    KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF))
  // Create a table to use for testing
  val tableUriFormatted: KijiURI = doAndRelease(makeTestKijiTable(tableLayoutFormatted)) {
    table: KijiTable =>
    table.getURI
  }

  val tableLayoutHashed = KijiTableLayout.newLayout(
    KijiTableLayouts.getLayout(KijiTableLayouts.HASHED_FORMATTED_RKF))
  // Create a table to use for testing
  val tableUriHashed: KijiURI = doAndRelease(makeTestKijiTable(tableLayoutHashed)) {
    table: KijiTable =>
      table.getURI
  }

  test("Create an EntityId from EntityIdContainer and vice versa") {
    val eid = EntityId(tableUriFormatted)("test", "1", "2", 1, 7L)
    val entityId = eid.getEntityId()
    val expected: java.util.List[java.lang.Object] = List[java.lang.Object](
        "test",
        "1",
        "2",
        new java.lang.Integer(1),
        new java.lang.Long(7))
        .asJava
    assert(expected == entityId.getComponents)

    val recreate = EntityId(tableUriFormatted, entityId)
    assert(eid == recreate)
    assert(recreate(0) == "test")
  }

  test("Test equality between EntityIds") {
    val eid1: EntityId = EntityId(tableUriHashed)("test")
    val eid2: EntityId = EntityId(tableUriHashed)("test")
    assert(eid1 == eid2)
    // get the Java EntityId
    val jEntityId: JEntityId = eid1.getEntityId()

    // this is how it would look if it were read from a table
    val tableEid = EntityId(tableUriHashed, jEntityId)

    // ensure equals works both ways
    assert(tableEid == eid1)
    assert(eid1 == tableEid)

    val otherEid = EntityId(tableUriHashed)("other")

    assert(otherEid != eid1)
    assert(otherEid != tableEid)

    val tableEid2 = EntityId(tableUriHashed, eid2.getEntityId())
    assert(tableEid == tableEid2)
  }
}
