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

package org.kiji.chopsticks

import scala.collection.JavaConverters._

import org.scalatest.FunSuite
import org.kiji.schema.avro._
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory


class EntityIdSuite extends FunSuite {
  private def makeRowKeyFormat: RowKeyFormat2 = {
    val rowKeyComponents = List(
        RowKeyComponent.newBuilder.setName("astring").setType(ComponentType.STRING).build,
        RowKeyComponent.newBuilder.setName("anint").setType(ComponentType.INTEGER).build,
        RowKeyComponent.newBuilder.setName("along").setType(ComponentType.LONG).build)

    val format: RowKeyFormat2 = RowKeyFormat2.newBuilder
        .setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder.build)
        .setComponents(rowKeyComponents.asJava)
        .build
    format
  }

  private def makeHashedRowKeyFormat: RowKeyFormat2 = {
    val rowKeyComponents = List(
      RowKeyComponent.newBuilder.setName("astring").setType(ComponentType.STRING).build)
    val format: RowKeyFormat2 = RowKeyFormat2.newBuilder
      .setEncoding(RowKeyEncoding.FORMATTED)
      .setSalt(HashSpec.newBuilder
          .setSuppressKeyMaterialization(true)
          .setHashSize(rowKeyComponents.size)
          .build)
      .setComponents(rowKeyComponents.asJava)
      .build

    format
  }

  test("Create an EntityId from EntityIdContainer and vice versa") {
    val factory = EntityIdFactory.getFactory(makeRowKeyFormat)
    val eid = EntityId("test", 1, 7L)
    val entityId = eid.getEntityId(factory)
    val expected: java.util.List[java.lang.Object] = List[java.lang.Object](
        "test",
        new java.lang.Integer(1),
        new java.lang.Long(7))
        .asJava
    assert(expected == entityId.getComponents)

    val recreate = EntityId(entityId, factory)
    assert(eid == recreate)
    assert(recreate(0) == "test")
  }

  test("Test equality between EntityIds") {
    val factory = EntityIdFactory.getFactory(makeHashedRowKeyFormat)
    val eid1 = EntityId("test")
    val eid2 = EntityId("test")
    assert(eid1 == eid2)
    // get the Java EntityId
    val jEntityId: JEntityId = eid1.getEntityId(factory)

    // this is how it would look if it were read from a table
    val tableEid = EntityId(jEntityId, factory)

    // ensure equals works both ways
    assert(tableEid == eid1)
    assert(eid1 == tableEid)

    val otherEid = EntityId("other")

    assert(otherEid != eid1)
    assert(otherEid != tableEid)

    val tableEid2 = EntityId(eid2.getEntityId(factory), factory)
    assert(tableEid == tableEid2)
  }
}
