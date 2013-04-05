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
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory

/**
 * This is the Chopsticks representation of a Kiji EntityId.
 *
 * Users can create EntityIds either by passing in the objects that compose it.
 * For example, if a Kiji table uses formatted row keys composed of a string as
 * their first component and a long as the second, the user can create this as:
 * EntityId("myString", 1L)
 *
 * Users can retrieve the index'th element of an EntityId (0-based), as follows:
 * EntityId(index)
 *
 * @param components is a variable number of objects that compose this EntityId.
 */
final case class EntityId private(factory: Option[EntityIdFactory],
    hbaseEntityId: Option[Array[Byte]], components: Any*){
  override def equals(obj: Any): Boolean = {
    obj match {
      case eid: EntityId => {
        if ((components == eid.components && components != null)
            || (hbaseEntityId == eid.hbaseEntityId && hbaseEntityId != None)) {
          true
        } else {
          // make sure we are not dealing with the case where we have a HashedEntityId
          // created by a user being compared to its corresponding representation as read
          // from a Kiji table.
          if ((hbaseEntityId != None) && (eid.hbaseEntityId == None)) {
            factory match {
              case Some(thisFactory) =>
                  thisFactory.getEntityId(
                      eid.components.map( comp => KijiScheme.convertScalaTypes(comp, null)).asJava
                  )
                  .getHBaseRowKey.deep == hbaseEntityId.get.deep
              case None => false
            }
          } else if ((hbaseEntityId == None) && (eid.hbaseEntityId != None)) {
            eid.factory match {
              case Some(eidFactory) =>
                  eidFactory.getEntityId(
                  components.map( comp => KijiScheme.convertScalaTypes(comp, null)).asJava
                  )
                  .getHBaseRowKey.deep == eid.hbaseEntityId.get.deep
              case None => false
            }
          } else {
            false
          }
        }
      }
      case _ => false
    }
  }

  /**
   * Get the kiji schema class [[org.kiji.schema.EntityId]] for the given
   * components.
   * @param factory is the [[org.kiji.schema.EntityIdFactory]] for the row keys in this
   *                Kiji table.
   * @return the [[org.kiji.schema.EntityId]] using the components for the Kiji table.
   */
  private[chopsticks] def getEntityId(factory: EntityIdFactory): JEntityId = {
    hbaseEntityId match {
      case Some(hbaseKey) => factory.getEntityIdFromHBaseRowKey(hbaseKey)
      case None => {
        factory.getEntityId(components.map( comp =>
            KijiScheme.convertScalaTypes(comp, null)).asJava)
      }
    }
  }

  /**
   * Get the index'th component of the EntityId.
   *
   * @param index is the 0 based index of the component to retrieve.
   * @return the component at index.
   */
  def apply(index: Int): Any = {
    require(null != components, "Components for this entity Id were not materialized. This may be"
      + "because you have suppressed materialization or used Hashed Entity Ids")
    components(index)
  }
}

object EntityId{
  /**
   * Create a Chopsticks EntityId given a Kiji EntityId and factory.
   * We require the factory in the case where we created one HashedEntityId by reading from
   * a Kiji table and another from its component. We now have no way of comparing the two
   * except by converting the one with the component to hbase. But since the user would not
   * have a factory lying around, we need to store it.
   *
   * @param entityId is the [[org.kiji.schema.EntityId]].
   * @param factory is the factory which was used to create the entityId.
   * @return the Chopsticks representation of this EntityId.
   */
  private [chopsticks] def apply(entityId: JEntityId, factory: EntityIdFactory): EntityId = {
    try {
      EntityId(Option(factory), Some(entityId.getHBaseRowKey), entityId.getComponents.asScala:_*)
    } catch {
      case exc: java.lang.IllegalStateException => {
        EntityId(Option(factory), Some(entityId.getHBaseRowKey), null)
      }
    }
  }

  /**
   * Create a Chopsticks EntityId from the given components.
   * @param components is a variable list of objects representing the EntityId.
   * @return a Chopsticks EntityId.
   */
  def apply(components: Any*): EntityId = {
    if (components.size == 1 && components(0).isInstanceOf[JEntityId]) {
      throw new RuntimeException("You need to provide the EntityId factory in order to create "
        + "a Chopsticks EntityId from a Scala EntityId")
    }
    EntityId(None, None, components:_*)
  }
}
