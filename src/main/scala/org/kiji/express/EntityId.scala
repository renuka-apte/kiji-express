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
import org.kiji.schema.{EntityId => JEntityId, KijiURI, EntityIdFactory}
import java.lang.IllegalStateException

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
final case class EntityId private(tableUriString: String,
      hbaseEntityId: Array[Byte], components: Any*){
  override def equals(obj: Any): Boolean = {
    obj match {
      case eid: EntityId => {
        if (hbaseEntityId.deep == eid.hbaseEntityId.deep)
          true
        else
          false
      }
      case _ => false
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

  def getEntityId(): JEntityId = {
    val eidFactory = RowKeyFormatCache.getFactory(KijiURI.newBuilder(tableUriString).build())
    val javaComponents: java.util.List[Object] = components.toList
      .map { elem => KijiScheme.convertScalaTypes(elem, null) }
      .asJava
    eidFactory.getEntityId(javaComponents)
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
   * @return the Chopsticks representation of this EntityId.
   */
  private [express] def apply(tableUri: KijiURI, entityId: JEntityId): EntityId = {
    val components = try {
      entityId.getComponents
        .asScala
        .toList
        .map { elem => KijiScheme.convertJavaTypes(elem) }
    } catch {
      case ise: IllegalStateException => null
    }
    EntityId(tableUri.toString,
        entityId.getHBaseRowKey, components:_*)
  }

  /**
   * Create a Chopsticks EntityId from the given components.
   * @param components is a variable list of objects representing the EntityId.
   * @return a Chopsticks EntityId.
   */
  def apply(tableUri: KijiURI)(components: Any*): EntityId = {
    if ((components.size == 1) && (components.isInstanceOf[JEntityId])) {
      throw new IllegalArgumentException("Trying to create scala EntityId using"
        + " a Java EntityId as component. You probably want to use EntityId(KijiURI, JEntityId) "
        + "for doing this.")
    }
    val jEntityIdFactory = RowKeyFormatCache.getFactory(tableUri)
    val javaComponents: java.util.List[Object] = components.toList
        .map { elem => KijiScheme.convertScalaTypes(elem, null) }
        .asJava
    EntityId(tableUri.toString,
        jEntityIdFactory.getEntityId(javaComponents).getHBaseRowKey,
        components:_*)
  }

}
