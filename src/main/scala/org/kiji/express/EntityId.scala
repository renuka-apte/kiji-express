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

import java.lang.IllegalStateException
import scala.collection.JavaConverters._

import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.KijiURI

/**
 * This is the Express representation of a Kiji EntityId.
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
      hbaseEntityId: Array[Byte],
      components: Any*){
  /**
   * Define when two EntityIds are equal.
   *
   * @param obj is the Object to compare this EntityId with.
   * @return whether the two are equal.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case eid: EntityId => {
        if (hbaseEntityId.deep == eid.hbaseEntityId.deep) {
          true
        } else {
          false
        }
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

  /**
   * Get the Java EntityId associated with this Scala EntityId.
   *
   * @return the Java EntityId.
   */
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
   * Create an Express EntityId given a Kiji EntityId and tableUri.
   * We need to be explicit about the table associated with this EntityId because we
   * need to eliminate the ambiguity between comparing hashed entity Ids or comparing
   * an entity id for one row key format with another, for the case when they have
   * the same components.
   *
   * @param entityId is the [[org.kiji.schema.EntityId]].
   * @return the Express representation of the Java EntityId.
   */
  private[express] def apply(tableUri: KijiURI, entityId: JEntityId): EntityId = {
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
   * Create an Express EntityId from the given components.
   *
   * @param components is a variable list of objects representing the EntityId.
   * @return an Express EntityId.
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

  /**
   * Create an Express EntityId from the given components.
   *
   * @param tableUriString is a string representation of a KijiURI for the table.
   * @param components is a variable list of objects representing the EntityId.
   * @return an Express EntityId.
   */
  def apply(tableUriString: String)(components: Any*): EntityId = {
    val tableUri = KijiURI.newBuilder(tableUriString).build()
    EntityId(tableUri)(components:_*)
  }
}
