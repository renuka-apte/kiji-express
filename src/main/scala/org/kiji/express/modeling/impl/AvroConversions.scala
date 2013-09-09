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

package org.kiji.express.modeling.impl

import org.apache.avro.specific.SpecificRecord

/**
 * Provides a `toAvro` method that converts a class into its Avro record representation. This trait
 * is intended to be used with [[org.kiji.express.modeling.impl.FromAvroConversions]].
 *
 * @tparam A is the type of the Avro record to return.
 */
private[express] trait ToAvroConversions[A <: SpecificRecord] {
  /**
   * Converts this record to its Avro record representation.
   *
   * @return an Avro record representing this record.
   */
  private[express] def toAvro: A
}

/**
 * Provides a `fromAvro` method that converts an Avro record into an instance of the provided type.
 * This trait is intended to be used with [[org.kiji.express.modeling.impl.ToAvroConversions]].
 *
 * @tparam A is the type of the Avro record to convert.
 * @tparam T is the type to convert to.
 */
private[express] trait FromAvroConversions[A <: SpecificRecord, T] {
  /**
   * Converts the provided Avro record to an instance of the provided record type. This method
   * provides the type signature for a factory method.
   *
   * @param record to convert.
   * @return an instance of the provided type.
   */
  private[express] def fromAvro(record: A): T
}
