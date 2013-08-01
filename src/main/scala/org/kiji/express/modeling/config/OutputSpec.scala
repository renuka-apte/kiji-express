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

package org.kiji.express.modeling.config

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.avro.AvroKijiOutputSpec
import org.kiji.express.avro.AvroOutputSpec

/**
 * Represents the configuration for an output data source.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait OutputSpec {
  /**
   * Creates an Avro OutputSpec from this specification.
   *
   * @return an Avro OutputSpec from this specification.
   */
  private[express] def toAvroOutputSpec(): AvroOutputSpec
}

/**
 * Configuration necessary to use a Kiji table as a data sink.
 *
 * @param tableUri addressing the Kiji table that this output spec will write from.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how output fields are mapped onto columns in a Kiji table.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class KijiOutputSpec(
    tableUri: String,
    fieldBindings: Seq[FieldBinding]) extends OutputSpec{
  private[express] override def toAvroOutputSpec(): AvroOutputSpec = {
    val avroKijiOutputSpec = AvroKijiOutputSpec
        .newBuilder()
        .setTableUri(tableUri)
        .setFieldBindings(fieldBindings.map { _.toAvroFieldBinding } .asJava)
        .build()

    AvroOutputSpec
        .newBuilder()
        .setSpecType("KIJI")
        .setConfiguration(avroKijiOutputSpec)
        .build()
  }
}

/**
 * The companion object to KijiOutputSpec for factory methods.
 */
object KijiOutputSpec {
  /**
   * Converts an Avro KijiOutputSpec specification into a KijiOutputSpec case class.
   *
   * @param avroKijiOutputSpec is the Avro specification.
   * @return the avro KijiOutputSpec specification as a KijiOutputSpec case class.
   */
  private[express] def apply(avroKijiOutputSpec: AvroKijiOutputSpec): KijiOutputSpec = {
    KijiOutputSpec(
        tableUri = avroKijiOutputSpec.getTableUri,
        fieldBindings = avroKijiOutputSpec.getFieldBindings.asScala.map { FieldBinding(_) })
  }
}
