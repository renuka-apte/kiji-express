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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.KijiURI

/**
 * Container for a Kiji row data and Kiji table layout object that is required by a map reduce
 * task while reading from a Kiji table.
 *
 * @param rowContainer is the representation of a Kiji row.
 * @param tableUriString is the URI of the Kiji table.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] case class KijiSourceContext (
    rowContainer: KijiValue,
    tableUriString: String) {
}
