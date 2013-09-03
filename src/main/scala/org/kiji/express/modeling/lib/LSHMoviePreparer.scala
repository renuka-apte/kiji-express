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

package org.kiji.express.modeling.lib

import org.kiji.express.modeling.Preparer
import com.twitter.scalding.Source
import org.kiji.express.{AvroRecord, EntityId, KijiSlice}
import com.twitter.algebird.{MinHashSignature, MinHasher32, MinHasher}
import com.redeyetechguy.kiji.Rating
import scala.collection.JavaConverters._


class LSHMoviePreparer extends Preparer {
  private def getUsers(slice: KijiSlice[AvroRecord]): Set[Long] = {
    slice.cells.map(cell => cell.datum("user_id").asString().toLong).toSet
  }

  class LSHMovieJob[T, H](input: Source, output: Source, mh : MinHasher[H]) extends PreparerJob {
    input
        .map('movieVector -> 'movieBuckets) { movieVector: KijiSlice[AvroRecord] =>
          val users: Set[Long] = getUsers(movieVector)
          val signature = users.map{user => mh.init(user)}.reduce{
            (a, b) => mh.plus(a,b)
          }
          mh.buckets(signature).mkString(",")
        }
        .write(output)
  }

  override def prepare(input: Source, output: Source): Boolean = {
    val mh: MinHasher32 = new MinHasher32(0.6, 512)
    new LSHMovieJob[Long, Int](input, output, mh).run
    true
  }
}
