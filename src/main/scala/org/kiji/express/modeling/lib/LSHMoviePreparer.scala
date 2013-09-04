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
import com.twitter.algebird.{MinHasher32, MinHasher}



class LSHMoviePreparer extends Preparer {
  private def createMovieFeatureVectorFromRatings(slice: KijiSlice[AvroRecord]): Set[Long] = {
    slice.cells.filter(cell => cell.datum("rating").asInt() >= 3)
      .map(cell => cell.datum("user_id").asString().toLong)
      .toSet
  }

  class LSHMovieJob[T, H](input: Source, output: Source, mh : MinHasher[H]) extends PreparerJob {
    input
        .flatMapTo(('entityId, 'movieName, 'movieRatings) -> ('bucketId, 'movieName, 'entityId))
    { movieRatingsTuple : (EntityId, KijiSlice[String], KijiSlice[AvroRecord]) =>
          val entityId = movieRatingsTuple._1
          val movieName = movieRatingsTuple._2.getFirstValue()
          val ratingsVector = movieRatingsTuple._3
          val minHashSignature = createMovieFeatureVectorFromRatings(ratingsVector)
            .map{user => mh.init(user)}
            .reduce{
              (a, b) => mh.plus(a,b)
            }
          mh.buckets(minHashSignature).map{bucket => (bucket, movieName, entityId)}
      }

      .groupBy('bucketId){_.toList[Tuple2[EntityId, String]](('entityId, 'movieName) -> 'movieIds)}

      .flatMapTo(('bucketId, 'movieIds) -> ('movieId, 'similarMovieId)){
      bucketMoviesTuple: (Long, List[Tuple2[EntityId, String]]) =>
         val moviesInBucket = bucketMoviesTuple._2
         moviesInBucket
           .flatMap{ movie => moviesInBucket.filterNot({ _ == movie })
           .map{ similarMovie =>
              (movie._1(0), similarMovie._2)
            }
         }
      }

      .groupBy('movieId){_.toList[String]('similarMovieId -> 'similar_movie_ids)}

      .mapTo(('movieId, 'similar_movie_ids)  -> ('entityId, 'similar_movie_ids)){
      movieRecommendationsTuple: (String, List[String]) =>
        (EntityId(movieRecommendationsTuple._1), movieRecommendationsTuple._2.distinct)
      }

      .packAvro('similar_movie_ids -> 'recommendations)

      .write(output)
  }

  override def prepare(input: Source, output: Source): Boolean = {
    val mh: MinHasher32 = new MinHasher32(0.7, 512)
    new LSHMovieJob[Long, Int](input, output, mh).run
    true
  }
}
