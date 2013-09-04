package org.kiji.express.tool

import org.kiji.express.flow._
import com.twitter.scalding.{Tsv, Args}
import org.kiji.express.{AvroList, AvroRecord, KijiSlice}



class NetflixExporter(args: Args) extends KijiJob(args){
  KijiInput(args("table-uri"))(Map(
    Column("info:movie_name", latest) -> 'movieName,
    Column("info:recommendations", latest) -> 'recommendations,
    Column("info:movie_id", latest) -> 'movieId
  ))
    .mapTo(('movieName, 'recommendations, 'movieId) -> 'recommendations){
    movieRecoTuple: (KijiSlice[String], KijiSlice[AvroRecord], KijiSlice[String]) =>
      movieRecoTuple._3.getFirstValue() + "|" +
      movieRecoTuple._1.getFirstValue() + "|" +
      "\"" +
      movieRecoTuple._2.getFirstValue()("similar_movie_ids").asInstanceOf[AvroList].asList()
        .map(av => av.asString().replaceAll("\"", "\"\"")).mkString("|") +
      "\""

  }
  .write(Tsv(args("output-file")))
}
