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

import org.kiji.express.modeling.{KeyValueStore, Trainer}
import com.twitter.scalding.{TupleConversions, Tsv, TextLine, Source}
import org.kiji.express.KijiSlice
import collection.mutable.ArrayBuffer
import org.kiji.express.flow.KijiSource

class LMTrainer(numFeatures:Int, learningRate: Double, epsilon: Double, maxIterations: Int)
    extends Trainer {
  /**
   *
   * @param attr
   * @return
   */
  def vectorizeDataPoint(attr: KijiSlice[Double]): IndexedSeq[Double] = {
    // TODO Decide how the input source will look like for data with more than one attribute.
    val attributes  = new ArrayBuffer[Double]
    attributes.append(1.0, attr.getFirstValue())
    attributes.toIndexedSeq
  }

  /**
   *
   * @param thetas is the source containing the currently calculated parameters for linear
   *    regression.
   * @return an ordered sequence of doubles representing the thetas.
   */
  def vectorizeParameters(thetas: Source): IndexedSeq[Double] = {
    thetas.readAtSubmitter[String]
        .map((line: String) => {
          val lineComponents = line.split("\t")
          (lineComponents(0).toInt, lineComponents(1).toDouble)
        })
        .sortWith((a,b) => a._1.compareTo(b._1) < 0)
        .map(paramTuple => paramTuple._2)
        .toIndexedSeq
  }

  class LMJob (input: Map[String, Source], output: Map[String, Source],
      parameters:IndexedSeq[Double]) extends TrainerJob {
    val datasetSource:KijiSource = input.getOrElse("dataset", null).asInstanceOf[KijiSource]

    datasetSource.mapTo(('attributes, 'target) -> 'gradient) {
      dataPoint: (KijiSlice[Double], KijiSlice[Double]) => {
        // X
        val attributes: IndexedSeq[Double] = vectorizeDataPoint(dataPoint._1)
        // y
        val target: Double = dataPoint._2.getFirstValue()
        // y - (theta)'(X)
        val delta: Double = target - parameters.zip(attributes).map(x => x._1 * x._2).reduce(_+_)
        // TODO: May need to convert this into a tuple (attributes(0), attributes(1),......)
        attributes.map(x => x * delta)
      }
    }
    // index thetas by position
    .flatMapTo('gradient -> ('index, 'indexedGradient)) {
      gradient: IndexedSeq[Double] => gradient.zipWithIndex.map{ x =>(x._2, x._1) }
    }
    // calculating new thetas
    .groupBy('index) {
      _.reduce('indexedGradient -> 'totalIndexedGradient) {
        (gradientSoFar : Double, gradient : Double) => gradientSoFar + gradient
      }
    }
    // update old theta depending on learning rate and convert thetas to a string
    .mapTo(('index, 'totalIndexedGradient) -> 'indexedGradientString) {
      gradientTuple: (Int, Double) => {
       gradientTuple._1.toString  + "\t" + (parameters(gradientTuple._1) +
          (learningRate * gradientTuple._2)).toString
      }
    }
    .write(output.getOrElse("parameters", null))
  }

  /*
  def converged(oldParams: IndexedSeq[Double], newParams: IndexedSeq[Double]): Boolean = {
    oldParams.zip(newParams)
      .map(params => math.abs(params._1 - params._2))
      .reduce(_+_)/(numFeatures + 1) < epsilon
  }
  */

  override def train(input: Map[String, Source], output: Map[String, Source]): Boolean = {
    // Read the parameters (thetas) from a text file on hdfs and convert them into a sequence
    // of doubles that can be consumed by a job.
    val parameterSource:TextLine = input.getOrElse("parameters", null).asInstanceOf[TextLine]
    val parameters:IndexedSeq[Double] = vectorizeParameters(parameterSource)

    new LMJob(input, output, parameters).run

    true
  }
}