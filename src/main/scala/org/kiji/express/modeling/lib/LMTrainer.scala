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

final case class LMTrainer() extends Trainer {
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

  def errorDistance(param1: IndexedSeq[Double], param2: IndexedSeq[Double]): Double = {
    math.sqrt(param1.zip(param2).map {
      case (t1: Double, t2: Double) => math.pow(t1 - t2, 2)
    }
    .reduce(_ + _))
  }

  /**
   *
   * @param thetas is the source containing the currently calculated parameters for linear
   *    regression.
   * @return an ordered sequence of doubles representing the thetas.
   */
  def vectorizeParameters(thetas: TextLine): IndexedSeq[Double] = {
    thetas.readAtSubmitter[(Long, String)]
        .map((entry: (Long, String)) => {
          val (lineNum, line) = entry
          val lineComponents = line.split("\\s+")
          (lineComponents(0).toInt, lineComponents(1).toDouble)
        })
        .sortWith((a,b) => a._1.compareTo(b._1) < 0)
        .map(paramTuple => paramTuple._2)
        .toIndexedSeq
  }

  class LMJob (input: Map[String, Source], output: Map[String, Source],
      val parameters:IndexedSeq[Double], numFeatures:Int = 1, val learningRate: Double = 0.25)
      extends TrainerJob {
    input.getOrElse("dataset", null)
        .mapTo(('attributes, 'target) -> 'gradient) {
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
    var parameterSource:TextLine = input.getOrElse("parameters", null).asInstanceOf[TextLine]
    var outputSource: TextLine = output.getOrElse("parameters", null).asInstanceOf[TextLine]
    // TODO EXP-203 When EXP-203 is completed, change this to an appropriate default.
    val max_iter = 1
    val epsilon = 0.001
    var previousParms: IndexedSeq[Double] = null
    var dist: Double = Double.MaxValue
    var index = 0
    while (index < max_iter && dist > epsilon) {
      val parameters:IndexedSeq[Double] = vectorizeParameters(parameterSource)
      if (previousParms != null) {
        dist = errorDistance(previousParms, parameters)
      }
      previousParms = parameters
      new LMJob(input, output, parameters).run
      parameterSource = outputSource
    }
    // TODO report - number of iterations, error, etc.
    true
  }
}
