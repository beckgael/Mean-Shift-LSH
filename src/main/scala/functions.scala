/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author Beck Gaël
 */

package msLsh

import scala.util.Random
import scala.util.Sorting.quickSort
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{min, max}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.storage.StorageLevel
import java.io._
import spire.implicits._

object Fcts extends Serializable {

  /**
   * Create a tab with random vector where component are taken on normal law N(0,1) for LSH
   */
  val tabHash = (nb:Int, dim:Int) =>
  {
    val tabHash0 = new Array[Array[Double]](nb)
    for( ind <- 0 until nb) {
      val vechash1 = new Array[Double](dim)
      for( ind2 <- 0 until dim) vechash1(ind2) = Random.nextGaussian
      tabHash0(ind) = vechash1
    }
    tabHash0
  }

  /**
   *  Generate the hash value for a given vector x depending on w, b, tabHash1
   */
  val hashfunc = (x:Vector, w:Double, b:Double, tabHash1:Array[Array[Double]]) =>
  {
    val tabHash = new Array[Double](tabHash1.size)
    for( ind <- tabHash1.indices) {
      var sum = 0.0
      for( ind2 <- 0 until x.size ) {
        sum += ( x(ind2) * tabHash1(ind)(ind2) )
        }     
      tabHash(ind) = (sum + b) / w
    }
    tabHash.reduce(_ + _)
  }

  /**
   * Function which compute centroïds
   */
  val computeCentroid = (tab:Array[(Vector, Double)], k:Int) =>
  {
    val vectors = tab.map(_._1.toArray)
    val vectorsReduct = vectors.reduce(_ + _)
    val centroid = vectorsReduct.map(_ / k)
    Vectors.dense(centroid)
  }
  
  /**
   * Scale data to they fit on range [0,1]
   * Return a tuple where :
   *   First element is the scale rdd
   *   Second element is the array of max value for each component
   *   Third element is the array of min value for each component
   * Theses array are used in order to descale RDD
   */
  def scaleRdd(rdd1:RDD[(Long, Vector)]) : (RDD[(Long, Vector)], Array[Double], Array[Double]) = {
    rdd1.cache
    val vecttest = rdd1.first._2
    val size1 = vecttest.size

    val minMaxArray = rdd1.map{ case (id, vector) => vector.toArray.map(value => (value, value))}.reduce( (v1, v2) => v1.zip(v2).map{ case (((min1, max1), (min2, max2))) => (min(min1, min2), max(max1, max2))})

    val minArray = minMaxArray.map{ case ((min, max)) => min }
    val maxArray = minMaxArray.map{ case ((min, max)) => max }

    val rdd2 = rdd1.map{ case (id, vector) => {
      val tabcoord = new Array[Double](size1)
      for( ind <- 0 until size1) {
        val coordXi = ( vector(ind) - minArray(ind) ) / (maxArray(ind) - minArray(ind))
        tabcoord(ind) = coordXi
      }
      (id, Vectors.dense(tabcoord))
    }}
    (rdd2, maxArray, minArray)
  }

  /**
   * Restore centroid's original value
   */
  def descaleRDDcentroid(rdd1:RDD[(Int, Vector)], maxMinArray:(Array[Double], Array[Double])) : RDD[(Int, Vector)] = {
    val vecttest = rdd1.first._2
    val size1 = vecttest.size
    val maxArray = maxMinArray._1
    val minArray = maxMinArray._2
    val rdd2 = rdd1.map{ case (label, vector) => {
      val tabcoord = new Array[Double](size1)
      for( ind <- 0 until size1) {
        val coordXi = vector(ind) * ( maxArray(ind) - minArray(ind) ) + minArray(ind)
        tabcoord(ind) = coordXi
      }
      (label, Vectors.dense(tabcoord))         
    }}
    rdd2
  }	

}