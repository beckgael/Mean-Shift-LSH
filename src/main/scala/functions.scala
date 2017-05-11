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
import scala.collection.mutable.ArrayBuffer
import spire.implicits._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.mllib.util._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.storage.StorageLevel
import org.apache.spark
import java.io._
import scala.math.{min, max}

object Fcts extends Serializable {

  /**
   * Create a tab with random vector where component are taken on normal law N(0,1) for LSH
   */
  def tabHash(nb:Int, dim:Int) = {
    var tabHash0 : Array[Array[Double]] = Array()
    for( ind <- 0 until nb) {
      var vechash1 : Array[Double] = Array()
      for( ind <- 0 until dim) {
        val nG = Random.nextGaussian
        vechash1 = vechash1 :+ nG
      }
      tabHash0 = tabHash0 :+ vechash1
    }
  tabHash0
  }

  def hashfunc(x:Vector, w:Double, b:Double, tabHash1:Array[Array[Double]]) : Double = {
    var tabHash = Array.empty[Double]
    val x1 = x.toArray
    for( ind <- tabHash1.indices) {
      var sum = 0.0
      for( ind2 <- x1.indices) {
        sum = sum + ( x1(ind2) * tabHash1(ind)(ind2) )
        }     
      tabHash =  tabHash :+ (sum+b)/w
    }
    tabHash.reduce(_+_)
  }

  /**
   * Function which compute centroïds
   */
  def bary(tab1:Array[(Vector,Double)],k:Int) : Vector = {
    var tab2 = tab1.map(_._1.toArray)
    var bary1 = tab2.reduce(_+_)
    bary1 = bary1.map(_/k)
    Vectors.dense(bary1)
  }
  
  /**
   * Scale data to they fit on range [0,1]
   * Return a tuple where :
   *   First element is the scale rdd
   *   Second element is the array of max value for each component
   *   Third element is the array of min value for each component
   * Theses array are used in order to descale RDD
   */
  def scaleRdd(rdd1:RDD[(Long,Vector)]) : (RDD[(Long,Vector)],Array[Double],Array[Double]) = {
    rdd1.cache
    val vecttest = rdd1.first()._2
    val size1 = vecttest.size

    val minMaxArray = rdd1.map{ case(idx, vector) => vector.toArray.map(value => (value, value))}.reduce( (v1, v2) => v1.zip(v2).map{ case(((min1,max1),(min2,max2))) => (min(min1, min2), max(max1, max2))})

    val minArray = minMaxArray.map{ case((min, max)) => min}
    val maxArray = minMaxArray.map{ case((min, max)) => max}

    val rdd2 = rdd1.map( x => {
      var tabcoord : Array[Double] = Array()
      for( ind0 <- 0 until size1) {
        val coordXi = (x._2(ind0)-minArray(ind0))/(maxArray(ind0)-minArray(ind0))
        tabcoord = tabcoord :+ coordXi
      }
      (x._1,Vectors.dense(tabcoord))
    })
    (rdd2,maxArray,minArray)
  }

  /**
   * Restore centroid's original value
   */
  def descaleRDDcentroid(rdd1:RDD[(Int, Vector)],maxMinArray0:Array[Array[Double]]) : RDD[(Int, Vector)] = {
    val vecttest = rdd1.first()._2
    val size1 = vecttest.size
    val maxArray = maxMinArray0(0)
    val minArray = maxMinArray0(1)
    val rdd2 = rdd1.map{ case(label, vector) => {
      var tabcoord : Array[Double] = Array()
      for( ind0 <- 0 until size1) {
        val coordXi = vector(ind0)*(maxArray(ind0)-minArray(ind0))+minArray(ind0)
        tabcoord = tabcoord :+ coordXi
      }
      (label, Vectors.dense(tabcoord))         
    }}
    rdd2
  }	

}