package clustering

import model.Customer
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

trait Clustering {
  def kmeans(means: Array[ListBuffer[Double]], vectors: RDD[Customer], distance: String, iter: Int = 1, debug: Boolean = false, kmeansEta: Double, kmeansMaxIterations: Double): (Array[ListBuffer[Double]], RDD[(Int, Customer)])
  def dtw(x: ListBuffer[Double], y: ListBuffer[Double]): Double
  def fastDtw(x: ListBuffer[Double], y: ListBuffer[Double], window: Int): Double
  def softDtw(x: ListBuffer[Double], y: ListBuffer[Double], gamma: Double): Double
  def read(lines: RDD[String]): RDD[Array[Double]]
}
