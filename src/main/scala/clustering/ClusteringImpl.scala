package clustering

import model.Customer
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.{DataConverter, Utils}

import scala.collection.mutable.ListBuffer

object ClusteringImpl extends Clustering {

  override def kmeans(means: Array[ListBuffer[Double]], customers: RDD[Customer], distanceType: String, iter: Int, debug: Boolean, kmeansEta: Double, kmeansMaxIterations: Double):
  (Array[ListBuffer[Double]], RDD[(Int, Customer)]) = {
    val clustered = customers.map(v => (findClosest(v.balances_norm, means, distanceType), v))
    val clusterAvg = clustered.groupByKey().mapValues(averageVectors).collect().toMap
    val newMeans = means.indices.map(i => clusterAvg.getOrElse(i, means(i))).toArray

    val distance = meansChanged(means, newMeans)

    if (debug) {
      println(
        s"""Iteration: $iter
           |  * current distance: $distance
           |  * desired distance: $kmeansEta
           |  * means:""".stripMargin)
      for (idx <- means.indices)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${subRows(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance, kmeansEta)) {
       println("Current iteration " + iter)
      (newMeans, clustered)
    }
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, customers, distanceType, iter + 1, debug, kmeansEta, kmeansMaxIterations)
    else {
      println("Reached max iterations!")
      (newMeans, clustered)
    }
  }

  override def dtw(x: ListBuffer[Double], y: ListBuffer[Double]): Double = {
    val len_x = x.size
    val len_y = y.size
    val D = Array.ofDim[Double](len_x + 1, len_y + 1)
    for (i <- 0 to len_x) D(i)(0) = Double.MaxValue
    for (j <- 0 to len_y) D(0)(j) = Double.MaxValue

    for (i <- 1 to len_x) {
      for (j <- 1 to len_y) {
        val dist = euclidean(x(i - 1), y(j - 1))
        if (i == 1 && j == 1)
          D(i)(j) = dist
        else
          D(i)(j) = min(D(i - 1)(j - 1), D(i - 1)(j), D(i)(j - 1)) + dist
      }
    }

    D(len_x - 1)(len_y - 1)
  }

  def meansChanged(means: Array[ListBuffer[Double]], newMeans: Array[ListBuffer[Double]]): Double = {
    var changed: Double = 0
    for (i <- means.indices) {
      val mean = means(i)
      val newMean = newMeans(i)
      changed += subRows(mean, newMean)
    }
    changed
  }

  def subRows(a: ListBuffer[Double], b: ListBuffer[Double]): Double = {
    var i = 0
    var diff: Double = 0
    while (i < a.size) {
      diff += math.abs(a(i) - b(i))
      i += 1
    }
    diff
  }

  def euclidean(a: Double, b: Double): Double = {
    math.abs(a - b)
  }

  def squaredEuclidean(a: Double, b: Double): Double = {
    math.pow(a - b, 2)
  }

  def min(a: Double, b: Double, c: Double): Double = {
    math.min(math.min(a, b), c)
  }

  def softMin(a: Double, b: Double, c: Double, gamma: Double): Double = {
    val a_a = a / (-gamma)
    val b_b = b / (-gamma)
    val c_c = c / (-gamma)
    val max = math.max(math.max(a_a, b_b), c_c)
    var tmp: Double = 0
    tmp += math.exp(a_a - max)
    tmp += math.exp(b_b - max)
    tmp += math.exp(c_c - max)
    -gamma * (math.log(tmp) + max)
  }

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double, kmeansEta: Double): Boolean =
    distance < kmeansEta

  /** Return the closest point */
  def findClosest(p: ListBuffer[Double], centers: Array[ListBuffer[Double]], distanceType: String): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- centers.indices) {

      var tempDist: Double = 0
      if (distanceType == "dtw") {
        tempDist = dtw(p, centers(i))
      } else {
        tempDist = softDtw(p, centers(i), 0.01)
      }

      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[Customer]): ListBuffer[Double] = {
    val iter = ps.iterator
    var count = 0
    val sumArray = iter.next().balances_norm
    while (iter.hasNext) {
      val item = iter.next.balances_norm
      for (i <- item.indices) {
        sumArray.updated(i, sumArray(i) + item(i))
      }
      count += 1
    }

    for (i <- sumArray.indices) {
      sumArray.updated(i, sumArray(i) / count)
    }
    sumArray
  }

  override def fastDtw(x: ListBuffer[Double], y: ListBuffer[Double], window: Int): Double = ???

  override def softDtw(x: ListBuffer[Double], y: ListBuffer[Double], gamma: Double): Double = {
    val len_x = x.size
    val len_y = y.size
    val D = Array.ofDim[Double](len_x + 1, len_y + 1)
    for (i <- 0 to len_x) D(i)(0) = Double.MaxValue
    for (j <- 0 to len_y) D(0)(j) = Double.MaxValue
    for (i <- 1 to len_x) {
      for (j <- 1 to len_y) {
        val dist = squaredEuclidean(x(i - 1), y(j - 1))
        if (i == 1 && j == 1)
          D(i)(j) = dist
        else
          D(i)(j) = softMin(D(i - 1)(j - 1), D(i - 1)(j), D(i)(j - 1), gamma) + dist
      }
    }
    D(len_x)(len_y)
  }

  override def read(lines: RDD[String]): RDD[Array[Double]] = lines
    .map(line => line.split(",").map(_.toDouble))

  def prediction(): Unit = {

  }

  def silhouette(clustered: RDD[(Int, Customer)], means: Array[ListBuffer[Double]]): collection.Map[Int, Double] = {
    println("Start calculate silhouette")
    clustered.map(cluster => {
      val cluster_id = cluster._1
      val customer = cluster._2
      val ai = dtw(customer.balances_norm, means(cluster_id))
      val bi = compute_mean_dist(cluster_id, customer.balances_norm, means)
      val si = (bi - ai) / math.max(ai, bi)

      (cluster_id, (1, si))
    }).reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
      .mapValues(v => v._2 / v._1)
      .collectAsMap()
  }

  def compute_mean_dist(except_id: Int, balances: ListBuffer[Double], means: Array[ListBuffer[Double]]): Double = {
    var dists = Array[Double]()
    for(i <- means.indices) {
      if(i != except_id)
        dists = dists :+ dtw(balances, means(i))
    }
    dists.min
  }

}
