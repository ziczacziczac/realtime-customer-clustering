package main

import clustering.ClusteringImpl.{kmeans, silhouette}
import model.Customer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.Utils

import scala.collection.mutable.ListBuffer

object Main {
  def PROJECT_ID: String = "halogen-emblem-263413"

  def sample: Array[ListBuffer[Double]] = Array()
  var WINDOW_LENGTH: Int = 120

  var SLIDING_INTERVAL: Int = 60

  var DISTANCE_METHOD: String = "dtw"

  var TIME_RUNNING_IN_HOUR: Int = 10

  var NEW_MEAN_METHOD: String = "random"

  var ADDED_MONTH: ListBuffer[Int] = ListBuffer[Int]()

  def CHECKPOINT_DIRECTORY: String = "hdfs:///user/spark/checkpoint"

  var BUGKET_DIR = "gs://clustering-result/"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DTW")

    val sc: SparkContext = new SparkContext(conf)
    val source = args(0)
    /** K-means parameter: Convergence criteria */
    val kmeansEta: Double = args(1).toDouble
    /** K-means parameter: Maximum iterations */
    val kmeansMaxIterations = args(2).toInt

    /** Start by previous clustering result */
    val using_old_centers = args(3).toBoolean

    /** Minimum and maximum number of cluster will try to find
     * the best number of cluster*/
    val minimum_cluster = args(4).toInt
    val maximum_cluster = args(5).toInt

    val from = args(6).toInt
    val to = args(7).toInt

    if(args.length > 8) {
      NEW_MEAN_METHOD = args(8)
    }

    if(using_old_centers) {
      clustering_with_history_cluster_center(source, sc, kmeansEta, kmeansMaxIterations, minimum_cluster,
        maximum_cluster, from, to)

    } else {
      clustering_with_random_cluster_center(source, sc, kmeansEta, kmeansMaxIterations, minimum_cluster,
        maximum_cluster, from, to)
    }

  }

  def clustering_with_random_cluster_center(source: String, sc: SparkContext,
                                            kmeansEta: Double,
                                            kmeansMaxIterations: Int,
                                            minum_cluster: Int,
                                            maximum_cluster: Int,
                                            from_month: Int,
                                            to_month: Int): Unit = {
    for(i <- from_month to to_month) {
      val source_file = source + "/" + "month_" + i + ".csv"
      println("Source from: " + source_file)
      //    stream_clustering(args);
      println()
      println("Start load data")
      val customers = Utils.load_customer_data(sc, source_file)
      val begin = System.currentTimeMillis()
      println()
      val (best_mean, best_clustered, best_sil_mean_map, best_sil_mean) = optimize(minum_cluster,
        maximum_cluster, customers, kmeansEta, kmeansMaxIterations)
      val end = System.currentTimeMillis()
      val spent_time: Long = (end - begin) / 1000
      println("Total time for clustering is " + spent_time)
      Utils.print_means(i, best_mean)
      Utils.print_silhouette(i, best_sil_mean_map)
      Utils.save_result(i, spent_time, best_clustered, best_mean, best_sil_mean_map, null, "random")
    }
  }

  def clustering_with_history_cluster_center(source: String, sc: SparkContext,
                                             kmeansEta: Double,
                                             kmeansMaxIterations: Int,
                                             minum_cluster: Int,
                                             maximum_cluster: Int,
                                             from_month: Int,
                                             to_month: Int): Unit = {
    var previous_sil_mean: Double = -1
    var previous_sil_mean_map: scala.collection.Map[Int, Double] = null
    var previous_means: Array[ListBuffer[Double]] = null
    var previous_clustered: RDD[(Int, Customer)] = null
    var monitored: collection.mutable.Map[(Int, Int), (collection.immutable.List[Int], Double)] = null
    for(i <- from_month to to_month) {
      val source_file = source + "/" + "month_" + i + ".csv"
      println("Source from: " + source_file)
      //    stream_clustering(args);
      println()
      println("Start load data")
      val customers = Utils.load_customer_data(sc, source_file)
      val begin = System.currentTimeMillis()
      println()
      var best_k = 0
      var best_sil_mean_map: scala.collection.Map[Int, Double] = null

      var best_sil_mean: Double = 0
      var best_mean: Array[ListBuffer[Double]] = null
      var best_clustered: RDD[(Int, Customer)] = null

      if(previous_sil_mean == -1) {


        val (mean, clustered, sil_mean_map, sil_mean) = optimize(minum_cluster, maximum_cluster, customers, kmeansEta, kmeansMaxIterations)
        best_mean = mean
        best_clustered = clustered
        best_sil_mean_map = sil_mean_map
        best_sil_mean = sil_mean

        best_k = best_mean.length
      } else {
        val (means, clustered) = kmeans(previous_means, customers, DISTANCE_METHOD, 0, false, kmeansEta, kmeansMaxIterations)
        val sil = silhouette(clustered, means)
        val sil_mean = sil.values.sum / means.length
        println("Silhouette at " + i + " when clustered with " + means.length + " clusters is " + sil_mean)
        val (backward_mean, backward_clustered, backward_sil_mean_map, backward_sil_mean)
        = optimize_backward(minum_cluster, means.length - 1, sil_mean, previous_means.clone(), sil, clustered, customers, kmeansEta, kmeansMaxIterations)
        println("Silhouette backward at " + i + " when clustered with " + means.length + " clusters is " + backward_sil_mean)

        val (forward_mean, forward_clustered,forward_sil_mean_map, forward_sil_mean)
        = optimize_forward(maximum_cluster, means.length + 1, sil_mean, previous_means.clone(), sil, clustered, customers, kmeansEta, kmeansMaxIterations)
        println("Silhouette forward at " + i + " when clustered with " + means.length + " clusters is " + forward_sil_mean)

        if(backward_sil_mean < forward_sil_mean) {
          best_clustered = forward_clustered
          best_mean = forward_mean
          best_sil_mean_map = forward_sil_mean_map
          best_sil_mean = forward_sil_mean
          best_k = best_mean.length
        } else if(backward_sil_mean > forward_sil_mean){
          best_clustered = backward_clustered
          best_mean = backward_mean
          best_sil_mean_map = backward_sil_mean_map
          best_sil_mean = backward_sil_mean
          best_k = best_mean.length
        } else {
          best_clustered = clustered
          best_mean = means
          best_sil_mean_map = sil
          best_sil_mean = sil_mean
          best_k = best_mean.length
        }
        monitored = monitor_cluster_change(previous_clustered, best_clustered)
      }
      previous_means = best_mean
      previous_sil_mean = best_sil_mean
      previous_sil_mean_map = best_sil_mean_map
      previous_clustered = best_clustered

      val end = System.currentTimeMillis()
      val spent_time: Long = (end - begin) / 1000
      println("Total time for clustering is " + spent_time)
      Utils.print_means(i, best_mean)
      Utils.print_silhouette(i, best_sil_mean_map)
      Utils.save_result(i, spent_time, best_clustered, best_mean, best_sil_mean_map, monitored, "history")
    }

  }

  def monitor_cluster_change(previous_clustered: RDD[(Int, Customer)], current_clustered: RDD[(Int, Customer)]): collection.mutable.Map[(Int, Int), (collection.immutable.List[Int], Double)] = {
    println("Start monitor cluster change")
    val previous = previous_clustered.mapValues(cus => List(cus.id))
      .reduceByKey(Utils.merge_list).collectAsMap()
    val current = current_clustered.mapValues(cus => List(cus.id))
      .reduceByKey(Utils.merge_list).collectAsMap()
    var map = collection.mutable.Map[(Int, Int), (collection.immutable.List[Int], Double)]()
    for(i <- previous.keySet) {
      val list_i = previous(i)
//      println("Number of member in previous group " + i + ": " + list_i.size)
      for(j <- current.keySet) {
        val list_j = current(j)
//        println("Number of member in current group " + j + ": " + list_j.size)
        val cross = Utils.inner_join(list_i, list_j)
        println("Inner joint between previous group " + i + " and current group " + j + " is " + cross.length)
        if(cross.nonEmpty)
          map = map + ((i, j) -> (cross, cross.length * 1.0/list_i.length))
      }
    }
    map
  }

  def optimize_backward(minimum_cluster: Int,
                        current_cluster: Int,
                        previous_sil_mean: Double,
                        previous_means: Array[ListBuffer[Double]],
                        previous_sil_map: collection.Map[Int, Double],
                        previous_clustered: RDD[(Int, Customer)],
                        customers: RDD[Customer],
                        kmeansEta: Double,
                        kmeansMaxIterations: Int):
  (Array[ListBuffer[Double]], RDD[(Int, Customer)], collection.Map[Int, Double], Double) = {
    print("Start looking backward with number of cluster " + current_cluster)
    if(minimum_cluster > current_cluster) {
      ( previous_means, previous_clustered, previous_sil_map, previous_sil_mean)
    } else {
      val cluster_have_min_sil = previous_sil_map.min._1
      val newMeans = Utils.removeAt(previous_means, cluster_have_min_sil)
      val (means, clustered) = kmeans(newMeans, customers, DISTANCE_METHOD, 0, false, kmeansEta, kmeansMaxIterations)
      val sil = silhouette(clustered, means)
      val sil_mean = sil.values.sum / current_cluster
      if(sil_mean <= previous_sil_mean) {
        (previous_means, previous_clustered, previous_sil_map, previous_sil_mean)
      } else {
        optimize_backward(minimum_cluster, current_cluster - 1, sil_mean, means, sil,
          clustered, customers, kmeansEta, kmeansMaxIterations)
      }
    }
  }


  def optimize_forward(maximum_cluster: Int,
                       current_cluster: Int,
                       previous_sil_mean: Double,
                       previous_means: Array[ListBuffer[Double]],
                       previous_sil_map: collection.Map[Int, Double],
                       previous_clustered: RDD[(Int, Customer)],
                       customers: RDD[Customer],
                       kmeansEta: Double,
                       kmeansMaxIterations: Int):
  (Array[ListBuffer[Double]], RDD[(Int, Customer)], collection.Map[Int, Double], Double) = {
    println("Start looking forward with number of cluster " + current_cluster)
    if(maximum_cluster < current_cluster) {
      (previous_means, previous_clustered, previous_sil_map, previous_sil_mean)
    } else {
      var newMeans: Array[ListBuffer[Double]] = null

      if(NEW_MEAN_METHOD.equals("random")) {
        println("Generate new mean using random method")
        newMeans = Utils.addNewRandomMean(previous_means)
      } else if (NEW_MEAN_METHOD.equals("furthest")) {
        println("Generate new mean using random furthest")
        newMeans = Utils.addNewFurthestMean(previous_means, previous_clustered)
        Utils.print_means(-1, newMeans)
      } else {
        println("Generate new mean using mean of previous centroid")
        newMeans = Utils.addNewMean(previous_means)
      }

      val (means, clustered) = kmeans(newMeans, customers, DISTANCE_METHOD, 0, false, kmeansEta, kmeansMaxIterations)
      val sil = silhouette(clustered, means)
      val sil_mean = sil.values.sum / current_cluster
      println("Optimize forward with " + current_cluster + " is: " + sil_mean + " compare with previous: " + previous_sil_mean)
      if(sil_mean <= previous_sil_mean) {
        (previous_means, previous_clustered, previous_sil_map, previous_sil_mean)
      } else {
        optimize_forward(maximum_cluster, current_cluster + 1, sil_mean, means, sil,
          clustered, customers, kmeansEta, kmeansMaxIterations)
      }
    }
  }

  def optimize(minium_cluster: Int, maximum_cluster: Int,
               customers: RDD[Customer], kmeansEta: Double, kmeansMaxIterations: Int): (Array[ListBuffer[Double]], RDD[(Int, Customer)], collection.Map[Int, Double], Double) = {
    var best_k = 0
    var best_sil_mean_map: scala.collection.Map[Int, Double] = null

    var best_sil_mean: Double = 0
    var best_mean: Array[ListBuffer[Double]] = null
    var best_clustered: RDD[(Int, Customer)] = null
    for(k <- minium_cluster to maximum_cluster) {
      println("=========Trying clustering with " + k + " clusters==========")
      val initMeans = Utils.take_sample(k, 12).toArray
      val (means, clustered) = kmeans(initMeans, customers, DISTANCE_METHOD, 0, false, kmeansEta, kmeansMaxIterations)
      val sil = silhouette(clustered, means)
      val sil_mean = sil.values.sum / k

      if(best_sil_mean < sil_mean) {
        best_sil_mean = sil_mean
        best_k = k
        best_clustered = clustered
        best_mean = means
        best_sil_mean_map = sil
      }
      println()
      println("========Cluster with " + k + " cluster and silhouette is: " + sil_mean + "==========")

      println()

    }
    (best_mean, best_clustered, best_sil_mean_map, best_sil_mean)
  }
}
