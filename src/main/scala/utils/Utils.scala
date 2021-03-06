package utils

import clustering.ClusteringImpl.{euclidean, min}
import com.google.cloud.Timestamp
import com.google.cloud.datastore.{FullEntity, IncompleteKey, KeyFactory}
import model.Customer
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{PCA, PCAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Utils {

  def printCurrentCus(customers: ListBuffer[Customer]): Unit = {
    customers.foreach(cus => {
      println("Customer ID: " + cus.id)
      cus.balances_norm.foreach(balance => print(" " + balance))
      println()
    })
  }

  def convertToEntity(message: Array[String], keyFactoryBuilder: String => KeyFactory): FullEntity[IncompleteKey] = {
    val messageFactory = keyFactoryBuilder("message")
    FullEntity.newBuilder(messageFactory.newKey())
      .set("value", message.toString)
      .set("datetime", Timestamp.now())
      .build()
  }

  def writeClusteringResult(bucket: String, clustered: RDD[(Int, Customer)], bestSil: Double, bestK: Int, month: Int): Unit = {
    val fileName = bucket + month + "_" + bestK + "_" + math.round(bestSil * 1000)
    clustered.map(pair => {
      pair._1 + "," + customerToString(pair._2)
    }).saveAsTextFile(fileName)
  }

  def writePcaResult(bucket: String, explainedVariance: Array[Double], pca_cus: RDD[Customer], month: Int): Unit = {
    val fileName = bucket + month + "_pca"
    pca_cus.map(customerToString).saveAsTextFile(fileName);
  }

  def customerToString(customer: Customer): String = {
    customer.id + "," + customer.balances_norm.mkString(",")
  }

  def stringToCustomer(cusInString: String, balance_length: Int): Customer = {
    if (cusInString != null && !cusInString.isEmpty) {
      val cusElems = cusInString.split(",")
      val cusId = cusElems(0).toInt
      val balances: ListBuffer[Double] = ListBuffer[Double]()

      for (i <- 1 until cusElems.size) {
        balances += cusElems(i).toDouble
      }
      val balance_raw = balances.slice(0, balance_length)
      Customer(cusId, normalize(balance_raw), balance_raw, balances)
    } else {
      null
    }
  }

  def normalize(balances: ListBuffer[Double]): ListBuffer[Double] = {
    val max = balances.max
    val min = balances.min

    var normalized_balances: ListBuffer[Double] = ListBuffer[Double]()
    if (max == min) {
      for (_ <- balances.indices) {
        normalized_balances += 0
      }
    } else {
      for (i <- balances.indices) {
        normalized_balances += (balances(i) - min) / (max - min)
      }
    }

    normalized_balances
  }

  def sum_balance(balances_1: ListBuffer[Double], balances_2: ListBuffer[Double]): ListBuffer[Double] = {
    var balance_total: ListBuffer[Double] = ListBuffer()
    for (i <- balances_1.indices) {
      balance_total += balances_1(i) + balances_2(i)
    }
    balance_total
  }

  def print_means(time_point: Int, means: Array[ListBuffer[Double]]): Unit = {
    println()
    println("Means in time point: " + time_point)
    means.indices
      .foreach(i => {
        print("Mean of cluster " + i + ": ");
        means(i).foreach(d => print(d + " "))
        println()
      })
  }

  def print_silhouette(time_point: Int, silhouettes: collection.Map[Int, Double]): Unit = {
    println()
    println("Silhouette values of time point: " + time_point)
    silhouettes.foreach(sil => println(sil._1 + ": " + sil._2))
    println()
  }

  def compare_balances_norm(this_balances: ListBuffer[Double], that_balances: ListBuffer[Double]): Boolean = {
    var same = true
    for (i <- this_balances.indices) {
      if (that_balances(i) != this_balances(i))
        same = false
    }
    same
  }

  def take_sample(n_samples: Int, len: Int): List[ListBuffer[Double]] = {
    val random = Random
    var samples = List[ListBuffer[Double]]()
    for (_ <- 1 to n_samples) {
      var list = ListBuffer[Double]()
      for (_ <- 1 to len) {
        list += random.nextDouble()
      }
      samples = samples :+ list
    }
    samples
  }

  def save_result(time_point: Int, spent_time: Long,
                  clustered: RDD[(Int, Customer)], means: Array[ListBuffer[Double]],
                  silhouettes: collection.Map[Int, Double],
                  monitor_cluster: collection.mutable.Map[(Int, Int), (List[Int], Double)],
                  n_samples: Int,
                  kind_prefix: String,
                  new_mean_method: String,
                  balance_length: Int): Unit = {

    /**
     * Save the statistic of each clusters
     */
    clustered.mapValues(cus => (1, cus.balances_raw))
      .reduceByKey((k1, k2) => (k1._1 + k2._1, Utils.sum_balance(k1._2, k2._2)))
      .foreach(pair => {
        val cluster_id = pair._1
        val values = pair._2
        val count = values._1
        val total_balances = values._2
        val means = Utils.divide(total_balances, count)
        val m = Utils.mean(means)
        val sd = Utils.sd(means)
        DataConverter.save_clustered_statistic(kind_prefix, time_point, cluster_id, m, sd, new_mean_method, balance_length)
      })

    /**
     * Save the cluster total balances
     */
    clustered.mapValues(_.balances_future)
      .reduceByKey(Utils.sum_balance)
      .foreach(balances_total => DataConverter.save_clustered_total_balance(kind_prefix, time_point,
        balances_total._1, balances_total._2, new_mean_method, balance_length))

    /**
     * Save the clustering result
     */
    for (i <- means.indices) {
      if (silhouettes.contains(i))
        DataConverter.save_clustered_result(kind_prefix, time_point, spent_time, silhouettes(i), i, means(i),
          new_mean_method, balance_length)
      else
        DataConverter.save_clustered_result(kind_prefix, time_point, spent_time, -1, i, means(i),
          new_mean_method, balance_length)
    }

    /**
     * Save the cluster monitor
     */
    if (monitor_cluster != null)
      DataConverter.save_cluster_monitor(kind_prefix, time_point, monitor_cluster, new_mean_method, balance_length)

    /**
     * Save the cluster mean distance
     */
    calculate_mean_distance_in_clusters(means, clustered)
      .foreach(pair => DataConverter.save_cluster_mean_distance(kind_prefix, time_point, pair._1, pair._2,
        new_mean_method, balance_length))

    /**
     * Save the cluster mean sample
     */
    val pca2 = pca_fit(clustered, 2)
    val pca3 = pca_fit(clustered, 3)
    val samples_map = take_cluster_sample(n_samples, means, clustered)

    sample_pca(pca2, pca3, samples_map)
      .foreach(pair => {
        DataConverter.save_cluster_sample(kind_prefix, time_point, pair._1, 2, pair._2, new_mean_method, balance_length)
      })

    /**
     * Save the number of members of each cluster
     */
    count_cluster_members(clustered)
      .foreach(pair => DataConverter.save_cluster_members_count(kind_prefix, time_point, pair._1,
        pair._2, new_mean_method, balance_length))

    /**
     * Save the cluster members
     */
    cluster_members(clustered)
      .saveAsTextFile("gs://real_time_clustering_data/cluster_members/" + kind_prefix + "_" + new_mean_method + "_" + time_point + "_cluster_members.txt")
//      .foreach(pair => {
//        DataConverter.save_cluster_members(kind_prefix, time_point, pair._1, pair._2, new_mean_method, balance_length)
//      })
  }

  def means(clustered: RDD[(Int, Customer)]): scala.collection.Map[Int, ListBuffer[Double]] = {
    clustered
      .mapValues(cus => (1, cus.balances_norm))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, sum_balance(v1._2, v2._2)))
      .mapValues(pair => divide(pair._2, pair._1))
      .collectAsMap()
  }

  def count_cluster_members(clustered: RDD[(Int, Customer)]): collection.Map[Int, Int] = {
    clustered.mapValues(_ => 1)
      .reduceByKey((c1, c2) => c1 + c2)
      .collectAsMap()
  }

  def take_cluster_sample(n_samples: Int, means: Array[ListBuffer[Double]], clustered: RDD[(Int, Customer)]): scala.collection.Map[Int, Seq[(ListBuffer[Double], Double)]] = {
    clustered.mapValues(customer => Array(customer.balances_norm))
      .reduceByKey((arr1, arr2) => arr1 ++ arr2)
      .collectAsMap()
      .map(pair => {
        val cluster_id = pair._1
        val array = pair._2
        var dis_map: Map[ListBuffer[Double], Double] = Map()
        array.foreach(cus => {
          dis_map += cus -> dtw(cus, means(cluster_id))
        })


        if (array.length < n_samples) {
          (cluster_id, dis_map.toSeq.sortWith(_._2 < _._2).take(array.length))
        } else {
          (cluster_id, dis_map.toSeq.sortWith(_._2 < _._2).take(n_samples))
        }
      })
  }

  def cluster_members(clustered: RDD[(Int, Customer)]): RDD[(Int, String)] = {
    clustered.mapValues(cus => cus.id + "")
      .reduceByKey((v1, v2) => v1 + ";"+ v2)
  }

  def load_customer_data(sc: SparkContext, source: String, balance_length: Int): RDD[Customer] = {
    sc.textFile(source)
      .flatMap(data => data.split("\n"))
      .filter(line => line.nonEmpty)
      .map(line =>
        Utils.stringToCustomer(line, balance_length)
      )
  }

  def pca(customers: RDD[Customer]): (Array[Double], RDD[Customer]) = {
    val rows = customers
      .map(cus => new LabeledPoint(cus.id, Vectors.dense(cus.balances_norm.toArray)))
    val pca = new PCA(2).fit(rows.map(_.features))
    println("Explained Variance: ")
    pca.explainedVariance.values
      .foreach(v => print(v + " "))
    println()
    val cusPca = rows.map(p => p.copy(features = pca.transform(p.features)))
      .map(p => model.Customer(p.label.toInt, ListBuffer(p.features.toArray: _*), ListBuffer(p.features.toArray: _*), ListBuffer(p.features.toArray: _*)))
    (pca.explainedVariance.toArray, cusPca)
  }

  def pca_fit(clustered: RDD[(Int, Customer)], dim: Int): PCAModel = {
    val rows = clustered.map(pair => new LabeledPoint(pair._1, Vectors.dense(pair._2.balances_norm.toArray)))
    new PCA(dim).fit(rows.map(_.features))
  }

  /**
   * Transform sample to PCA
   *
   * @param pca_model2   : fitted pca model
   * @param pca_model3   : fitted pca model
   * @param samples_map : cluster samples map
   * @return
   */
  def sample_pca(pca_model2: PCAModel, pca_model3: PCAModel, samples_map: scala.collection.Map[Int, Seq[(ListBuffer[Double], Double)]]):
  scala.collection.Map[Int, Seq[(ListBuffer[Double], ListBuffer[Double], ListBuffer[Double], Double)]] = {
    samples_map.map(pair => {
      val seq = pair._2.map(pair2 => {
        val sample = pair2._1
        val point = new LabeledPoint(pair._1, Vectors.dense(sample.toArray))
        val point_pca2 = point.copy(features = pca_model2.transform(point.features))
        val point_pca3 = point.copy(features = pca_model3.transform(point.features))
        (pair2._1, ListBuffer(point_pca2.features.toArray: _*), ListBuffer(point_pca3.features.toArray: _*), pair2._2)
      })
      (pair._1, seq)
    })
  }

  def removeAt(array: Array[ListBuffer[Double]], index: Int): Array[ListBuffer[Double]] = {
    if (array == null || array.isEmpty) return array
    array.indices.collect({ case i if i != index => array(i) }).toArray
  }

  def addNewMean(array: Array[ListBuffer[Double]]): Array[ListBuffer[Double]] = {
    if (array == null || array.isEmpty) return array
    var newMean = array(0).clone()
    for (i <- 1 until array.length) {
      for (j <- newMean.indices) {
        newMean(j) = newMean(j) + array(i)(j)
      }
    }
    array :+ newMean.map(_ / array.length)
  }

  def addNewRandomMean(array: Array[ListBuffer[Double]], balance_length: Int): Array[ListBuffer[Double]] = {
    if (array == null || array.isEmpty) return array
    val newMean = take_sample(1, balance_length)(0)
    array :+ newMean
  }

  def addNewFurthestMean(array: Array[ListBuffer[Double]], clustered: RDD[(Int, Customer)]): Array[ListBuffer[Double]] = {
    val newMean = clustered.map(pair => {
      val cluster_id = pair._1
      val customer = pair._2
      val own_dis = dtw(customer.balances_norm, array(cluster_id))

      var total_distance: Double = 0
      for (i <- array.indices) {
        if(i != cluster_id) {
          val dis = dtw(customer.balances_norm, array(i))
          total_distance += dis
        }
      }
      (customer.balances_norm, own_dis * total_distance)
    }).max()(new Ordering[Tuple2[ListBuffer[Double], Double]]() {
      override def compare(x: (ListBuffer[Double], Double), y: (ListBuffer[Double], Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })._1

    array :+ newMean
  }

  def addNewFurthestMeanInCluster(array: Array[ListBuffer[Double]], clustered: RDD[(Int, Customer)], min_cluster_silhouette: Int): Array[ListBuffer[Double]] = {
    val mean = array(min_cluster_silhouette)
    val newMean = clustered.filter(pair => pair._1 == min_cluster_silhouette).map(pair => {
      val customer = pair._2
      val dis = dtw(customer.balances_norm, mean)
      (customer.balances_norm, dis)
    }).max()(new Ordering[Tuple2[ListBuffer[Double], Double]]() {
      override def compare(x: (ListBuffer[Double], Double), y: (ListBuffer[Double], Double)): Int =
        Ordering[Double].compare(x._2, y._2)
    })._1

    array :+ newMean
  }

  def calculate_mean_distance_in_clusters(means: Array[ListBuffer[Double]], clustered: RDD[(Int, Customer)]): RDD[(Int, Double)] = {
    clustered.map(pair => {
      val cluster_id = pair._1
      val customer = pair._2

      (cluster_id, (1, dtw(customer.balances_norm, means(cluster_id))))
    }).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(pair => pair._2 / pair._1)
  }

  def dtw(x: ListBuffer[Double], y: ListBuffer[Double]): Double = {
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

  def merge_list(arr1: List[Int], arr2: List[Int]): List[Int] = {
    val merged = arr1 ::: arr2
    merged.distinct
  }

  def inner_join(arr1: List[Int], arr2: List[Int]): List[Int] = {
    var inner = List[Int]()
    for (i <- arr1.indices) {
      if (arr2.contains(arr1(i)))
        inner = arr1(i) :: inner
    }
    inner
  }

  def divide(arr: ListBuffer[Double], count: Int): ListBuffer[Double] = {
    val array_divided = arr.clone()
    for (i <- array_divided.indices) {
      array_divided(i) = array_divided(i) / count
    }
    array_divided
  }

  def mean(arr: ListBuffer[Double]): Double = {
    var total: Double = 0
    for (i <- arr.indices) {
      total = total + arr(i)
    }
    total / arr.length
  }

  def sd(arr: ListBuffer[Double]): Double = {
    val m = mean(arr)
    var total: Double = 0
    for (i <- arr.indices) {
      total = total + math.pow(arr(i) - m, 2)
    }
    math.sqrt(total / (arr.length - 1))
  }

  def main(args: Array[String]): Unit = {
    val arr1 = Array(ListBuffer[Double](1, 2, 3))
    val arr2 = Array(ListBuffer[Double](3, 4, 5))
    val arr = arr1 ++ arr2
    println(arr.length)
    //    take_sample(5, 12).toArray
    //          val l1 = ListBuffer[Double](1,2, 3, 4)
    //    println(mean(l1))
    //    println(sd(l1))
    //    val l2 = divide(l1, 2)
    //    val s = "17291,0.070957,0.070957,0.070957,0.070957,0.070957,0.070957,0.070957,0.070957,500.070957,0.070957,0.06435700000000001,0.057757,0.041257,0.024756999999999998,0.008256999999999999"
    //    stringToCustomer(s)
    //    take_sample(2, 12)
    //          val l2 = ListBuffer[Double](2,3)
    //          val l3 = ListBuffer[Double](3,4)
    //          val l4 = ListBuffer[Double](4,5)
    //          var elem = Array(l1, l2, l3, l4)
    //          var elem2 = addNewMean(elem)
    //          elem = addNewMean(elem)
    //          for(i <- elem2.indices){
    //            print(elem2(i))
    //          }
    //    for(i <- l2.indices) {
    //      println(l2(i))
    //    }
    //    val l1 = List(1, 2, 3)
    //    val l2 = List(2, 3, 4)
    ////    val elem = merge_list(l1, l2)
    //    val elem = inner_join(l1, l2)
    //    for (i <- elem.indices) {
    //      print(elem(i))
    //    }

    //    val s = "true"
    //    s.toBoolean
    //    var map: collection.mutable.Map[(Int, Int), List[Int]] = collection.mutable.Map[(Int, Int), collection.immutable.List[Int]]()
    //    map = map + ((1, 2) -> collection.immutable.List(1, 2, 3))
    //    map = map + ((1, 4) -> collection.immutable.List(1, 2, 3))
    //    for(i <- map.keySet) {
    //      println(map.get(i))
    //    }

  }
}
