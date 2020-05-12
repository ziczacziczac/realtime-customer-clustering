package utils

import com.google.cloud.datastore._
import model.Customer

import scala.collection.mutable.ListBuffer

object DataConverter {
  private def convert_to_clustered_customer(keyFactory: KeyFactory,
                                            time_point: Int,
                                            cluster_id: Int,
                                            customer: Customer,
                                            new_mean_method: String,
                                            balance_length: Int): FullEntity[IncompleteKey] = {

    val entity_builder = FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("cus_id", customer.id)
      .set("cluster_id", cluster_id)
      .set("new_mean_method", new_mean_method)
      .set("balance_length", balance_length)

    val balance_raw = customer.balances_raw
    val balance_norm = customer.balances_norm
    for (i <- balance_norm.indices) {
      entity_builder.set("balance" + i, balance_norm(i) + "_" + balance_raw(i))
    }
    entity_builder.build()
  }

  private def convert_to_clustering_result(keyFactory: KeyFactory,
                                           time_point: Int,
                                           spent_time: Long,
                                           silhouette: Double,
                                           cluster_id: Int,
                                           means: ListBuffer[Double],
                                           new_mean_method: String,
                                           balance_length: Int): FullEntity[IncompleteKey] = {
    val entity_builder = FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("spent_time", spent_time)
      .set("silhouette", silhouette)
      .set("cluster_id", cluster_id)
      .set("new_mean_method", new_mean_method)
      .set("balance_length", balance_length)

    for (i <- means.indices) {
      entity_builder.set("mean" + i, means(i))
    }
    entity_builder.build()
  }

  private def convert_to_cluster_total_balances(keyFactory: KeyFactory,
                                                time_point: Int,
                                                cluster_id: Int, total: ListBuffer[Double],
                                                new_mean_method: String,
                                                balance_length: Int): FullEntity[IncompleteKey] = {
    val entity_builder = FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("cluster_id", cluster_id)
      .set("new_mean_method", new_mean_method)
      .set("balance_length", balance_length)

    for (i <- total.indices) {
      entity_builder.set("total" + i, total(i))
    }
    entity_builder.build()
  }

  private def convert_to_cluster_statistic(keyFactory: KeyFactory,
                                           time_point: Int,
                                           cluster_id: Int,
                                           mean: Double,
                                           variance: Double,
                                           new_mean_method: String,
                                           balance_length: Int): FullEntity[IncompleteKey] = {
    FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("cluster_id", cluster_id)
      .set("variance", variance)
      .set("new_mean_method", new_mean_method)
      .set("mean", mean)
      .set("balance_length", balance_length)
      .build()
  }

  private def convert_to_cluster_monitor(keyFactory: KeyFactory, time_point: Int, old_cluster: Int,
                                         new_cluster: Int, number_cus: Int, change_proportion: Double,
                                         new_mean_method: String,
                                         balance_length: Int): FullEntity[IncompleteKey] = {
    FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("cluster_previous", old_cluster)
      .set("cluster_current", new_cluster)
      .set("change_proportion", change_proportion)
      .set("number_cus", number_cus)
      .set("balance_length", balance_length)
      .set("new_mean_method", new_mean_method).build()
  }

  private def convert_to_cluster_distance(keyFactory: KeyFactory, time_point: Int, cluster_id: Int,
                                          mean_distance: Double, new_mean_method: String,
                                          balance_length: Int): FullEntity[IncompleteKey] = {
    FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("cluster_id", cluster_id)
      .set("new_mean_method", new_mean_method)
      .set("balance_length", balance_length)
      .set("mean_distance", mean_distance).build()
  }

  private def convert_to_cluster_samples(keyFactory: KeyFactory,
                                         time_point: Int,
                                         cluster_id: Int,
                                         sample: ListBuffer[Double],
                                         dist: Double,
                                         new_mean_method: String,
                                         balance_length: Int): FullEntity[IncompleteKey] = {
    val entity_builder = FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("cluster_id", cluster_id)
      .set("dist", dist)
      .set("new_mean_method", new_mean_method)
      .set("balance_length", balance_length)

    for (i <- sample.indices) {
      entity_builder.set("mean" + i, sample(i))
    }

    entity_builder.build()
  }

  private def convert_to_cluster_count(keyFactory: KeyFactory,
                                       time_point: Int,
                                       cluster_id: Int,
                                       count: Int,
                                       new_mean_method: String,
                                       balance_length: Int): FullEntity[IncompleteKey] = {
    FullEntity.newBuilder(keyFactory.newKey())
      .set("time_point", time_point)
      .set("cluster_id", cluster_id)
      .set("cluster_count", count)
      .set("new_mean_method", new_mean_method)
      .set("balance_length", balance_length)
      .build()
  }

  def save_clustered_customers(time_point: Int,
                               cluster_id: Int,
                               customer: Customer,
                               new_mean_method: String,
                               balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind("customer_clustered")
    val entity: FullEntity[IncompleteKey] = convert_to_clustered_customer(keyFactoryBuilder, time_point,
      cluster_id, customer, new_mean_method, balance_length)
    try {
      datastore.add(entity)
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def save_clustered_result(kind_prefix: String,
                            time_point: Int,
                            spent_time: Long,
                            silhouette: Double,
                            cluster_id: Int,
                            means: ListBuffer[Double],
                            new_mean_method: String,
                            balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind(kind_prefix + "_clustered_result")

    val entity: FullEntity[IncompleteKey] = convert_to_clustering_result(keyFactoryBuilder, time_point, spent_time, silhouette,
      cluster_id, means, new_mean_method, balance_length)
    try {
      datastore.add(entity)
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def save_clustered_total_balance(kind_prefix: String, time_point: Int,
                                   cluster_id: Int, total: ListBuffer[Double],
                                   new_mean_method: String, balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind(kind_prefix + "_cluster_balances")

    val entity: FullEntity[IncompleteKey] = convert_to_cluster_total_balances(keyFactoryBuilder, time_point, cluster_id,
      total, new_mean_method, balance_length)
    try {
      datastore.add(entity)
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def save_clustered_statistic(kind_prefix: String, time_point: Int,
                               cluster_id: Int, mean: Double, variance: Double,
                               new_mean_method: String, balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind(kind_prefix + "_cluster_statistic")

    val entity: FullEntity[IncompleteKey] = convert_to_cluster_statistic(
      keyFactoryBuilder, time_point, cluster_id, mean, variance, new_mean_method, balance_length)
    try {
      datastore.add(entity)
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def save_cluster_monitor(kind_prefix: String, time_point: Int, monitor_cluster: collection.mutable.Map[(Int, Int), (collection.immutable.List[Int], Double)],
                           new_mean_method: String, balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind(kind_prefix + "_cluster_monitor")

    for (key <- monitor_cluster.keySet) {
      val entity: FullEntity[IncompleteKey] = convert_to_cluster_monitor(keyFactoryBuilder, time_point, key._1, key._2,
        monitor_cluster(key)._1.length, monitor_cluster(key)._2, new_mean_method, balance_length)
      datastore.add(entity)
    }
  }

  def save_cluster_mean_distance(kind_prefix: String, time_point: Int, cluster_id: Int,
                                 mean_distance: Double,
                                 new_mean_method: String,
                                 balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind(kind_prefix + "_cluster_distance")
    val entity: FullEntity[IncompleteKey] = convert_to_cluster_distance(keyFactoryBuilder, time_point, cluster_id,
      mean_distance, new_mean_method, balance_length)
    datastore.add(entity)
  }

  def save_cluster_sample(kind_prefix: String, time_point: Int,
                          cluster_id: Int, seq: Seq[(ListBuffer[Double], Double)], new_mean_method: String,
                          balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind(kind_prefix + "_cluster_samples")
    seq.foreach(pair => {
      val entity: FullEntity[IncompleteKey] = convert_to_cluster_samples(keyFactoryBuilder,
        time_point, cluster_id, pair._1, pair._2, new_mean_method, balance_length)
      datastore.add(entity)
    })
  }

  def save_cluster_members_count(kind_prefix: String, time_point: Int,
                                 cluster_id: Int, number: Int, new_mean_method: String,
                                 balance_length: Int): Unit = {
    val datastore: Datastore = DatastoreOptions.getDefaultInstance.getService
    val keyFactoryBuilder = datastore.newKeyFactory().setKind(kind_prefix + "_cluster_member_count")
    val entity: FullEntity[IncompleteKey] = convert_to_cluster_count(keyFactoryBuilder, time_point, cluster_id, number,
      new_mean_method, balance_length)
    datastore.add(entity)
  }
}
