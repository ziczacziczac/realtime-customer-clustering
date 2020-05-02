package model

case class ClusteringResult(time_point: Int, num_clusters: Int, spent_time: Int,
                            silhouette: Double, clusters: Array[Cluster])
