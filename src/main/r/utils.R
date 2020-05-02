setClass("customer", slots = list(id="numeric", balances="array", cluster="numeric", pca="array"))
setClass("clustering_res_monthly", slots = list(month="numeric", customers="array", sil="numeric", n_clusters="numeric"))
parse_dir_name <- function(dir_name) {
  elems = as.double(strsplit(basename(dir_name), "_")[[1]])
  clustering_res_month = new ("clustering_res_monthly", month = elems[1], n_clusters = elems[2], sil = elems[3] / 1000)
  return(clustering_res_month)
}

#input: directory of clustering result
#output: monthly clustering result
read_each_result <- function(res_dir) {
  customers = c()
  list_files = list.files(res_dir)
  print(res_dir)
  for(part in list_files) {
    #print(part)
    if(part != "_SUCCESS") {
      file_path = paste(res_dir, part, sep = "")
      part_csv = as.matrix(read.csv(file_path, header = FALSE, sep = ","))
      for(i in 1:length(part_csv[, 1])) {
        r <- as.array(part_csv[i,])
        
        cus <- new("customer", id = r[2], cluster = r[1], balances = r[3:length(r)])
        customers <- c(customers, cus)
      }
      
    }
  }
  
  clustering_res_month = parse_dir_name(res_dir)
  
  month <- clustering_res_month@month
  pca_month <- paste(month, "pca", sep = "_")
  pca_dir <- paste(paste(gsub("/", "\\\\", dirname(res_dir)), pca_month, sep = "\\"), "\\", sep = "")
  pca <- read_each_month_pca(pca_dir)
  clustering_res_month@customers = as.array(add_pca_to_cus(customers, pca))
  return(clustering_res_month)
}

#input: Pca dir for each month
#output: Pca matrix for all customer in this month
read_each_month_pca <- function(pca_dir) {
  print(pca_dir)
  pca <- c()
  list_files = list.files(pca_dir)
  for(part in list_files) {
    if(part != "_SUCCESS") {
      file_path = paste(pca_dir, part, sep = "")
      #print(file_path)
      part_csv = as.matrix(read.csv(file_path, header = FALSE, sep = ","))
      pca <- rbind(pca, part_csv)
    }
  }
  return (pca)
}

#input: list customer and pca matrix
#ouput: customers with pca value
add_pca_to_cus <- function(customers, pca) {
  list_cus <- c()
  for(cus in customers) {
    cus_id <- cus@id
    cus_pca <- as.array(pca[pca[, 1] == cus_id][2:3])
    cus@pca <- cus_pca
    list_cus <- c(list_cus, cus)
  }
  return(list_cus)
}

#input: clustering result directory
#output: list month, each month is the clustering result of each customer
read_clustering_res <- function(clustering_res_dir){
  list_dirs = list.dirs(clustering_res_dir)
  list_months = c()
  for(dir in list_dirs) {
    if(clustering_res_dir != dir && !grepl("pca", dir)){
      dir_name = basename(dir)
      dir_path = paste(clustering_res_dir, dir_name, "\\", sep = "")
      #print(dir_path)
      
      r = read_each_result(dir_path)
      list_months = c(list_months, r)
    }
    
  }
  return(list_months)
}

#input: customers
#ouput: matrix of all customers in rows
get_all_member_of_cluster <- function(cluster_id, customers) {
  cus_ids <- c()
  customers_in_cluster <- c()
  for(cus in customers) {
    if(cus@cluster == cluster_id) {
      cus_ids <- c(cus_ids, cus@id)
      customers_in_cluster <- rbind(customers_in_cluster, cus@balances)
    }
  }
  rownames(customers_in_cluster) <- cus_ids
  return(customers_in_cluster)
}

#input: month cluster result
#ouput: all centers of clusters
get_all_centers_of_cluster <- function(month_cluster) {
  num_clusters = month_cluster@n_clusters
  centers <- c()
  for(i in 0:(num_clusters - 1)) {
    cuss <- get_all_member_of_cluster(i, month_cluster@customers)
    
    if(!is.null(cuss)) {
      center <- colMeans(cuss)
      centers <- rbind(centers, center)
    } else {
      centers <- rbind(centers, rep(-1, 12))
    }
    
    
  }
  return(centers)
}

#input: cluster id, month clustering result
#output: nearest customer id with center
get_nearest_customer_with_center <- function(cluster_id, month_cluster, dist_type = "dtw") {
  dis <- c();
  cluster_members <- get_all_member_of_cluster(cluster_id, month_cluster@customers)
  if(is.null(cluster_members)) {
    return(c(-1))
  }
  center <- colMeans(cluster_members)
  for(i in 1:length(cluster_members[, 1])) {
    if(dist_type == "dtw") {
      dis[i] <- dtw(cluster_members[i, ], center)$distance;
    } else {
      dis[i] <- sdtw(cluster_members[i, ], center);
    }
  }
  names(dis) <- rownames(cluster_members);
  sorted_dis <- sort(dis)
  return(as.integer(names(sorted_dis)))
}

#input: list customer and a customer id
#output: customer have id equal cus_id
get_customer_balance_by_id <- function(customers, cus_id) {
  for(cus in customers) {
    if(cus@id == cus_id) {
      return(cus)
    }
  }
  return(NULL)
}

cluster_matrix <- function(customers) {
  m <- matrix(0, nrow = length(customers), ncol = length(customers))
  for(i in 1:length(customers)) {
    for(j in 1:length(customers)) {
      if(customers[[i]]@cluster == customers[[j]]@cluster) {
        m[i, j] = 1
      }
    }
  }
  return(m)
}

get_customer_change_cluster <- function(list_cluster_matrix, n_month, threshold) {
  list_change <- list()
  for(i in 1:length(list_cluster_matrix)) {
    print(i)
    if(i + n_month <= length(list_cluster_matrix)) {
      current_matrix <- list_cluster_matrix[[i]]
      next_matrix <- list_cluster_matrix[[i + n_month]]
      sum_matrix <- current_matrix + next_matrix
      sum_matrix <- sum_matrix == 2
      row_sums <- rowSums(sum_matrix)
      remain_ratio <- row_sums / rowSums(current_matrix)
      list_change[[i]] <- which(remain_ratio <= threshold)
    }
  }
  return(list_change)
}

mark_member <- function(customer, customers) {
  mark_array <- rep(0, length(customers))
  for(cus in customers) {
    if(cus@cluster == customer@cluster || cus@id == customer@id) {
      mark_array[cus@id] = 1
    }
  }
  return(mark_array)
}

change_between_month <- function(current_customers, next_customers, threshold) {
  list_cus_id_change <- c()
  for(i in 1:length(current_customers)) {
    current <- get_cus_by_id(i, current_customers)
    nextt <- get_cus_by_id(i, next_customers)
    
    current_mark <- mark_member(current, current_customers)
    nextt_mark <- mark_member(nextt, next_customers)
    sum_mark <- current_mark + nextt_mark
    remain <- sum(sum_mark == 2)
    remain_ratio <- remain / sum(current_mark)
    if(remain_ratio <= threshold) {
      list_cus_id_change <- c(list_cus_id_change, i)
    }
  }
  return(list_cus_id_change)
}

get_cus_by_id <- function(cus_id, customers) {
  for(cus in customers) {
    if(cus@id == cus_id) {
      return(cus)
    }
  }
  return(NULL)
}

#plot pca for each month
plot_pca_for_each_month <- function(month_cluster) {
  colors = c("red", "green", "blue", "purple", "black", "orange")
  cluster_colors <- c()
  clusters <- c()
  month_pca <- c()
  for(cus in month_cluster@customers) {
    clusters <- c(clusters, cus@cluster + 15)
    cluster_colors <- c(cluster_colors, colors[cus@cluster + 1])
    month_pca <- rbind(month_pca, cus@pca)
  }
  plot(as.data.frame(month_pca), col = cluster_colors, pch = clusters, cex = .5, font.lab = 2, family = "A", main = paste("Month", month_cluster@month))
  legend("topleft", cex = 0.7, legend = paste("Cluster ", sort(unique(clusters)) - 14, sep = " "), col = colors[sort(unique(clusters)) - 14], pch = clusters)
}

plot_core_pca_member <- function(month_cluster, cluster_id, non_core_member) {
  colors = c("red", "blue")
  month_pca <- c()
  core_pca_colors <- c()
  for(cus in month_cluster@customers) {
    if(cus@cluster == cluster_id) {
      if(is.na(match(cus@id, non_core_member))) {
        core_pca_colors <- c(core_pca_colors, colors[1])
      } else {
        core_pca_colors <- c(core_pca_colors, colors[2])
      }
      month_pca <- rbind(month_pca, cus@pca)
    }
    
  }
  plot(as.data.frame(month_pca), col = core_pca_colors, cex = .9)
  legend("topleft", legend = c("CORE MEMBER", "NON-CORE MEMBER"), col = colors, lty = 1:1, lwd = 2)
}

plot_core_pca <- function(month_cluster, non_core_member) {
  colors = c("red", "green", "blue", "purple", "black", "orange")
  cluster_colors <- c()
  clusters <- c()
  month_pca <- c()
  for(cus in month_cluster@customers) {
    if(is.na(match(cus@id, non_core_member))) {
      clusters <- c(clusters, cus@cluster + 15)
      cluster_colors <- c(cluster_colors, colors[cus@cluster + 1])
      month_pca <- rbind(month_pca, cus@pca)
    }
    
  }
  print(dim(month_pca))
  plot(as.data.frame(month_pca), col = cluster_colors, pch = clusters, cex =0.6)
  legend("topleft", legend = paste("Cluster ", unique(clusters) - 14, sep = " "), col = colors[unique(clusters) - 14], lty = 1:1, lwd = 2)
}

#plot clustering centers for each month in one plot
plot_centers_for_each_month <- function(month_cluster) {
  colors = c("red", "green", "blue", "purple", "black", "orange")
  centers <- get_all_centers_of_cluster(month_cluster)
  plot((month_cluster@month - 11) : month_cluster@month, centers[1, ], type = "l", col = colors[1], ylim = c(0, 1), xlab = "Month", ylab = "", 
       main = paste("Current Month is ", month_cluster@month, sep = ""), lwd = 2)
  for(i in 2:length(centers[, 1])) {
    lines(centers[i, ], col = colors[i], lwd = 2)
  }
  legend("topleft", legend = paste("Cluster: ", 1:length(centers[, 1])), col = colors[1:length(centers[, 1])], lty = 1:1, lwd = 2)
}

#plot clustering nearest neighbor for each cluster for each month
plot_nearest_member_with_center <- function(month_cluster, number_sample) {
  centers <- get_all_centers_of_cluster(month_cluster)
  n_clusters = month_cluster@n_clusters
  par(mfrow = c(3, 2))
  
  for(i in 0:(n_clusters - 1)) {
    plot_nearest_memeber_for_each_cluster(i, centers[i + 1, ], month_cluster, number_sample)
  }
  #mtext(paste("Month", month_cluster@month), outer=TRUE,  cex=1, line=-1, font = 2, family = "A")
}

plot_nearest_memeber_for_each_cluster <- function(cluster_id, cluster_center, month_cluster, number_sample) {
  si <- get_nearest_customer_with_center(cluster_id, month_cluster)
  if(si[1] != -1) {
    plot(cluster_center, col = "red", lwd = 4, ylim = c(0, 1), xlim = c(1, 12), type = "l", main = paste("Cluster ", cluster_id + 1), xlab = "", ylab = "", axes = F, font.lab = 2, family = "A")
    axis(1, family = 'A', font = 2)
    axis(2, family = 'A', font = 2)
    if(length(si) < number_sample){
      number_sample <- length(si);
    }
    for(k in 1:number_sample) {
      lines(get_customer_balance_by_id(month_cluster@customers, si[k])@balances, col = "gray66");
    }
    lines(cluster_center, col = "red", lwd = 4)
  }
  
}

plot_silhouette <- function(list_month) {
  sil <- c()
  for(i in 1:length(list_month)) {
    sil <- c(sil, list_month[[i]]@sil)
  }
  plot(sil, type = "l", lwd = 2, col = c("blue"), ylim = c(0.7, 1), ylab = "Silhouette", xlab = "Months", font.lab = 2, family = "A")
  axis(1, family = 'A', font = 2)
  axis(2, family = 'A', font = 2)
}


#core customer by one, 3, 6 and 12 months