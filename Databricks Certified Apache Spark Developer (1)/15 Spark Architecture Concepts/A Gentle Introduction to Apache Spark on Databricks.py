# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Apache Spark Architecture
# MAGIC
# MAGIC Before proceeding with our example, let's see an overview of the Apache Spark architecture. As mentioned before, Apache Spark allows you to treat many machines as one machine and this is done via a master-worker type architecture where there is a `driver` or master node in the cluster, accompanied by `worker` nodes. The master sends work to the workers and either instructs them to pull to data from memory or from disk (or from another data source like S3 or Redshift).
# MAGIC
# MAGIC The diagram below shows an example Apache Spark cluster, basically there exists a Driver node that communicates with executor nodes. Each of these executor nodes have slots which are logically like execution cores. 
# MAGIC
# MAGIC ![spark-architecture](http://training.databricks.com/databricks_guide/gentle_introduction/videoss_logo.png)
# MAGIC
# MAGIC The Driver sends Tasks to the empty slots on the Executors when work has to be done:
# MAGIC
# MAGIC ![spark-architecture](http://training.databricks.com/databricks_guide/gentle_introduction/spark_cluster_tasks.png)
# MAGIC
# MAGIC Note: In the case of the Community Edition there is no Worker, and the Master, not shown in the figure, executes the entire code.
# MAGIC
# MAGIC ![spark-architecture](http://training.databricks.com/databricks_guide/gentle_introduction/notebook_microcluster.png)
# MAGIC
# MAGIC You can view the details of your Apache Spark application in the Apache Spark web UI.  The web UI is accessible in Databricks by going to "Clusters" and then clicking on the "View Spark UI" link for your cluster, it is also available by clicking at the top left of this notebook where you would select the cluster to attach this notebook to. In this option will be a link to the Apache Spark Web UI.
# MAGIC
# MAGIC At a high level, every Apache Spark application consists of a driver program that launches various parallel operations on executor Java Virtual Machines (JVMs) running either in a cluster or locally on the same machine. In Databricks, the notebook interface is the driver program.  This driver program contains the main loop for the program and creates distributed datasets on the cluster, then applies operations (transformations & actions) to those datasets.
# MAGIC Driver programs access Apache Spark through a `SparkSession` object regardless of deployment location.
