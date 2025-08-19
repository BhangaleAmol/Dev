# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Let us copy the required data sets used for this course by using `databricks fs`.
# MAGIC * Make sure **Databricks CLI** is installed and configured with token.
# MAGIC * Run this command to validate you have access to the environment you are working with - `databricks fs ls`
# MAGIC * Create a folder by name **dbfs:/public** using `databricks fs mkdirs dbfs:/public`.
# MAGIC
# MAGIC Here are the instructions to setup **retail_db** data set under **dbfs:/public**.
# MAGIC * Clone the GitHub repository by using `git clone https//github.com/dgadiraju/retail_db.git`.
# MAGIC * Make sure to get into the folder and remove **.git** folder by using `rm -rf .git`. If you are using Windows you can use **File Explorer** to delete the folder.
# MAGIC * Run the below command to copy the **retail_db** folder into **dbfs:/public**. Make sure to provide fully qualified path.
# MAGIC
# MAGIC ```
# MAGIC databricks fs cp /Users/itversity/retail_db dbfs:/public/retail_db --recursive
# MAGIC ```
# MAGIC * Use the below command to validate. It should return details related to 6 folders and files.
# MAGIC
# MAGIC ```
# MAGIC databricks fs ls dbfs:/public/retail_db
# MAGIC ```
# MAGIC
# MAGIC Follow the similar instructions to setup **retail_db_json** data set under **dbfs:/public** using this git repo - `https//github.com/itversity/retail_db_json.git` 

# COMMAND ----------

fs ls
