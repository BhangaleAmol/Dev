// Databricks notebook source
// MAGIC
// MAGIC %python
// MAGIC # ****************************************************************************
// MAGIC # Utility method to count & print the number of records in each partition.
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def printRecordsPerPartition(df):
// MAGIC   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
// MAGIC   results = (df.rdd                   # Convert to an RDD
// MAGIC     .mapPartitions(countInPartition)  # For each partition, count
// MAGIC     .collect()                        # Return the counts to the driver
// MAGIC   )
// MAGIC   
// MAGIC   print("Per-Partition Counts")
// MAGIC   i = 0
// MAGIC   for result in results: 
// MAGIC     i = i + 1
// MAGIC     print("#{}: {:,}".format(i, result))
// MAGIC   
// MAGIC # ****************************************************************************
// MAGIC # Utility to count the number of files in and size of a directory
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def computeFileStats(path):
// MAGIC   bytes = 0
// MAGIC   count = 0
// MAGIC
// MAGIC   files = dbutils.fs.ls(path)
// MAGIC   
// MAGIC   while (len(files) > 0):
// MAGIC     fileInfo = files.pop(0)
// MAGIC     if (fileInfo.isDir() == False):               # isDir() is a method on the fileInfo object
// MAGIC       count += 1
// MAGIC       bytes += fileInfo.size                      # size is a parameter on the fileInfo object
// MAGIC     else:
// MAGIC       files.extend(dbutils.fs.ls(fileInfo.path))  # append multiple object to files
// MAGIC       
// MAGIC   return (count, bytes)
// MAGIC
// MAGIC # ****************************************************************************
// MAGIC # Utility method to cache a table with a specific name
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def cacheAs(df, name, level = "MEMORY-ONLY"):
// MAGIC   from pyspark.sql.utils import AnalysisException
// MAGIC   if level != "MEMORY-ONLY":
// MAGIC     print("WARNING: The PySpark API currently does not allow specification of the storage level - using MEMORY-ONLY")  
// MAGIC     
// MAGIC   try: spark.catalog.uncacheTable(name)
// MAGIC   except AnalysisException: None
// MAGIC   
// MAGIC   df.createOrReplaceTempView(name)
// MAGIC   spark.catalog.cacheTable(name)
// MAGIC   
// MAGIC   return df
// MAGIC
// MAGIC
// MAGIC # ****************************************************************************
// MAGIC # Simplified benchmark of count()
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def benchmarkCount(func):
// MAGIC   import time
// MAGIC   start = float(time.time() * 1000)                    # Start the clock
// MAGIC   df = func()
// MAGIC   total = df.count()                                   # Count the records
// MAGIC   duration = float(time.time() * 1000) - start         # Stop the clock
// MAGIC   return (df, total, duration)
// MAGIC
// MAGIC # ****************************************************************************
// MAGIC # Utility methods to terminate streams
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def getActiveStreams():
// MAGIC   try:
// MAGIC     return spark.streams.active
// MAGIC   except:
// MAGIC     # In extream cases, this funtion may throw an ignorable error.
// MAGIC     print("Unable to iterate over all active streams - using an empty set instead.")
// MAGIC     return []
// MAGIC
// MAGIC def stopStream(s):
// MAGIC   try:
// MAGIC     print("Stopping the stream {}.".format(s.name))
// MAGIC     s.stop()
// MAGIC     print("The stream {} was stopped.".format(s.name))
// MAGIC   except:
// MAGIC     # In extream cases, this funtion may throw an ignorable error.
// MAGIC     print("An [ignorable] error has occured while stoping the stream.")
// MAGIC
// MAGIC def stopAllStreams():
// MAGIC   streams = getActiveStreams()
// MAGIC   while len(streams) > 0:
// MAGIC     stopStream(streams[0])
// MAGIC     streams = getActiveStreams()
// MAGIC     
// MAGIC # ****************************************************************************
// MAGIC # Utility method to wait until the stream is read
// MAGIC # ****************************************************************************
// MAGIC
// MAGIC def untilStreamIsReady(name, progressions=3):
// MAGIC   import time
// MAGIC   queries = list(filter(lambda query: query.name == name or query.name == name + "_p", getActiveStreams()))
// MAGIC
// MAGIC   while (len(queries) == 0 or len(queries[0].recentProgress) < progressions):
// MAGIC     time.sleep(5) # Give it a couple of seconds
// MAGIC     queries = list(filter(lambda query: query.name == name or query.name == name + "_p", getActiveStreams()))
// MAGIC
// MAGIC   print("The stream {} is active and ready.".format(name))

// COMMAND ----------

// ****************************************************************************
// Utility method to count & print the number of records in each partition.
// ****************************************************************************

def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
  // import org.apache.spark.sql.functions._
  val results = df.rdd                                   // Convert to an RDD
    .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
    .collect()                                           // Return the counts to the driver

  println("Per-Partition Counts")
  var i = 0
  for (r <- results) {
    i = i +1
    println("#%s: %,d".format(i,r))
  }
}

// ****************************************************************************
// Utility to count the number of files in and size of a directory
// ****************************************************************************

def computeFileStats(path:String):(Long,Long) = {
  var bytes = 0L
  var count = 0L

  import scala.collection.mutable.ArrayBuffer
  var files=ArrayBuffer(dbutils.fs.ls(path):_ *)

  while (files.isEmpty == false) {
    val fileInfo = files.remove(0)
    if (fileInfo.isDir == false) {
      count += 1
      bytes += fileInfo.size
    } else {
      files.append(dbutils.fs.ls(fileInfo.path):_ *)
    }
  }
  (count, bytes)
}

// ****************************************************************************
// Utility method to cache a table with a specific name
// ****************************************************************************

def cacheAs(df:org.apache.spark.sql.DataFrame, name:String, level:org.apache.spark.storage.StorageLevel):org.apache.spark.sql.DataFrame = {
  try spark.catalog.uncacheTable(name)
  catch { case _: org.apache.spark.sql.AnalysisException => () }
  
  df.createOrReplaceTempView(name)
  spark.catalog.cacheTable(name, level)
  return df
}

// ****************************************************************************
// Simplified benchmark of count()
// ****************************************************************************

def benchmarkCount(func:() => org.apache.spark.sql.DataFrame):(org.apache.spark.sql.DataFrame, Long, Long) = {
  val start = System.currentTimeMillis            // Start the clock
  val df = func()                                 // Get our lambda
  val total = df.count()                          // Count the records
  val duration = System.currentTimeMillis - start // Stop the clock
  (df, total, duration)
}

// ****************************************************************************
// Benchmarking and cache tracking tool
// ****************************************************************************

case class JobResults[T](runtime:Long, duration:Long, cacheSize:Long, maxCacheBefore:Long, remCacheBefore:Long, maxCacheAfter:Long, remCacheAfter:Long, result:T) {
  def printTime():Unit = {
    if (runtime < 1000)                 println(f"Runtime:  ${runtime}%,d ms")
    else if (runtime < 60 * 1000)       println(f"Runtime:  ${runtime/1000.0}%,.2f sec")
    else if (runtime < 60 * 60 * 1000)  println(f"Runtime:  ${runtime/1000.0/60.0}%,.2f min")
    else                                println(f"Runtime:  ${runtime/1000.0/60.0/60.0}%,.2f hr")
    
    if (duration < 1000)                println(f"All Jobs: ${duration}%,d ms")
    else if (duration < 60 * 1000)      println(f"All Jobs: ${duration/1000.0}%,.2f sec")
    else if (duration < 60 * 60 * 1000) println(f"All Jobs: ${duration/1000.0/60.0}%,.2f min")
    else                                println(f"Job Dur: ${duration/1000.0/60.0/60.0}%,.2f hr")
  }
  def printCache():Unit = {
    if (Math.abs(cacheSize) < 1024)                    println(f"Cached:   ${cacheSize}%,d bytes")
    else if (Math.abs(cacheSize) < 1024 * 1024)        println(f"Cached:   ${cacheSize/1024.0}%,.3f KB")
    else if (Math.abs(cacheSize) < 1024 * 1024 * 1024) println(f"Cached:   ${cacheSize/1024.0/1024.0}%,.3f MB")
    else                                               println(f"Cached:   ${cacheSize/1024.0/1024.0/1024.0}%,.3f GB")
    
    println(f"Before:   ${remCacheBefore / 1024.0 / 1024.0}%,.3f / ${maxCacheBefore / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheBefore/maxCacheBefore}%.2f%%")
    println(f"After:    ${remCacheAfter / 1024.0 / 1024.0}%,.3f / ${maxCacheAfter / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheAfter/maxCacheAfter}%.2f%%")
  }
  def print():Unit = {
    printTime()
    printCache()
  }
}

case class Node(driver:Boolean, executor:Boolean, address:String, maximum:Long, available:Long) {
  def this(address:String, maximum:Long, available:Long) = this(address.contains("-"), !address.contains("-"), address, maximum, available)
}

class Tracker() extends org.apache.spark.scheduler.SparkListener() {
  
  sc.addSparkListener(this)
  
  val jobStarts = scala.collection.mutable.Map[Int,Long]()
  val jobEnds = scala.collection.mutable.Map[Int,Long]()
  
  def track[T](func:() => T):JobResults[T] = {
    jobEnds.clear()
    jobStarts.clear()

    val executorsBefore = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
    val maxCacheBefore = executorsBefore.map(_.maximum).sum
    val remCacheBefore = executorsBefore.map(_.available).sum
    
    val start = System.currentTimeMillis()
    val result = func()
    val runtime = System.currentTimeMillis() - start
    
    Thread.sleep(1000) // give it a second to catch up

    val executorsAfter = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
    val maxCacheAfter = executorsAfter.map(_.maximum).sum
    val remCacheAfter = executorsAfter.map(_.available).sum

    var duration = 0L
    
    for ((jobId, startAt) <- jobStarts) {
      assert(jobEnds.keySet.exists(_ == jobId), s"A conclusion for Job ID $jobId was not found.") 
      duration += jobEnds(jobId) - startAt
    }
    JobResults(runtime, duration, remCacheBefore-remCacheAfter, maxCacheBefore, remCacheBefore, maxCacheAfter, remCacheAfter, result)
  }
  override def onJobStart(jobStart: org.apache.spark.scheduler.SparkListenerJobStart):Unit = jobStarts.put(jobStart.jobId, jobStart.time)
  override def onJobEnd(jobEnd: org.apache.spark.scheduler.SparkListenerJobEnd): Unit = jobEnds.put(jobEnd.jobId, jobEnd.time)
}

val tracker = new Tracker()

// ****************************************************************************
// Utility methods to terminate streams
// ****************************************************************************

def getActiveStreams():Seq[org.apache.spark.sql.streaming.StreamingQuery] = {
  return try {
    spark.streams.active
  } catch {
    case e:Throwable => {
      // In extream cases, this funtion may throw an ignorable error.
      println("Unable to iterate over all active streams - using an empty set instead.")
      Seq[org.apache.spark.sql.streaming.StreamingQuery]()
    }
  }
}

def stopStream(s:org.apache.spark.sql.streaming.StreamingQuery):Unit = {
  try {
    s.stop()
  } catch {
    case e:Throwable => {
      // In extream cases, this funtion may throw an ignorable error.
      println(s"An [ignorable] error has occured while stoping the stream.")
    }
  }
}

def stopAllStreams():Unit = {
  var streams = getActiveStreams()
  while (streams.length > 0) {
    stopStream(streams(0))
    streams = getActiveStreams()
  }
}

// ****************************************************************************
// Utility method to wait until the stream is read
// ****************************************************************************

def untilStreamIsReady(name:String, progressions:Int = 3):Unit = {
  var queries = getActiveStreams().filter(s => s.name == name || s.name == name + "_s")
  
  while (queries.length == 0 || queries(0).recentProgress.length < progressions) {
    Thread.sleep(5*1000) // Give it a couple of seconds
    queries = getActiveStreams().filter(s => s.name == name || s.name == name + "_s")
  }
  println("The stream %s is active and ready.".format(name))
}

//**********************************
// CREATE THE MOUNTS
//**********************************

def getAwsRegion():String = {
  try {
    import scala.io.Source
    import scala.util.parsing.json._

    val jsonString = Source.fromURL("http://169.254.169.254/latest/dynamic/instance-identity/document").mkString // reports ec2 info
    val map = JSON.parseFull(jsonString).getOrElse(null).asInstanceOf[Map[Any,Any]]
    map.getOrElse("region", null).asInstanceOf[String]

  } catch {
    // We will use this later to know if we are Amazon vs Azure
    case _: java.io.FileNotFoundException => null
  }
}

def getAzureRegion():String = {
  import com.databricks.backend.common.util.Project
  import com.databricks.conf.trusted.ProjectConf
  import com.databricks.backend.daemon.driver.DriverConf

  new DriverConf(ProjectConf.loadLocalConfig(Project.Driver)).region
}

// These keys are read-only so they're okay to have here
val awsAccessKey = "AKIAJBRYNXGHORDHZB4A"
val awsSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
val awsAuth = s"${awsAccessKey}:${awsSecretKey}"

def getAwsMapping(region:String):(String,Map[String,String]) = {

  val MAPPINGS = Map(
    "ap-northeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-1/common", Map[String,String]()),
    "ap-northeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-2/common", Map[String,String]()),
    "ap-south-1"     -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-south-1/common", Map[String,String]()),
    "ap-southeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-1/common", Map[String,String]()),
    "ap-southeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-2/common", Map[String,String]()),
    "ca-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ca-central-1/common", Map[String,String]()),
    "eu-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
    "eu-west-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-1/common", Map[String,String]()),
    "eu-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-2/common", Map[String,String]()),

    // eu-west-3 in Paris isn't supported by Databricks yet - not supported by the current version of the AWS library
    // "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-3/common", Map[String,String]()),
    
    // Use Frankfurt in EU-Central-1 instead
    "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
    
    "sa-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-sa-east-1/common", Map[String,String]()),
    "us-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-1/common", Map[String,String]()),
    "us-east-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-2/common", Map[String,String]()),
    "us-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]()),
    "_default"       -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]())
  )

  MAPPINGS.getOrElse(region, MAPPINGS("_default"))
}

def getAzureMapping(region:String):(String,Map[String,String]) = {

  var MAPPINGS = Map(
    "australiacentral"    -> ("dbtrainaustraliasoutheas",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiacentral2"   -> ("dbtrainaustraliasoutheas",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiaeast"       -> ("dbtrainaustraliaeast",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=FM6dy59nmw3f4cfN%2BvB1cJXVIVz5069zHmrda5gZGtU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "australiasoutheast"  -> ("dbtrainaustraliasoutheas",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=br8%2B5q2ZI9osspeuPtd3haaXngnuWPnZaHKFoLmr370%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "canadacentral"       -> ("dbtraincanadacentral",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=dwAT0CusWjvkzcKIukVnmFPTmi4JKlHuGh9GEx3OmXI%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "canadaeast"          -> ("dbtraincanadaeast",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SYmfKBkbjX7uNDnbSNZzxeoj%2B47PPa8rnxIuPjxbmgk%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "centralindia"        -> ("dbtraincentralindia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=afrYm3P5%2BB4gMg%2BKeNZf9uvUQ8Apc3T%2Bi91fo/WOZ7E%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "centralus"           -> ("dbtraincentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=As9fvIlVMohuIV8BjlBVAKPv3C/xzMRYR1JAOB%2Bbq%2BQ%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastasia"            -> ("dbtraineastasia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=sK7g5pki8bE88gEEsrh02VGnm9UDlm55zTfjZ5YXVMc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastus"              -> ("dbtraineastus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=tlw5PMp1DMeyyBGTgZwTbA0IJjEm83TcCAu08jCnZUo%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "eastus2"             -> ("dbtraineastus2",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=Y6nGRjkVj6DnX5xWfevI6%2BUtt9dH/tKPNYxk3CNCb5A%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "japaneast"           -> ("dbtrainjapaneast",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=q6r9MS/PC9KLZ3SMFVYO94%2BfM5lDbAyVsIsbBKEnW6Y%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "japanwest"           -> ("dbtrainjapanwest",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=M7ic7/jOsg/oiaXfo8301Q3pt9OyTMYLO8wZ4q8bko8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northcentralus"      -> ("dbtrainnorthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northcentralus"      -> ("dbtrainnorthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=GTLU0g3pajgz4dpGUhOpJHBk3CcbCMkKT8wxlhLDFf8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "northeurope"         -> ("dbtrainnortheurope",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=35yfsQBGeddr%2BcruYlQfSasXdGqJT3KrjiirN/a3dM8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southcentralus"      -> ("dbtrainsouthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southcentralus"      -> ("dbtrainsouthcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=3cnVg/lzWMx5XGz%2BU4wwUqYHU5abJdmfMdWUh874Grc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southindia"          -> ("dbtrainsouthindia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=0X0Ha9nFBq8qkXEO0%2BXd%2B2IwPpCGZrS97U4NrYctEC4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "southeastasia"       -> ("dbtrainsoutheastasia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=H7Dxi1yqU776htlJHbXd9pdnI35NrFFsPVA50yRC9U0%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "uksouth"             -> ("dbtrainuksouth",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=SPAI6IZXmm%2By/WMSiiFVxp1nJWzKjbBxNc5JHUz1d1g%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "ukwest"              -> ("dbtrainukwest",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=olF4rjQ7V41NqWRoK36jZUqzDBz3EsyC6Zgw0QWo0A8%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westcentralus"       -> ("dbtrainwestcentralus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=UP0uTNZKMCG17IJgJURmL9Fttj2ujegj%2BrFN%2B0OszUE%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westeurope"          -> ("dbtrainwesteurope",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=csG7jGsNFTwCArDlsaEcU4ZUJFNLgr//VZl%2BhdSgEuU%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westindia"           -> ("dbtrainwestindia",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=fI6PNZ7YvDGKjArs1Et2rAM2zgg6r/bsKEjnzQxgGfA%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westus"              -> ("dbtrainwestus",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=%2B1XZDXbZqnL8tOVsmRtWTH/vbDAKzih5ThvFSZMa3Tc%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "westus2"             -> ("dbtrainwestus2",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"),
    "_default"            -> ("dbtrainwestus2",
                              "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=DD%2BO%2BeIZ35MO8fnh/fk4aqwbne3MAJ9xh9aCIU/HiD4%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z")
  )

  val (account: String, sasKey: String) = MAPPINGS.getOrElse(region, MAPPINGS("_default"))

  val blob = "training"
  val source = s"wasbs://$blob@$account.blob.core.windows.net/"
  val configMap = Map(
    s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
  )

  (source, configMap)
}

def mountFailed(msg:String): Unit = {
  println(msg)
}

def retryMount(source: String, mountPoint: String): Unit = {
  try { 
    // Mount with IAM roles instead of keys for PVC
    dbutils.fs.mount(source, mountPoint)
  } catch {
    case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
  }
}

def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
  try {
    dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
  } catch {
    case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
    case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
  }
}

def autoMount(fix:Boolean = false, failFast:Boolean = false, mountDir:String = "/mnt/training"): String = {
  var awsRegion = getAwsRegion()

  val (source, extraConfigs) = if (awsRegion != null)  {
    spark.conf.set("com.databricks.training.region.name", awsRegion)
    getAwsMapping(awsRegion)

  } else {
    val azureRegion = getAzureRegion()
    spark.conf.set("com.databricks.training.region.name", azureRegion)
    initAzureDataSource(azureRegion)
  }
  
  val resultMsg = mountSource(fix, failFast, mountDir, source, extraConfigs)
  resultMsg
}

def initAzureDataSource(azureRegion:String):(String,Map[String,String]) = {
  val mapping = getAzureMapping(azureRegion)
  val (source, config) = mapping
  val (sasEntity, sasToken) = config.head

  val datasource = "%s\t%s\t%s".format(source, sasEntity, sasToken)
  spark.conf.set("com.databricks.training.azure.datasource", datasource)

  return mapping
}

def mountSource(fix:Boolean, failFast:Boolean, mountDir:String, source:String, extraConfigs:Map[String,String]): String = {
  val mntSource = source.replace(awsAuth+"@", "")

  if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
    val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
    if (mount.source == mntSource) {
      return s"""Datasets are already mounted to <b>$mountDir</b> from <b>$mntSource</b>"""
      
    } else if (failFast) {
      throw new IllegalStateException(s"Expected $mntSource but found ${mount.source}")
      
    } else if (fix) {
      println(s"Unmounting existing datasets ($mountDir from $mntSource)")
      dbutils.fs.unmount(mountDir)
      mountSource(fix, failFast, mountDir, source, extraConfigs)

    } else {
      return s"""<b style="color:red">Invalid Mounts!</b></br>
                      <ul>
                      <li>The training datasets you are using are from an unexpected source</li>
                      <li>Expected <b>$mntSource</b> but found <b>${mount.source}</b></li>
                      <li>Failure to address this issue may result in significant performance degradation. To address this issue:</li>
                      <ol>
                        <li>Insert a new cell after this one</li>
                        <li>In that new cell, run the command <code style="color:blue; font-weight:bold">%scala fixMounts()</code></li>
                        <li>Verify that the problem has been resolved.</li>
                      </ol>"""
    }
  } else {
    println(s"""Mounting datasets to $mountDir from $mntSource""")
    mount(source, extraConfigs, mountDir)
    return s"""Mounted datasets to <b>$mountDir</b> from <b>$mntSource<b>"""
  }
}

def fixMounts(): Unit = {
  autoMount(true)
}

val resultMsg = autoMount(true)

displayHTML("Datasets mounted and student environment set up")
