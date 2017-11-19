package com.weebly
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object App {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Top_Paths")
      .set("spark.worker.cleanup.enabled", "true")
      .set("spark.driver.memory", "32g")
    val sc = new SparkContext(conf)
    val spark = SparkSessionSingleton.getInstance(sc.getConf)
    import spark.implicits._

    val input = sc.textFile("INPUT_FILE")
    val filterData = input
      .map { x => x.split('|') }
    val userGroupData = filterData
      .map { line =>
        (line(0), new TimeEvent(line(2), line(3), line(4), line(5), line(6), line(15)))
      }
      .groupByKey()
    userGroupData.count()
    val userSignup = userGroupData
      .map { x => sortTime(x._2) }
      .filter(x => x != null)
    userSignup.count()
    val userCombinations = userSignup
      .flatMap { x =>
        if (x.length < 5) x.combinations(x.length);
        else x.combinations(4)
      }

    var timeEvent1 = new TimeEvent("2017-10-03 02:13:30", "struct", "user", "signup_ok", "", "")
    var timeEvent2 = new TimeEvent("2017-10-24 22:09:27", "page_view", "", "", "", "www.weebly.com")
    var timeEvent3 = new TimeEvent("2017-10-15 11:07:44.98", "struct", "homepage", "view_ecommerce", "", "")
    var timeEvent4 = new TimeEvent("2017-10-15 11:07:58.999", "struct", "user", "login_ok", "", "")
    var test1 : Iterable[TimeEvent] = List(timeEvent1,timeEvent2,timeEvent3,timeEvent4)
    println(sortTime(test1))

    val pathCount = sc.parallelize(userCombinations
      .map { x =>
        (flatten(x), 1)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(1000))
    pathCount.map { x => PathCount(x._1, x._2) }
      .toDF.sort(desc("count"))
      .show(100, false)
    pathCount.coalesce(1, true)
      .saveAsTextFile("OUTPUT_FILE")
  }

  case class PathCount(Path:String, Count: Long)
  def flatten(input: Seq[String]) : String = {
    val path = StringBuilder.newBuilder
    for (elem <- input) {
      path.append("""""""+ elem + """" -> """)
    }
    if(path.length >= 4) {
      return path.substring(0,path.length-4).toString()
    } else {
      return ""
    }
  }

  class TimeEvent extends Ordered[TimeEvent] with Serializable {
    var timestamp: String = null
    var event: String = null
    var category: String = null
    var action: String = null
    var label: String = null
    var url: String = null

    def this(timestamp: String, event: String, category: String, action: String, label: String, url: String) {
      this();
      this.timestamp = timestamp
      this.event = event
      this.category = category
      this.action = action
      this.label = label
      this.url = url
    }

    def compare (that: TimeEvent) = {
      val thisParsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(this.timestamp)
      val thatParsed = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(that.timestamp)
      val thisTimestamp = new java.sql.Date(thisParsed.getTime())
      val thatTimestamp = new java.sql.Date(thatParsed.getTime())
      if (this.timestamp == that.timestamp)
        if(this.category == that.category)
          0
        else if (this.category > that.category)
          1
        else
          -1
      else if (this.timestamp > that.timestamp)
        1
      else
        -1
    }

    override def toString: String = {
      if(event contains("view")) {
        return timestamp + " " + event + " " + url
      } else {
        return timestamp + " " + category + " " + action + " " + label
      }
    }
  }

  def sortTime(input: Iterable[TimeEvent]) : Seq[String] = {
    val sortResult = input.toList.sorted[TimeEvent]
    var result = Seq[String]()
    var events = scala.collection.mutable.HashSet[String]()

    sortResult.foreach{ (x : TimeEvent) =>
      var event:String = null
      if(x.event contains "view") {
        event = x.event + " "+ x.url
      } else{
        event = x.category + " " + x.action + " " + x.label
      }

      if(!events(event)) {
        if(event.contains("signup_ok")) {
          return result
        }
        if(event.contains("login_ok")) {
          return null
        }
        events += event
        result = result :+ event
      }
    }
    return null
  }
}

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}