/**
  * Created by Ambition on 6/15/16.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.linalg.DenseVector
import org.apache.log4j.{Logger, Level}
import org.apache.spark.rdd._




object book {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Book_Recommend").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // (user,item,rating,timestamp)
    val data = sc
      .textFile("/Users/Ambition/Desktop/final_project/ratings_Books.csv")
      .map(_.split(","))
    var i = 0
    val usr = data
      .map{field =>
        i += 1
        (field(0), i)}
      .collect()
      .toMap
    val rating = data.map{ field =>
      Rating(usr(field(0)), field(1).toInt, field(2).toDouble)
    }.cache()
    val rank = 5
    val iter = 10
    val model_als = ALS.train(rating, rank, iter)

    val test = rating
      .map{case Rating(user, product, rate) =>  (user, product)}
      .cache()
    val pred = model_als
      .predict(test)
      .map{case Rating(user, product, rate) =>
        ((user, product), rate)}
    val rate_pred = rating
      .map{case Rating(user, product, rate) =>
        ((user, product), rate)}
      .join(pred)
    val mse = rate_pred
      .map{case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err}
      .mean()
    println("MSE = " + mse)


  }
}

// Exception in thread thread_name: java.lang.OutOfMemoryError: GC Overhead limit exceeded
// garbage collector is running all the time and Java program is making very slow progress. After a garbage collection, if the Java process is spending more than approximately 98% of its time doing garbage collection and if it is recovering less than 2% of the heap and has been doing so far the last 5 (compile time constant) consecutive garbage collections, then a java.lang.OutOfMemoryError is thrown