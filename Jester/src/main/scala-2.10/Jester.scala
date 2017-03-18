/**
  * Created by Ambition on 5/12/16.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}


object Jester {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Jester").setMaster("local[4]")
    val sc = new SparkContext(conf)
    var id = 0;
    //var i = 1;
    val data = sc.textFile("/Users/Ambition/Desktop/Jester/jester-data-*.csv")
      .map(_.split(","))
      .map{case field:Array[String] =>
          id += 1
          field(0) = id.toString
          field
      }

    //data.first().foreach(println)
    // RDD_i: RDD[Array[String]] = {id, rating1, ... rating100}
//    var seq = Seq(Rating(0, 0, 0.0))
//    data.foreach{ case field:Array[String] =>
//      for (i <- 1 to 100) {
//        if (field(i).toDouble != 99) {
//          seq = seq :+ Rating(field(0).toInt, i, field(i).toDouble + 10)
//          //println("y")
//        }
//      }
//    }


    // use ID 1 as my_rate
    val train = data.flatMap{ case field:Array[String] =>
      var seq = Seq(Rating(0, 0, 0.0))
      for (i <- 1 to 100) {
        if (field(i).toDouble != 99) {
          seq = seq :+ Rating(field(0).toInt, i, field(i).toDouble)
        }
      }
      seq
    }

    train.take(100).foreach(println)
    val model: MatrixFactorizationModel = ALS.train(train, 20, 10, 10.0)


    val test = sc.parallelize(Seq(Rating(0, 1, 5.0), Rating(0, 2, 7.5), Rating(0, 3, -3.5), Rating(0, 4, -4.4), Rating(0, 5, 9.9)))



    //filter.take(2)(0).foreach(println)
    // data := RDD[Array[String]]

//      .map{ line =>
//        val field = line.split(",")
//        id += 1
//        for (i <- 1 to 100) {
//          Rating(id, i, field(i).toFloat)
//        }
//
//      }
  }

}
