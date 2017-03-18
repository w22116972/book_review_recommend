/**
  * Created by Ambition on 6/16/16.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import java.util.Calendar
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.{Logger, Level}
import org.apache.spark.rdd._
object BX {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Book_Recommend").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val usr2Loc = sc
      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Users.csv")
      .map{_.split(";")}
      .map{attr =>
        (attr(0), attr(1))}
      .collect()
      .toMap
    val book2Name = sc
      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Books.csv")
      .map(_.split(";"))
      .map{attr =>
        (attr(0).split("\"")(1), attr(1).split("\"")(1))}
      .collect()
      .toMap

    var i = 0
    val bookMap = sc
      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Books.csv")
      .map(_.split(";"))
      .map{attr =>
        i += 1
        (attr(0), i)
      }
      .collect()
      .toMap
//    val s = "\"276747\"".split("\"")
    i = 0
    val bookId = sc
      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Book-Ratings.csv")
      .map(_.split(";"))
      .map { attr =>
        i += 1
        val book = attr(1).split("\"")(1)
        (book, i)
      }.collect().toMap
    i = 0
    val idBook = sc
      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Book-Ratings.csv")
      .map(_.split(";"))
      .map { attr =>
        i += 1
        val book = attr(1).split("\"")(1)
        (i, book)
      }.collect().toMap


    val rating = sc
      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Book-Ratings.csv")
      .map(_.split(";"))
      .map{attr =>
        val id = attr(0).split("\"")(1)
        val item = attr(1).split("\"")(1)
        val rate = attr(2).split("\"")(1)
        if (bookId.contains(item)) {
          Rating(id.toInt, bookId(item), rate.toDouble)
        }
        else {
          println("oops")
          Rating(id.toInt, 0, rate.toDouble)
        }
      }
      .cache()

    val improvement = (4.290268352234962 - 1.3252719578428072) / 4.290268352234962 * 100
    //println("Tuning iteration improves the default model by " + "%1.2f".format(improvement) + "%.")
    //var matrix = Array.ofDim[Double](278856, 582944)

/*
((10,0.01,20,-1),1.3192921338906438,63.95624542296529)
((11,1.0E-4,10),1.6655888814891318)
((11,1.0E-4,5),2.447961894970043)
((11,1.0,10),5.671993602264161)
((11,1.0E-4,20),1.3252719578428072)
((11,1.0E-6,20),1.440682594902883)
((11,1.0E-4,10),1.6331643708272123)
((11,1.0E-6,10),1.7820553760731273)
((11,1.0E-4,30),1.2297752422572399)
((11,1.0,20,-1),5.53831748751736,-122.86899221769987)
((11,0.01,1,-1),4.724946792597444,-29.394988451353495)
((11,1.0E-4,1,-1),4.290268352234962,-16.683407356690612)
block 20: 115691070910
block 10:  95523212344
block 5 : 107714440456
block -1:  63925225620
     [2]:  71880645939
     [8]:  77448430999
block 4:   62951545432
block 2:   73836873879


Don't Shoot the Dog! : The New Art of Teaching and Training
Variation on a Theme
Yogi: It Ain't over
Lost (Mira)
Stranger in a Strange Land
Touch of the Wolf
Star Searcher
*
* */

    val t0 = System.nanoTime()
    val eval =
      for (rank <- Array(11);
           lambda <- Array(0.01);
           iter <- Array(20);
           block <- Array(-1))
        yield {
          val model = ALS.train(rating, rank, iter, lambda, block)
          val test = rating
            .map{case Rating(user, product, rate) =>  (user, product)}
          val pred = model
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
          val meanRating = rating.map{_.rating}.mean()
          val baselineRmse = math.sqrt(pred.map{x => (meanRating - x._2) * (meanRating - x._2)}.mean())

          val recom_all = model.recommendUsersForProducts(5).foreach(println)

//          val recom = model.recommendProducts(87351, 5)
//          book2Name.filter{case(book, name) =>
//            val id = if (bookId.contains(book)) bookId(book) else "0"
//            recom.map(_.product).toSet.contains(id)
//          }.values.toArray.foreach(println)
//          val acc = rate_pred
//            .filter { case ((user, product), (r1, r2)) =>
//              r1 == r2}
//            .count()

          ((rank, lambda, iter, block), mse, (baselineRmse - mse) / baselineRmse * 100)
        }
    eval.sortBy(_._2).foreach(println)
    //
//
//
//    val t1 = System.nanoTime()
//    println("Execution time is " + "%d".format(t1 - t0))
//    val improvement = (8.8 - 5.671993602264161) / 8.8 * 100
//    println("The best model improves the default model by " + "%1.2f".format(improvement) + "%.")
//    // The ALS model improves the baseline model by 63.61%
//    // The best model improves the default model by 70.63%.
//    // If data preprocessed well, model improved by 35.55%
//    println("Defalut Parallel :" + "%d".format(sc.defaultParallelism))
//    println("Rating partition :" + "%d".format(rating.partitions.size))

    // Decision Tree
    import org.apache.spark.mllib.tree.DecisionTree
    import org.apache.spark.mllib.tree.model.DecisionTreeModel

    import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
    import org.apache.spark.mllib.linalg.{Matrix, DenseMatrix}

//    var matrix = Array.ofDim[Double](278856, 582944)
//    val entry = sc
//      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Book-Ratings.csv")
//      .map(_.split(";"))
//      .map{attr =>
//        val id = attr(0).split("\"")(1)
//        val item = attr(1).split("\"")(1)
//        val rate = attr(2).split("\"")(1)
//        matrix(id.toInt)(bookId(item).toInt) = rate.toDouble
//        MatrixEntry(id.toLong, bookId(item).toLong, rate.toDouble)}
//    val mat: CoordinateMatrix = new CoordinateMatrix(entry)
//    //println(mat.numCols() + " , " + mat.numRows())

    //val dist = mat
//    val value: Array[Double] = Array(
//    //row0 row1 row2 row3
//      1.0, 3.0, 5.0, 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0,    // col 0
//      2.0, 2.0, 0.0, 3.0, 2.0, 2.0, 3.0, 0.0, 0.0, 0.0,    // col 1
//      0.0, 5.0, 5.0, 5.0, 5.0, 4.0, 4.0, 8.0, 9.0, 10.0,   // col 2
//      6,0, 0.0, 0.0, 7.0, 8.0, 0.0, 8.0, 9.0, 10.0, 0.0,
//      0.0, 4.0, 10.0,9.0, 0.0, 9.0, 0.0, 2.0, 0.0
//      )
//    val m: DenseMatrix = new DenseMatrix(10, 5, value)
//    for (row <- 0 to 9;
//         col <- 0 to 4) {
//
//
//      println(m.apply(row, col))
//    }




    // Logistic Regression
//    import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
//    import org.apache.spark.mllib.evaluation.MulticlassMetrics
//    import org.apache.spark.mllib.regression.LabeledPoint
//
//    val data = sc
//      .textFile("/Users/Ambition/Desktop/final_project/BX-CSV-Dump/BX-Book-Ratings.csv")
//      .map{_.split(";")}
//      .map{attr =>
//        LabeledPoint(attr(2).split("\"")(1).toInt, Vectors.dense(attr(0).split("\"")(1).toDouble, bookId(attr(1).split("\"")(1)).toDouble) )
//      }
//      .randomSplit(Array(0.7, 0.3))
//
//    val train_log= data(0).cache()
//    val test_log = data(1)
//
//    val model_log = new LogisticRegressionWithLBFGS()
//      .setNumClasses(11)
//      .run(train_log)
//
//    val predLabel_log = test_log.map{case LabeledPoint(label, feat) =>
//      val pred = model_log.predict(feat)
//      (pred, label)
//    }
//    val acc_log = 1.0 * predLabel_log.filter(x => x._1 == x._2).count() / predLabel_log.count()
//    println("acc_log : " + "%1.2f".format(acc_log))
//    val mse_log = predLabel_log.filter(x => x._1 != x._2).map{ case (r1, r2) =>
//      val err = r1.toDouble - r2.toDouble
//        err * err
//    }.mean()
//    println("mse_log : " + "%1.2f".format(mse_log))



  }

}
