

object CFApp{
  def main(args:Array[String]){

    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming Collaborative Filtering")
    val ssc = new StreamingContext(conf, Seconds(3))
    val rddQueue = new SynchronizedQueue[RDD[MatrixEntry]]()
//    val rddQueue1 = new SynchronizedQueue[RDD[test]]()
    val trainingData = ssc.sparkContext.textFile("/Users/lxb/Desktop/data100.txt")
 //   val testData = ssc.sparkContext.textFile("/Users/lxb/Desktop/newdata.txt")
    val ratings = trainingData.map(_.split("\t").map(_.toInt)).map(i => (i(0) - 1, i(1) - 1, i(2).toDouble))

   // var p = 0
    val dataWithIndex = trainingData.zipWithIndex()
 //   val testWithIndex = testData.zipWithIndex()
    val inputStream = ssc.queueStream(rddQueue)
  //  val testStream = ssc.queueStream(rddQueue1)
    val model = new CollabFilter()
    var res = Array.fill(50)(0.0)
    model.train(inputStream)

  //  model.predictOn(testStream)
    ssc.start()
      for (p <- 0 to 35) {
        val newRDD = dataWithIndex.filter(i => i._2 < (p + 1) * 40 && i._2 >= p * 40).map(x => x._1).map(_.split("\t").map(_.toInt))
        val newRDD1 = newRDD.map(i => new MatrixEntry((i(0) - 1).toLong, (i(1) - 1).toLong, i(2).toDouble))
        rddQueue += newRDD1
        if(p > 3) {
          println(p + "loop's situation is: ------------------")
          res(p) = model.RMSE(ratings)

        }

        Thread.sleep(1000)

     }
//      val newRDDTest = testWithIndex.filter(i => i._2 < 1 * 20 && i._2 >= 0).map(x => x._1).map(_.split("\t").map(_.toInt))
//      val newRDDTest1 = newRDDTest.map(i => new test((i(0) - 1), (i(1) - 1)))
//      rddQueue1 += newRDDTest1
//
//
      res.foreach(println)
     // model.judge()
      ssc.stop()
  }
}
