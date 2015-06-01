
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import collection.mutable.HashMap
import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark._
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
//import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
case class Rating(user: Int, product: Int, rating: Double)

case class coordinate(var x: Int, var y: Int)
//case class LatentFactor(bias:Array[Array[Double]])

case class PerBlock(rows: Int = 10, cols: Int =10)

case class block(var rIndex: Int, var cIndex: Int, bloM: Array[Array[Double]])
case class test(user: Int, item: Int)

class CollabFilterModel(
                        var blockMat: RDD[((Int, Int), Array[Array[Double]])],
         //               val numUsers: Int,
         //               val numProducts: Int,
         //               val kValues: Int,
         //               val stepSize: Double,
         //               val lambda: Double,
         //               val features: Int,
                        var U: Array[Array[Double]],
                        var V: Array[Array[Double]],
                        var flagU: Array[Boolean],
                        var flagV: Array[Boolean],
                        var blockMatFlag: Array[Array[Int]],
                        var blockMatrixFlagUpdate: Array[Array[Int]],
                        var availableBlock: Array[coordinate],
                        var firstLoop: Int
           //             var blockMatrixFlagUpdate: DenseMatrix
           //               val seed: Int
          //              val rowsPerBlock: Int,
          //              val colsPerBlock: Int
                         ) {
  private val numUsers = 100
  private val numProducts = 100
  private val kValues = 10
  private val stepSize = 0.01
  private val lambda = 0.1
  private val rowsPerBlock = 10
  private val colsPerBlock = 10
  private val seed = 42
  private val iterate = 20

  private val  numPartForRow= numUsers / rowsPerBlock
  private val numPartForCol = numProducts / colsPerBlock

  private val numAvailableBlock = 10


  def setFirstLoop(x: Int): Unit ={
    this.firstLoop = x
  }

  def selectBlock(): Unit = {
    var count: Int = 0
    var tmpU = flagU.clone()
    var tmpV = flagV.clone()
    var num = 0
    for (t <- 0 to numPartForRow - 1) {
        for (j <- 0 to numPartForCol - 1 if blockMatrixFlagUpdate(t)(j) == 1
          && tmpU(t) == true && tmpV(j) == true ) {
            availableBlock(num) = coordinate(t, j)
            tmpU(t) = false
            tmpV(j) = false
            blockMatrixFlagUpdate(t)(j) = 0
            num += 1
        }
      }
  }
  def checkWhenEnd(): Boolean = {
    blockMatrixFlagUpdate.map(x =>x.mkString(",")).foreach(println)
    for (t <- 0 to numPartForRow - 1) {
      for (j <- 0 to numPartForCol - 1) {
        if (blockMatrixFlagUpdate(t)(j) == 1) {
          return false
        }
      }
    }
    true
  }


  def update(data: RDD[MatrixEntry], timeUnit: String): CollabFilterModel = {
    val sc = data.sparkContext
    val rand = new XORShiftRandom(seed)
    blockMatrixFlagUpdate = Array.fill(numPartForRow)(Array.fill(numPartForCol)(0))
    if (U == null) {
      U = Array.fill(numUsers)(Array.fill(kValues)(rand.nextDouble()))
    }

    if (V == null) {
      V = Array.fill(numProducts)(Array.fill(kValues)(rand.nextDouble()))
    }

    if (flagU == null) {
      flagU = Array.fill(numPartForRow)(false)
    }

    if (flagV == null) {
      flagV = Array.fill(numPartForCol)(false)
    }

    if (blockMatFlag == null) {
      blockMatFlag = Array.fill(numPartForRow)(Array.fill(numPartForCol)(0))
    }

    if (blockMat == null) {
      var tmp = new Array[((Int, Int), (Array[Array[Double]]))](100)
      for (t <- 0 to 99) {
        tmp(t) = (((t / 10).toInt, t % 10), Array.fill(10)(Array.fill(10)(0.0)))
      }
      blockMat = sc.parallelize(tmp)
    }


    val dataCol = data.collect()
    dataCol.foreach { entry =>
      val col = (entry.j / colsPerBlock).toInt
      val row = (entry.i / rowsPerBlock).toInt
      blockMatFlag(row)(col) = 1
      flagU(row) = true
      flagV(col) = true
    }

    val rowsPerBlock_br = sc.broadcast(rowsPerBlock).value
    val colsPerBlock_br = sc.broadcast(colsPerBlock).value
    val kValues_br = sc.broadcast(kValues).value
    val stepSize_br = sc.broadcast(stepSize).value
    val lambda_br = sc.broadcast(lambda).value
    val numProducts_br = sc.broadcast(numProducts).value
    val numUsers_br = sc.broadcast(numUsers).value

    val mark = data.map(x => (((x.i / rowsPerBlock_br).toInt, (x.j / colsPerBlock_br).toInt), (x.i.toInt, x.j.toInt, x.value)))
    val mark_br = sc.broadcast(mark.collect()).value

    val tmpblockMat = blockMat.map {
      case ((blockRowIndex, blockColIndex), matrix) =>
        for (i <- 0 to mark_br.length - 1 if mark_br(i)._1 ==(blockRowIndex, blockColIndex)) {
          val effrow = (mark_br(i)._2._1 - blockRowIndex * rowsPerBlock_br).toInt
          val effcol = (mark_br(i)._2._2 - blockColIndex * colsPerBlock_br).toInt
          matrix(effrow)(effcol) = mark_br(i)._2._3
        }
        ((blockRowIndex, blockColIndex), matrix)
    }
    blockMat = tmpblockMat
    var l = 0
    while (l < 10) {
      blockMatrixFlagUpdate(l) = blockMatFlag(l).clone()
      l += 1
    }
    while (!checkWhenEnd()) {

      val U_br = sc.broadcast(U).value
      val V_br = sc.broadcast(V).value
      availableBlock = Array.fill(numAvailableBlock)(new coordinate(20, 20))
      selectBlock()
      val availableBlock_br = sc.broadcast(availableBlock).value
      val RddNew = blockMat.filter(entry => CollabFilterModel.Filter(entry, availableBlock_br)).flatMap {
        case ((blockRowIndex, blockColIndex), matrix) =>
        val elements = for (i <- 0 to rowsPerBlock_br - 1) yield {
          val single = for (j <- 0 to colsPerBlock_br - 1 if matrix(i)(j).toInt != 0) yield {
            val uIndex = blockRowIndex * rowsPerBlock_br + i
            val vIndex = blockColIndex * colsPerBlock_br + j

            val res = CollabFilterModel.SGD(matrix(i)(j), uIndex, vIndex, blockRowIndex, blockColIndex,
              U_br, V_br, kValues_br, stepSize_br, lambda_br, numProducts_br, numUsers_br)

            U_br(res._1) = res._3
            V_br(res._2) = res._4
            (res._1, res._2, res._3, res._4)
          }
          single
        }
        elements.flatMap(i => i)
      }
      val updateInforForU = RddNew.map { case (uIndex, vIndex, u_new, v_new) =>
        (uIndex, Array((vIndex, u_new)))
      }.reduceByKey(_ ++: _).map(data => {
        val part2 = data._2
        val len = part2.length
        val tmp = part2.map(d => d._1)
        val Max = tmp.max
        val ind = tmp.indexOf(Max)
        val unew = part2(ind)._2
        (data._1, Max, unew)
      })

      val localUpdU = updateInforForU.collect()
      val updateInforForV = RddNew.map { case (uIndex, vIndex, u_new, v_new) =>
        (vIndex, Array((uIndex, v_new)))
      }.reduceByKey(_ ++: _).map(data => {
        val part2 = data._2
        val len = part2.length
        val tmp = part2.map(d => d._1)
        val Max = tmp.max
        val ind = tmp.indexOf(Max)
        val vnew = part2(ind)._2
        (data._1, Max, vnew)
      })

      val localUpdV = updateInforForV.collect()

      localUpdU.foreach { case (uInd, max, unew) =>
        U(uInd) = unew
      }

      localUpdV.foreach { case (vInd, max, vnew) =>
        V(vInd) = vnew
      }
    }




  this
  }

  def RMSE(ratings: RDD[(Int, Int, Double)]): Double ={
    var result = Array.fill(10000)((0, 0, 0.0))
    for(i <- 0 to 9999){
      val row = i / 100
      val col = i % 100
        result(row + col) = (row, col, multiply(row, col))

    }

    val sc = ratings.sparkContext
    val pred = sc.parallelize(result).map{case(user, item, rate) => ((user, item), rate)}
    val ratesAndPreds = ratings.map { case (user, product, rate) =>
      ((user, product), rate)
    }.join(pred)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)
    RMSE

  }


  def repeat(sc: SparkContext): Unit = {
    if (firstLoop == 1) {


      val rowsPerBlock_br = sc.broadcast(rowsPerBlock).value
      val colsPerBlock_br = sc.broadcast(colsPerBlock).value
      val kValues_br = sc.broadcast(kValues).value
      val stepSize_br = sc.broadcast(stepSize).value
      val lambda_br = sc.broadcast(lambda).value
      val numProducts_br = sc.broadcast(numProducts).value
      val numUsers_br = sc.broadcast(numUsers).value

      for (i <- 0 to 1) {
        blockMatrixFlagUpdate = Array.fill(numPartForRow)(Array.fill(numPartForCol)(0))

        for (l <- 0 to 9) {
          blockMatrixFlagUpdate(l) = blockMatFlag(l).clone()
        }
        while (!checkWhenEnd()) {

          val U_br = sc.broadcast(U).value
          val V_br = sc.broadcast(V).value
          availableBlock = Array.fill(numAvailableBlock)(new coordinate(20, 20))
          selectBlock()
          println("available block is:                         ")
          availableBlock.foreach(println)
          val availableBlock_br = sc.broadcast(availableBlock).value
          val RddNew = blockMat.filter(entry => CollabFilterModel.Filter(entry, availableBlock_br)).flatMap { case ((blockRowIndex, blockColIndex), matrix) => //filter needs fix
            val elements = for (i <- 0 to rowsPerBlock_br - 1) yield {
              val single = for (j <- 0 to colsPerBlock_br - 1 if matrix(i)(j).toInt != 0) yield {
                val uIndex = blockRowIndex * rowsPerBlock_br + i
                val vIndex = blockColIndex * colsPerBlock_br + j //need fix
                val res = CollabFilterModel.SGD(matrix(i)(j), uIndex, vIndex, blockRowIndex, blockColIndex,
                    U_br, V_br, kValues_br, stepSize_br, lambda_br, numProducts_br, numUsers_br) // needs fix
                println("after SGD, the result is : ")
                println(res._1, res._2)
                res._3.mkString(",").foreach(print)
                println(" ")
                res._4.mkString(",").foreach(print) //fixed
                U_br(res._1) = res._3
                V_br(res._2) = res._4
                (res._1, res._2, res._3, res._4)
              }
              single
            }
            elements.flatMap(i => i)
          }
          val updateInforForU = RddNew.map { case (uIndex, vIndex, u_new, v_new) =>
            (uIndex, Array((vIndex, u_new)))
          }.reduceByKey(_ ++: _).map(data => {
            val part2 = data._2
            val len = part2.length
            val tmp = part2.map(d => d._1)
            val Max = tmp.max
            val ind = tmp.indexOf(Max)
            val unew = part2(ind)._2
            (data._1, Max, unew)
          })
          val localUpdU = updateInforForU.collect()

          val updateInforForV = RddNew.map { case (uIndex, vIndex, u_new, v_new) =>
            (vIndex, Array((uIndex, v_new)))
          }.reduceByKey(_ ++: _).map(data => {
            val part2 = data._2
            val len = part2.length
            val tmp = part2.map(d => d._1)
            val Max = tmp.max
            val ind = tmp.indexOf(Max)
            val vnew = part2(ind)._2
            (data._1, Max, vnew)
          })
          val localUpdV = updateInforForV.collect()
          localUpdU.foreach { case (uInd, max, unew) =>

            U(uInd) = unew

          }

          localUpdV.foreach { case (vInd, max, vnew) =>
            V(vInd) = vnew

          }
        }
      }
    }
  }
  def multiply(i: Int, j: Int): Double ={
    var res = 0.0
    for(t <- 0 to kValues - 1){
      res += U(i)(t) * V(j)(t)
    }
    res
  }
  private[spark] def getRating(
                                userFeatures: Array[Array[Double]] ,
                                prodFeatures: Array[Array[Double]],
                                numUser:Int,
                                numProduct:Int,
                                bias: Double,
                                minRating: Double,
                                maxRating: Double): Double = {
    math.min(maxRating, math.max(minRating, getRating(userFeatures, prodFeatures,numUser,numProduct, bias)))
  }

  private[spark] def getRating(
                                userFeatures:Array[Array[Double]],
                                prodFeatures:Array[Array[Double]],
                                numUser:Int,
                                numProduct:Int,
                                bias: Double): Double = {
    val dotres = dot(userFeatures(numUser), prodFeatures(numProduct))
    dotres
  }



  private[spark] def dot(a: Array[Double], b: Array[Double]): Double = {
    var sum = 0.0
    val len = a.length
    var i = 0
    while (i < len) {
      sum += a(i) * b(i)
      i += 1
    }
    sum
  }

  def predict(data: RDD[test]): Array[Double] = {
    val test = data.collect()
    var res = Array.fill(20)(0.0)
    println("predict begins:                 ")
    for(t <- 0 to test.length - 1){
      res(t) = dot(U(test(t).user), V(test(t).item))
      print(test(t).user + 1)
      print("\t")
      print(test(t).item + 1)
      print("\t")
      print(res(t))
      println(" ")
    }
    res
  }

  def printAll(): Unit ={
    var result = Array.fill(numUsers)(Array.fill(numProducts)(0.0))
    for(i <- 0 to 99){
      for(j <- 0 to 99){
        result(i)(j) = multiply(i, j)
      }
    }
    result.map(x => x.mkString(",")).foreach(println)
  }
  def judge(): Unit = {
    println("the U matrix is: ")

 //   U.map(x => x.mkString(", ")).foreach(println)
    for(i <- 0 to 99){
      print(i + ":     ")
      U(i).mkString(",").foreach(print)
      println(" ")
    }

    println("-----------------")
    println("the V matrix is: ")

    for(i <- 0 to 99){
      print(i + ":     ")
      V(i).mkString(",").foreach(print)
      println(" ")
    }
//    println("blockMat is this: ")
//    val test = blockMat.foreach(x=> {
//      println(x._1 + ":      ")
//      x._2.map(v => v.mkString(",")).foreach(println)              fixed
//    }
//    )


    }
}


class CollabFilter(
                    var decayFactor: Double,
                    var timeUnit: String
                    ) {
  def this() = this(1.0, CollabFilter.BATCHES)
  var model: CollabFilterModel = new CollabFilterModel(null, null, null, null, null, null, null, null, 0)


  def print(): Unit ={
    model.printAll()
  }
  def train(data: DStream[MatrixEntry]) {
    data.foreachRDD { (rdd, time) =>

    //  rdd.take(100).foreach(println)
      model = model.update(rdd, timeUnit)
    }
  }
  def RMSE(ratings: RDD[(Int, Int, Double)]): Double ={
    model.RMSE(ratings)
  }
  def setFirstLoop(x: Int): Unit ={
    model.setFirstLoop(x)
  }

  def predictOn(data: DStream[test]) = {
    data.foreachRDD((rdd, time) => model.predict(rdd))
  }
  def judge(): Unit = {
    model.judge()
  }
  def repeat(x: SparkContext): Unit ={
    model.repeat(x)
  }


}

object CollabFilter {
  final val BATCHES = "batches"
  final val POINTS = "points"
}

object  CollabFilterModel {
  val numAvailableBlock = 10
  def Filter(data: ((Int, Int), Array[Array[Double]]), availBlock: Array[coordinate]): Boolean = {

    for (t <- 0 to numAvailableBlock - 1 ) {
      if (data._1._1 == availBlock(t).x && data._1._2 == availBlock(t).y) {
        return true
      }
    }
    return false
  }
  def SGD(
           rating: Double,
           effrow: Int,
           effcol: Int,
           blockRowIndex:Int,
           blockColIndex:Int,
           uFeatures: Array[Array[Double]],
           pFeatures: Array[Array[Double]],
           kValues_br: Int,
           stepSize_br: Double,
           lambda_br: Double,
           numProducts_br: Int,
           numUsers_br: Int
           ): (Int, Int, Array[Double], Array[Double])  = {

    var dotProduct = 0.0
    for(k <- 0 to kValues_br - 1){
      dotProduct += uFeatures(effrow)(k) * pFeatures(effcol)(k)
    }
    val ratingDiff = dotProduct - rating
    val uFeatures_new  = uFeatures(effrow).clone()
    val pFeatures_new  = pFeatures(effcol).clone()
    for(k <- 0 to kValues_br - 1){
      val oldUserWeight = uFeatures(effrow)(k)
      val oldProWeight = pFeatures(effcol)(k)
      uFeatures_new(k) -= 2 * stepSize_br*(ratingDiff*oldProWeight + (lambda_br / numProducts_br) * oldUserWeight)
      pFeatures_new(k) -= 2 * stepSize_br*(ratingDiff*oldUserWeight + (lambda_br / numUsers_br) * oldProWeight)
    }
    (effrow, effcol, uFeatures_new, pFeatures_new)
  }
}
