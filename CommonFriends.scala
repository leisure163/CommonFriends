import org.apache.spark.{SparkConf, SparkContext}

/**
  * This is an example of finding common friends of users
  * and it needs to enter two parameters
  * The first arg should be the path of the input file,and the format of the file is (user:fried1,fried2......)
  * The second arg should be the path of the output file,and the format of the file is (user1-user2:fried1,fried2......)
  *
  * Created by shang on 2017/3/17.
  */

object CommonFriends {
  def main(args: Array[String]): Unit = {

//    check that the args are valid
    require(args.length == 2)
//    assign two args to the variable input and output
    val input = args(0)
    val output = args(1)

//    set the configures of spark
    val conf = new SparkConf().setAppName("CommonFriends").setMaster("local")
    val sc = new SparkContext(conf)
   
//    read content from input file
    sc.textFile(input)
   
//    invert the data of each row,chang the data to ((firend1,user2),(fried2,user1),......)
      .flatMap(line => {
        val split = line.split(":")
        split(1).split(",").map(v => {(v,split(0))})
      })
   
//    grouping result to ((friend1,(user1,user2,......),(friend2,(user1,user2,......),......)
      .sortBy(_._2)
      .groupByKey()
   
//    generate the pairwise covering of the users like this (((user1，user2),friend1),(user1,user3),friend1),......)
      .flatMap(line => {
        val arr = line._2.toArray
        for {
          i <- 0 until arr.length-1
          j <- i+1 until arr.length
        }yield {
          ((arr(i), arr(j)), line._1)
        }
      })
   
//    grouping and sorting the result
      .sortBy(_._2)
      .groupByKey()
      .sortByKey()
   
//    format the result to (user1-user2:fried1,fried2……)
      .map(line => {
        s"${line._1._1}-${line._1._2}:${line._2.mkString(",")}"
      })
      .saveAsTextFile(output)
   
    sc.stop()
  }
}
