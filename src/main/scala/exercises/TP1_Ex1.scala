package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TP1_Ex1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1
    val rdd = sparkSession.sparkContext.textFile("data/films.csv")

    //Question 2
    val rdd_DiCaprio = rdd.filter(elem => elem.contains("Di Caprio"))
    print("Il y a : " + rdd_DiCaprio.count() + " films réalisés par Léonardo Di Caprio \n")


    //Question 3
    val counts = rdd_DiCaprio.map(item => (item.split(";")(2).toDouble) )
    val moyenne = counts.sum
    print("La moyenne des films de Di Caprio est: " + moyenne/rdd_DiCaprio.count() + "\n")


    //Question 4
    val total_vues_ldc = rdd_DiCaprio.map(item => (item.split(";")(1).toDouble))
    val total_vues = rdd.map(item => (item.split(";")(1).toDouble))
    val pourcentage_vues_ldc = (total_vues_ldc.sum() / total_vues.sum())*100
    print("Le pourcentage des vues des films de Leonardo Di Caprio est de " +pourcentage_vues_ldc + "%\n")


    //Question 5
    //Moyenne des notes par acteur
    val count: RDD[(String, Double)] = rdd.map(item => ((item.split(";")(3)), (item.split(";")(2).toDouble)))
    val withValue = count.mapValues(e => (1.0, e))
    val countSums = withValue.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    print("Moyenne des notes par acteur:\n")
    keyMeans.foreach(println)

    //Moyenne des vues par acteur
    val count2: RDD[(String, Double)] = rdd.map(item => ((item.split(";")(3)), (item.split(";")(1).toDouble)))
    val withValue2 = count2.mapValues(e => (1.0, e))
    val countSums2 = withValue2.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans2 = countSums2.mapValues(avgCount => avgCount._2 / avgCount._1)
    print("Moyenne des vues par acteur:\n")
    keyMeans2.foreach(println)

  }

}
