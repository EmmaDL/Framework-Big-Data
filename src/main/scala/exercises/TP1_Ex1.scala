package exercises

import org.apache.log4j.{Level, Logger}
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
    print("Le pourcentage des vues des films de Leonardo Di Caprio est de " +pourcentage_vues_ldc + "%")


    //Question 5

  }

}
