import org.apache.log4j.LogMF.trace
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import org.apache.spark.sql.hive
import org.apache.spark.sql.catalyst.util.DropMalformedMode._

import java.io.FileNotFoundException


object devop_session_spark {
  var ss : SparkSession = null
  private var trace_log: Logger = LogManager.getLogger("Logger_Console")


  def main(args: Array[String]): Unit = {
    val sc = Session_Spark(Env = true).sparkContext
    sc.setLogLevel("OFF")
    val rdd_test : RDD[String] = sc.parallelize(List(" alain ", " juvenal ", " julien ", " ama ", " toto "))
    //rdd_test.foreach{ l => println(l)}
    val rdd2: RDD[String] = sc.parallelize(Array("Lucie", "Fabien", "jules"))
    println("\n")
   // rdd2.foreach{ l => println(l)}

   // println(" ")

    val rdd3 = sc.parallelize(Seq(("Julien", "Programmation", "15"), ("Afi", "Programmation", "16"), ("Ali le King", "Programmation", "20")))
   // rdd3.foreach {l => println(l)}

  //  println("")
   // println("Affichage des 2 premiers element de rdd3")

  // rdd3.take(2).foreach(m => print(m))



    // Losqu'on enregistre le rdd3 dans un fichier, on a par défaut 3 fichiers correspondant
    // chacun à une partition du rdd (à une chaque valeur du rdd)

    // On pourra imposer d'avoir une seule partition, c'est à dire un fichier contenant les 3 valeurs
    // en faisant: rdd3.repartition(1).saveAsTextFile("chemin/nom_fichier")



    // Création d'un rdd à partir d'une source de données (un fichier)


    val rdd4 = sc.textFile("/home/ali_mjyw_leking/Bureau/BIG_DATA_SCALA/rdd4")

  //  println("\n")



  //  println("Affichage du contenu du rdd créé à partir d'un fichier (source de données)")
   // rdd4.foreach(l => println(l))

//    println("\n")

   // print("Lecture de tous les fichiers d'un dossier")

    val rdd5 = sc.textFile("/home/ali_mjyw_leking/Bureau/BIG_DATA_SCALA")

   // println("\n")


   // println("Affichage du contenu du rdd créé à partir d'un fichier (source de données)")
   // rdd5.foreach(l => println(l))

    val rdd_transform: RDD[String] = sc.parallelize(List("J'aime apprendre", "Je trouve le temps pour aprendre", "Et je me pefectionne sans cesse !"))
    val rdd_map = rdd_transform.flatMap(x => x.split(" ")) // Pour appliquer le split sur chaque element du rdd
                                                                // de préference utiliser le flatmapp() au lieu de map
                                                                  // car ca ne fonctionne pas avec le map
    rdd_transform.foreach(l => println(l))
    println(" ")
    rdd_transform.foreach(l => println(l.length))


    println(" ")

    rdd_map.foreach(l => println(l.length))

  //  println("Affichage des elements de rdd map")

    //rdd_map.foreach(l => println(l))

    //println(" ")
    //println("Nombre d'element du rdd transform: "+ rdd_transform.count())
   // println("Nombre d'element du rdd map: "+ rdd_map.count())

    println(" ")

    val rdd6 = rdd_map.map(m => (m, m.length))

    rdd6.foreach(l => println(l))

    println(" ")

    val rdd7 = rdd_transform.map(m => (m, m.length))

    rdd7.foreach(l => println(l))

    val rdd_fm = rdd_transform.flatMap(x => x.split(" ")).map(y => (y, 1))

    // rdd_fm.foreach(l => println(l))

    // Regroupement des mots
    println(" ")
    println("Regroupement des mots")

   // rdd_fm.foreach(m => println(m))

    println(" ")


    val rdd_reduced = rdd_fm.reduceByKey(_ + _)
    rdd_reduced.foreach(m => println(m))



  }
  // Développement d'une session spark
// Fonction qui initialise et instancie une session spark
// Lorsque Env est à true => on est en local si non on est en production (déployer sur un cluster)
  def Session_Spark(Env: Boolean = true ): SparkSession = {

    try {

      if (Env) {
        System.setProperty("hadoop.home.dir", "/")
        ss = SparkSession.builder
          .master(master = "local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      } else {
          // ss = SparkSession
        ss = SparkSession.builder()
          .appName(name = "Mon application Spark")
          .config("spark.master", "local")
          .config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.4")
          .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
          .config("hive.metastore.uris", "thrift://hivemetastore:9083")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport() // important pour se connecter à un metastore hive
          .getOrCreate();

      }

    }catch{

      //case ex: FileNotFoundException => trace_log.error(ex.printStackTrace())
      case ex: Exception => trace_log.error( "Erreur de l'initialisation dans la session spark "+ ex.printStackTrace())
    }
        return ss

  }




  // Cette fonction n'est pas suffisante pour le déploiement, il faut encore une autre qu'on appelle: spark smith
  // La base du developpement d'application spark robuste est le rdd (vraiment indispensable)

}
