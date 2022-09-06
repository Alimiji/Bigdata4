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
    val rdd_test : RDD[String] = sc.parallelize(List("alain", "juvenal", "julien", "ama", "toto"))
    rdd_test.foreach{
      l => print(l)
    }


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
          .enableHiveSupport()
          .getOrCreate()
      } else {
          // ss = SparkSession
        ss = SparkSession.builder()
          .appName(name = "Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport() // important pour se connecter à un metastore hive
          .getOrCreate()

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
