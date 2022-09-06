import scala.collection.mutable._

// Apprentissage du scala
object HelloWorldBigData {
  /*
  Mon premier programme en scala
   */

  def main(args: Array[String]): Unit = {
    println("Waouh! ca c'est mon premier programme en scala.")

    val var_immutable1: Float = 3.14f // Variable immutable
    val var_immutable2: Int = 5
    var var_mutable1 : Int = 100  // Variable mutable

    var_mutable1 += 50

    println("Valeur de la variable immutable 1: " + var_immutable1)
    println("Valeur de la variable immutable 2: " + var_immutable2)
    println("Valeur de la variable mutable 1: " + var_mutable1)


    // Appel de la fonction Somme
    //println(Somme(80000,8000))
    println("Nombres paires compris entre la variable mutable 1 et la variable immutable 2: ")

    AfficherNombresPaires(var_immutable2, var_mutable1)

    collectionScala()

  }

  // Fonction somme : Ma première fonction en scala

  def Somme(a: Int, b:Int): Int = {
    return a+b
  }

  // Methode procédurale qui affiche tout les nombres paires
  // comprises entre deux nombres entiers donnés en paramètre

  def AfficherNombresPaires(a: Int, b: Int): Unit ={

    var min: Int = 0
    var max: Int = 0

    if(a<=b){
      min = a
      max = b
    }else{
      min = b
      max = a
    }

    println("Affichage des nombres paires compris entre " + min + " et " + max)

    while(max >= min){

      if(max%2 == 0){
        println(max)
      }
      max -= 1
    }


  }

  def collectionScala(): Unit = {

    val  maliste_num1: List[Int] = List(1, 2, 3, 5, 18, 45, 15)
    val names = List("Joel", "eude", "chris", "maurice", "Nicole", "Joe", "joel")
    val maliste_num2 = List.range(100, 500, 15)
    println(maliste_num1)
    println(maliste_num2)
    println(names)
    println("Affichage de chaque élément de: " + maliste_num1)

    for(i <- maliste_num1){
      println(i)
    }

    // Utilisation des fonctions anonymes

    val tableau_resultat1: List[String] = names.filter(e => e.startsWith("J"))
    val tableau_resultat2: List[String] = names.filter(e => e.startsWith("J") || e.startsWith("j"))

    val tableau_resultat3 = names.map(e => e.capitalize)
    val tableau_resultat4 = names.map(e => e.toUpperCase)


    println(tableau_resultat1)
    println(tableau_resultat2)
    // Affichage des elements de tableau_resultat3
    println("tableau_resultat 3")
    tableau_resultat3.foreach(e => println(e))

    // Affichage des elements de tableau_resultat4
    println("Affichage des élements de  tableau_resultat 4")
    tableau_resultat4.foreach(e => println(e))

    // Autre facon d'afficher la liste names

    names.foreach(println(_))
    
  }



}
