object Main extends App {
  // a few French first names that can be composed with Jean
  val frenchFirstNameList = List(
    "Georges",
    "Claude",
    "Philippe",
    "Pierre",
    "François",
    "Michel",
    "Bernard",
    "Guillaume",
    "André",
    "Christophe",
    "Luc",
    "Louis",
  )

  frenchFirstNameList.foreach { name =>
    println(name + " and Jean-" + name + " are different French first names!")
  }

  println("-----")

  frenchFirstNameList.foreach(name => {
    var message: String = name + " and Jean-"
    message += name
    message += " are different French first names!"
    println(message)
  })
}
