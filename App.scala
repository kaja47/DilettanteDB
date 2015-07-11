package db

import scala.language.postfixOps
import Rel._
import Op._


object App extends App {

  def tabulate(table: Table): Seq[Seq[Any]] =
    for (i <- 0 until table.columns.head.length) yield
      table.columns map { _.getAsAny(i) } toSeq

  val len = 10000000
  val table1 = Table(Array(
    Column.make(0 until len toArray),
    Column.make(0 until len map (_ % 20) toArray)
  ))

  val table2 = Table(Array(
    Column.make(0 until 100 toArray),
    Column.make(0 until 100 map (_ * 2 % 20) toArray)  
  ))

  val database = Database(Array(table1, table2))

  for (t <- database.tables) {
    t.columns foreach println ; println()
  }


  val t1 = RealTableRef(database, 0)
  val t2 = RealTableRef(database, 1)

  import Rules.rewriteAll

  val q1 = TableScan(t1)
    .take(100)
    .filter(r => r(0) < Const(100))
    .filter(r => r(0) > Const(10))
    .flatMap(r1 =>
      TableScan(t2)
        .filter(r2 => r1(1) === r2(1))
        .map(r2 => RowRef(Array(
          r1(0), r1(1), r2(0), r2(1)
        )))
    )

  val q2 = rewriteAll(q1)

  val query = q2
    .groupBy(
      aggr = r => Seq(Use(r(0)), Use(r(2)), Sum(r(1)), Sum(r(3))),
      on   = r => r(0)
    )

  println(q1)
  println(q2)

  val queryFunc = Compile.compile(query)
  tabulate(queryFunc(database)) foreach println

}
