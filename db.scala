/* dilettanteDB - "database" from dilettantes to dilettantes */
/* more like prototype of prototype of experiment */

package db

import scala.language.postfixOps
import Rel._
import Op._
import types._

object types {
  type Columns = Array[Column]
  type Database = Array[Columns]
}



case class Column(data: Array[Int], info: Info, min: Int, max: Int) { 
  def range = max - min + 1
  def length = data.length
  def getAsAny(idx: Int) = data(idx)
}



case class Info(descending: Boolean, ascending: Boolean, unique: Boolean, oids: Boolean) {
  override def toString = s"descending = $descending, ascending = $ascending, unique = $unique, oids = $oids"
}

object Column {
  def make(data: Array[Int]) = {

    if (data.length == 0) {
      Column(data, Info(true, true, true, true), 0, -1)

    } else {

      var i = 1
      var ascending, descending = true
      var lastVal, min, max = data(0)
      var oids = data(0) == 0

      while (i < data.length) {
        if (data(i) >= lastVal) descending = false
        if (data(i) <= lastVal) ascending = false
        if (data(i) != i) oids = false
        min = math.min(min, data(i))
        max = math.max(max, data(i))
        lastVal = data(i)
        i += 1
      }

      Column(data, Info(descending, ascending, descending || ascending, oids), min, max)
    }
  }

}



// query AST

case class Aggregate(as: Array[Aggr], columns: Src, groupBy: Expr = null, orderBy: Expr = null)

sealed trait Src {
  def limit(limit: Int, offset: Int = 0) = Limit(this, limit, offset)
  def filter(cond: BoolExpr) = Filter(this, cond)
}

case class Table(database: Database, table: Int) extends Src {
  def columns = database(table)
}
case class Filter(src: Src, cond: BoolExpr) extends Src
case class Limit(src: Src, limit: Int, offset: Int) extends Src
case class Join(src1: Src, src2: Src, on: Expr) extends Src


sealed trait Aggr {
  def e: Expr
}

case class Use(e: Expr) extends Aggr
case class Sum(e: Expr) extends Aggr
case class Count(e: BoolExpr = null) extends Aggr
case class Max(e: Expr) extends Aggr
case class Min(e: Expr) extends Aggr
case class CountDistinct(e: Expr) extends Aggr




sealed trait Expr {
  def +(e: Expr) = Op.Plus(this, e)
  def +(x: Int)  = Op.Plus(this, Const(x))
  def -(e: Expr) = Op.Minus(this, e)
  def -(x: Int)  = Op.Minus(this, Const(x))
  def *(e: Expr) = Op.Times(this, e)
  def *(x: Int)  = Op.Times(this, Const(x))
  def /(e: Expr) = Op.Div(this, e)
  def /(x: Int)  = Op.Div(this, Const(x))
  def %(e: Expr) = Op.Mod(this, e)
  def %(x: Int)  = Op.Mod(this, Const(x))
  def > (b: Expr)   = Rel.Gt(this, b)
  def > (x: Int)    = Rel.Gt(this, Const(x))
  def < (b: Expr)   = Rel.Lt(this, b)
  def < (x: Int)    = Rel.Lt(this, Const(x))
  def === (b: Expr) = Rel.Eq(this, b)
  def !== (b: Expr) = Rel.Ne(this, b)

  //def preorder(f: PartialFunction[Expr => Unit])
}

case class Const(value: Int) extends Expr
case class Oid() extends Expr
case class Col(table: Int, col: Int) extends Expr
case class Op(x: Expr, y: Expr, op: String) extends Expr
case class Func1(f: Int => Int, e: Expr) extends Expr

object Op {
  def Plus(x: Expr, y: Expr)  = Op(x, y, "+")
  def Minus(x: Expr, y: Expr) = Op(x, y, "-")
  def Times(x: Expr, y: Expr) = Op(x, y, "*")
  def Div(x: Expr, y: Expr)   = Op(x, y, "/")
  def Mod(x: Expr, y: Expr)   = Op(x, y, "%")
}


sealed trait BoolExpr extends Expr {
  def unary_! = Not(this)
  def && (b: BoolExpr) = And(this, b)
  def || (b: BoolExpr) = Or(this, b)
}

case object True extends BoolExpr
case class Not(x: BoolExpr) extends BoolExpr
case class And(x: BoolExpr, y: BoolExpr) extends BoolExpr
case class Or(x: BoolExpr, y: BoolExpr) extends BoolExpr
case class Rel(x: Expr, y: Expr, op: String) extends BoolExpr

object Rel {
  def Eq(x: Expr, y: Expr) = Rel(x, y, "==")
  def Ne(x: Expr, y: Expr) = Rel(x, y, "!=")
  def Gt(x: Expr, y: Expr) = Rel(x, y, ">")
  def Lt(x: Expr, y: Expr) = Rel(x, y, "<")
}




object Compile {

  import scala.reflect.runtime.currentMirror
  import scala.tools.reflect.ToolBox
  val toolbox = currentMirror.mkToolBox()
  import toolbox.u.{Expr => _, _}
  import toolbox.{ u => universe  }

  class RecompilationNeeded extends Exception
  def assuming(x: Boolean) = if (!x) throw new RecompilationNeeded


  def freshName(n: String) =
    universe.internal.reificationSupport.freshTermName(n)


  def compileSrcInternal(src: Src, pream: Seq[Tree], post: Seq[Tree], cond: Tree, body: Tree): Tree = src match {
    case Limit(nested, limit, offset) =>
      val limitVar = freshName("_limit$")
      compileSrcInternal(nested, pream :+ q"var $limitVar = 0", post, q"$cond && $limitVar < ${limit+offset}", q"""
        ${if (offset > 0) {
            q"if ($limitVar >= $offset) { $body }"
          } else {
            q"$body"
          }
        }
        $limitVar += 1
      """)

//    case Filter(nested @ Table(_, _), Rel(Col(colidx), Const(c), ">")) if nested.columns(colidx).info.ascending =>
//      compileSrcInternal(nested, pream :+ q"""
//        idx = java.util.Arrays.binarySearch(columns($colidx).data, $c)
//        end = java.util.Arrays.binarySearch(columns($colidx).data, idx, columns($colidx).data.length, $c) + 1
//      """, post, cond, q"""
//        $body
//      """)

    case Filter(nested, _cond) =>
      compileSrcInternal(nested, pream, post, cond, q"""
        if (${compileExpr(_cond)}) {
          $body
        }
      """)

    case t @ Table(tables, tableidx) =>
      val columnFields  = for (tableidx <- 0 until tables.length ; i <- 0 until tables(tableidx).length) yield q"val ${TermName(s"column_${tableidx}_${i}")} = database($tableidx)($i).data"
      q"""
        var ${idx(tableidx)} = 0
        var end = database($tableidx)(0).data.length
        
        ..$columnFields
        ..$pream
        while (${idx(tableidx)} < end && $cond) {
          $body
          ${idx(tableidx)} += 1
        }
        ..$post
      """

    case Join(src1, src2, Rel(c1 @ Col(tableIdx1, colidx1), c2 @ Col(tableIdx2, colidx2), "==")) =>

      val mkJoinMap = compileSrcInternal(src2, Seq(q"var _mapping = collection.mutable.Map[Int, Int]()"), Seq(q"_mapping"), q"true", q"""
        _mapping.put(${compileExpr(c2)}, ${idx(tableIdx2)})
      """)

      compileSrcInternal(src1, q"val _mapping = $mkJoinMap" +: pream, post, cond, q"""
        val ${idx(tableIdx2)} = _mapping(${compileExpr(c1)})
        $body
      """)

//    case Join(src1, src2, _) =>
//      sys.error("cannot translate non-eqijoins (yet)")
  }



  def getColumn(colidx: Int, src: Src): Column = src match {
    case t @ Table(_, _) => t.columns(colidx)
    case Limit(s, _, _) => getColumn(colidx, s)
    case Filter(s, _) => getColumn(colidx, s)
    case Join(src1, _, _) => getColumn(colidx, src1)
  }

  def columnRange(colidx: Int, src: Src): Long = 
    getColumn(colidx, src) match {
      case c: Column => c.range
    }


  def compileAggr(a: Aggregate): Database => Columns = {
    val Aggregate(as, src, groupBy, orderBy) = a

    val counterFields = for (i <- 0 until as.length) yield q"var ${TermName(s"_counter_$i")} = ${compileAggrInit(as(i))}"

    def counterVar(i: Int): TermName = TermName(s"_counter_$i")


    val qq =
      groupBy match {
        case null =>
          q"""
            (database: db.types.Database) => {

              ..$counterFields

              ${compileSrcInternal(src, Seq(), Seq(), q"true", q"""
                ..${ for (i <- 0 until as.length) yield compileAggrCounter(as(i), q"${counterVar(i)}") }
              """)}

              Array(..${ for (i <- 0 until as.length) yield q"db.Column.make(Array(${counterVar(i)}))" })
            }
          """

        case Col(tableidx, colidx) if columnRange(colidx, src) < 32*1024 =>
          q"""
            (database: db.types.Database) => {
              class Counters {
                ..$counterFields
              }

              val base = database($tableidx)($colidx).min
              val m = Array.fill[Counters](database($tableidx)($colidx).range + 1)(null)

              ${compileSrcInternal(src, Seq(), Seq(), q"true", q"""
                val k = ${compileExpr(groupBy)}-base
                if (m(k) == null) m(k) = new Counters
                val c = m(k)
                ..${ for (i <- 0 until as.length) yield compileAggrCounter(as(i), q"c.${counterVar(i)}") }
              """)}

              val len = m.count(_ != null)
              val arrays = Array.fill(${as.length})(new Array[Int](len))

              var i, j = 0
              while (i < m.length) {
                if (m(i) != null) {
                  ..${ for (i <- 0 until as.length) yield q"arrays($i)(i) = ${compileAggrRes(as(i), q"m(i).${counterVar(i)}")}" }
                  j += 1
                }
                i += 1
              }

              arrays.map(db.Column.make)
            }
          """

        case groupBy =>
          q"""
            (database: db.types.Database) => {
              class Counters {
                ..$counterFields
              }

              val m = collection.mutable.Map[Int, Counters]()

              ${compileSrcInternal(src, Seq(), Seq(), q"true", q"""
                val c = m.getOrElseUpdate(${compileExpr(groupBy)}, new Counters)
                ..${ for (i <- 0 until as.length) yield compileAggrCounter(as(i), q"c.${counterVar(i)}") }
              """)}

              val len = m.size
              val arrays = Array.fill(${as.length})(new Array[Int](len))

              var i = 0
              m.values foreach { c =>
                ..${ for (i <- 0 until as.length) yield q"arrays($i)(i) = ${compileAggrRes(as(i), q"c.${counterVar(i)}")}" }
                i += 1
              }

              arrays.map(db.Column.make)
            }
          """
    }

    println(showCode(qq))
    toolbox.compile(qq)().asInstanceOf[Database => Columns]
  }

  def compileAggrCounter(a: Aggr, c: Tree) = a match {
    case Use(e)   => q"$c = ${compileExpr(e)}"
    case Sum(e)   => q"$c += ${compileExpr(e)}"
    case Count(null) => q"$c += 1"
    case Count(e) => q"if (${compileBoolExpr(e)}) $c += 1"
    case Min(e)   => q"if (${compileExpr(e)} < $c) $c = ${compileExpr(e)}"
    case Max(e)   => q"if (${compileExpr(e)} > $c) $c = ${compileExpr(e)}"
    case CountDistinct(e) => q"$c.+=(${compileExpr(e)})"
  }

  def compileAggrInit(a: Aggr) = a match {
    case Use(_)   => q"0"
    case Sum(_)   => q"0"
    case Count(_) => q"0"
    case Max(_)   => q"Int.MinValue"
    case Min(_)   => q"Int.MaxValue"
    case CountDistinct(_) => q"collection.mutable.Set[Int]()"
  }

  def compileAggrRes(a: Aggr, c: Tree) = a match {
    case CountDistinct(_) => q"$c.size"
    case _ => c
  }

  def compileExpr(e: Expr): Tree = e match {
    case be: BoolExpr => compileBoolExpr(be)
    case Const(x)     => q"$x"
    case Oid()        => q"idx"
    case Col(tableidx, colidx)  => q"${TermName(s"column_${tableidx}_${colidx}")}(${idx(tableidx)})"
    case Op(x, y, "+") => q"${compileExpr(x)} + ${compileExpr(y)}"
    case Op(x, y, "-") => q"${compileExpr(x)} - ${compileExpr(y)}"
    case Op(x, y, "*") => q"${compileExpr(x)} * ${compileExpr(y)}"
    case Op(x, y, "/") => q"${compileExpr(x)} / ${compileExpr(y)}"
    case Op(x, y, "%") => q"${compileExpr(x)} % ${compileExpr(y)}"
    case Func1(f, e) => q" ${reify(f)}(${compileExpr(e)}) " 
  }


  def compileBoolExpr(e: BoolExpr): Tree = e match {
    case True      => q"true"
    case Not(x)    => q"!${compileBoolExpr(x)}"
    case And(x, y) => q"${compileBoolExpr(x)} && ${compileBoolExpr(y)}"
    case Or(x, y)  => q"${compileBoolExpr(x)} || ${compileBoolExpr(y)}"
    case Rel(x, y, "==") => q"${compileExpr(x)} == ${compileExpr(y)}"
    case Rel(x, y, "!=") => q"${compileExpr(x)} != ${compileExpr(y)}"
    case Rel(x, y, ">")  => q"${compileExpr(x)} > ${compileExpr(y)}"
    case Rel(x, y, "<")  => q"${compileExpr(x)} < ${compileExpr(y)}"
  }

  def idx(tableidx: Int) = TermName("idx_"+tableidx)

}





object App extends App {

  def asTable(columns: Array[Column]): Seq[Seq[Any]] = {
    for (i <- 0 until columns.head.length) yield {
      columns map { _.getAsAny(i) } toSeq
    }
  }

  val len = 10000000
  val table1: Array[Column] = Array(
    Column.make(0 until len toArray),
    Column.make(Array.fill(len)(util.Random.nextInt)),
    Column.make(Array.fill(len)(util.Random.nextInt(20))),
    Column.make(len until 0 by -1 toArray)
  )

  val table2: Array[Column] = Array(
    Column.make(0 until 20 toArray),
    Column.make(0 until 20 map (_*2) toArray)  
  )

  val database = Array(table1, table2)

  for (columns <- database) {
    for (c <- columns) println(c)
    println()
  }

  println("running")


  val aggr = Aggregate(
    Array(Use(Col(0, 2)), Count(), Sum(Col(0,2)), Sum(Col(1,0)), Sum(Col(1,1)), CountDistinct(Col(1,1))),
    Join(Table(database, 0), Table(database, 1), Col(0,2) === Col(1,0)),
    groupBy = Col(0, 2)
  )

  {
    val sc = System.currentTimeMillis
    val f = Compile.compileAggr(aggr)
    println("compilation time: "+(System.currentTimeMillis - sc))

    val s = System.currentTimeMillis
//    for (_ <- 0 until 50) {
//      val s = System.currentTimeMillis
//      val res = f(columns)
//      print(res.length+" ")
//      println(System.currentTimeMillis - s)
//    }
//
    println()
    asTable(f(database)) foreach println

    println(System.currentTimeMillis - s)
  }

}
