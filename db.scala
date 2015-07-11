/* dilettanteDB - "database" from dilettantes to dilettantes */
/* more like prototype of prototype of experiment */

package db

import scala.language.postfixOps
import Expr._


case class Database(tables: Array[Table]) {
  override def toString = "Database()"
}

case class Table(columns: Array[Column]) {
  val length = columns(0).length
}

trait Column {
  def info: Info
  def length: Int
  def getAsAny(idx: Int): Any
}

case class IntColumn(data: Array[Int], info: Info, min: Int, max: Int) extends Column { 
  def range = max - min + 1
  def length = data.length
  def getAsAny(idx: Int) = data(idx)
}

case class LongColumn(data: Array[Long], info: Info, min: Long, max: Long) extends Column { 
  def range = max - min + 1
  def length = data.length
  def getAsAny(idx: Int) = data(idx)
}

case class FloatColumn(data: Array[Float], info: Info, min: Float, max: Float) extends Column {
  def length = data.length
  def getAsAny(idx: Int) = data(idx)
}

case class DoubleColumn(data: Array[Double], info: Info, min: Double, max: Double) extends Column {
  def length = data.length
  def getAsAny(idx: Int) = data(idx)
}



case class Info(tpe: ColumnType, descending: Boolean, ascending: Boolean, unique: Boolean, oids: Boolean) {
  override def toString = s"descending = $descending, ascending = $ascending, unique = $unique, oids = $oids"
}

object Info {
  def randomData(tpe: ColumnType) = Info(tpe, false, false, false, false)
}

sealed trait ColumnType
case object IntType extends ColumnType
case object LongType extends ColumnType
case object FloatType extends ColumnType
case object DoubleType extends ColumnType



object Column {
  def make(data: Array[Int]) = {
    if (data.length == 0) {
      IntColumn(data, Info(IntType, true, true, true, true), 0, -1)

    } else {

      var i = 1
      var ascending, descending, ascDisctinct, descDistinct = true
      var lastVal, min, max = data(0)
      var oids = data(0) == 0

      while (i < data.length) {
        if (data(i) >= lastVal) descending = false
        if (data(i) >  lastVal) descDistinct = false
        if (data(i) <= lastVal) ascending = false
        if (data(i) <  lastVal) ascDisctinct = false
        if (data(i) != i) oids = false
        min = math.min(min, data(i))
        max = math.max(max, data(i))
        lastVal = data(i)
        i += 1
      }

      IntColumn(data, Info(IntType, descending, ascending, descDistinct || ascDisctinct, oids), min, max)
    }
  }

  def makeWithoutChecking(data: Array[Int]) = {
    IntColumn(data, Info.randomData(IntType), 0, -1)
  }


  def make(data: Array[Long]) = {
    if (data.length == 0) {
      LongColumn(data, Info(LongType, true, true, true, true), 0, -1)

    } else {

      var i = 1
      var ascending, descending, ascDisctinct, descDistinct = true
      var lastVal, min, max = data(0)
      var oids = data(0) == 0

      while (i < data.length) {
        if (data(i) >= lastVal) descending = false
        if (data(i) >  lastVal) descDistinct = false
        if (data(i) <= lastVal) ascending = false
        if (data(i) <  lastVal) ascDisctinct = false
        if (data(i) != i) oids = false
        min = math.min(min, data(i))
        max = math.max(max, data(i))
        lastVal = data(i)
        i += 1
      }

      LongColumn(data, Info(LongType, descending, ascending, descDistinct || ascDisctinct, oids), min, max)
    }
  }

  def makeWithoutChecking(data: Array[Long]) = {
    LongColumn(data, Info.randomData(LongType), 0, -1)
  }

  def make(data: Array[Float]) = {
    if (data.length == 0) {
      FloatColumn(data, Info(FloatType, true, true, true, true), 0, -1)

    } else {

      var i = 1
      var ascending, descending, ascDisctinct, descDistinct = true
      var lastVal, min, max = data(0)
      var oids = data(0) == 0

      while (i < data.length) {
        if (data(i) >= lastVal) descending = false
        if (data(i) >  lastVal) descDistinct = false
        if (data(i) <= lastVal) ascending = false
        if (data(i) <  lastVal) ascDisctinct = false
        if (data(i) != i) oids = false
        min = math.min(min, data(i))
        max = math.max(max, data(i))
        lastVal = data(i)
        i += 1
      }

      FloatColumn(data, Info(FloatType, descending, ascending, descDistinct || ascDisctinct, oids), min, max)
    }
  }

  def makeWithoutChecking(data: Array[Float]) = {
    FloatColumn(data, Info.randomData(FloatType), 0, -1)
  }

  def make(data: Array[Double]) = {
    if (data.length == 0) {
      DoubleColumn(data, Info(DoubleType, true, true, true, true), 0, -1)

    } else {

      var i = 1
      var ascending, descending, ascDisctinct, descDistinct = true
      var lastVal, min, max = data(0)
      var oids = data(0) == 0

      while (i < data.length) {
        if (data(i) >= lastVal) descending = false
        if (data(i) >  lastVal) descDistinct = false
        if (data(i) <= lastVal) ascending = false
        if (data(i) <  lastVal) ascDisctinct = false
        if (data(i) != i) oids = false
        min = math.min(min, data(i))
        max = math.max(max, data(i))
        lastVal = data(i)
        i += 1
      }

      DoubleColumn(data, Info(DoubleType, descending, ascending, descDistinct || ascDisctinct, oids), min, max)
    }
  }

  def makeWithoutChecking(data: Array[Double]) = {
    DoubleColumn(data, Info.randomData(DoubleType), 0, -1)
  }

}



// *** query AST ***

/** TableRef represent way to reference source table. If one query uses one
  * table from multiple places (eg. table joined twice by 2 different condition)
  * there must one separate TableRefs for every use of that table. Iteration and
  * table lengths are bound to TableRef. */
sealed trait TableRef {
  def alias: String
  def apply(idx: Int): Col
  def cols: Array[Col]

  def row = RowRef(cols)

  def hasCol(col: Col) = col.table == this
}

object TableRef {
  private var c = -1
  def generateAlias() = {
    c += 1
    "table$"+c
  }
}


case class RealTableRef(database: Database, tableidx: Int, alias: String = TableRef.generateAlias()) extends TableRef { 
  def apply(idx: Int) = cols(idx)
  val cols = database.tables(tableidx).columns.zipWithIndex map { case (c, idx) => Col(this, idx)(c.info) }
}
case class TempTableRef(infos: Array[Info], alias: String = TableRef.generateAlias()) extends TableRef {
  def apply(idx: Int) = cols(idx)
  val cols = infos.zipWithIndex map { case (info, idx) => Col(this, idx)(info) }
}




/** RowRef represents composition of current logical row. It aggregate way to
  * reference columns of interest, but it have no connection to iteration. */
case class RowRef(cols: Array[Col]) {
  def apply(idx: Int) = cols(idx)
  def length = cols.length

  def tableRefs = cols.map(_.table).toSet
}



sealed trait Src {
  def row: RowRef

  def take(n: Int) = Take(this, n)
  def filter(f: RowRef => BoolExpr) = Filter(this, f(row))
  def map(f: RowRef => RowRef) = Map(this, f(row))
  def flatMap(f: RowRef => Src) = FlatMap(this, f(row))

  def groupBy(aggr: RowRef => Seq[Aggr], on: RowRef => Expr = null) = GroupBy(this, aggr(row), if (on == null) null else on(row))
  def getAll = GetAll(this)
}

sealed trait Result {
  def tableRef: TableRef
}



case class TableScan(table: TableRef, start: Seq[Expr] = Seq(Const(0)), end: Seq[Expr] = Seq()) extends Src { def row = table.row }
case class Take(src: Src, limit: Int, offset: Int = 0) extends Src { def row = src.row }
case class Filter(src: Src, expr: BoolExpr) extends Src { def row = src.row }
case class Map(src: Src, row: RowRef) extends Src
case class FlatMap(src: Src, inner: Src) extends Src { // src.flatMap(r => inner)
  def row = inner.row
}
case class OidLookup(table: TableRef, e: Expr) extends Src { def row = table.row }

/** Builds hashmap of inner that is used to lookup data in inner by oid. Hashtable's
  * keys are results of evaluating expression `innerExpr` on `inner`, values are
  * corresponding `oids`. */
case class HashLookup(src: Src, inner: Src, srcExpr: Expr, innerExpr: Expr) extends Src { def row = inner.row }

/** Same as HashLookup but multiple value can be associated with multiple oids */
case class HashMultiLookup(src: Src, inner: Src, srcExpr: Expr, innerExpr: Expr) extends Src { def row = inner.row }

//case class Join(src1: Src, src2: Src, on: Expr) extends Src

case class GroupBy(src: Src, aggrs: Seq[Aggr], on: Expr = null) extends Result {
  val tableRef = TempTableRef(aggrs map { a => Info.randomData(IntType) } toArray)
}
case class GetAll(src: Src) extends Result {
  def tableRef = TempTableRef(src.row.cols.map(_.info))
}



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

  //override def nestedExprs: Seq[Expr]

  def +(e: Expr): Expr = Op(this, e, "+")
  def -(e: Expr): Expr = Op(this, e, "-")
  def *(e: Expr): Expr = Op(this, e, "*")
  def /(e: Expr): Expr = Op(this, e, "/")
  def %(e: Expr): Expr = Op(this, e, "%")
  def +(x: Int): Expr  = this + Const(x)
  def -(x: Int): Expr  = this - Const(x)
  def *(x: Int): Expr  = this * Const(x)
  def /(x: Int): Expr  = this / Const(x)
  def %(x: Int): Expr  = this % Const(x)
  def > (b: Expr): BoolExpr   = Rel(this, b, ">")
  def < (b: Expr): BoolExpr   = Rel(this, b, "<")
  def >=(b: Expr): BoolExpr   = Rel(this, b, ">=")
  def <=(b: Expr): BoolExpr   = Rel(this, b, "<=")
  def > (x: Int): BoolExpr    = this > Const(x)
  def < (x: Int): BoolExpr    = this < Const(x)
  def >=(x: Int): BoolExpr    = this >= Const(x)
  def <=(x: Int): BoolExpr    = this <= Const(x)
  def === (b: Expr): BoolExpr = Rel(this, b, "==")
  def !== (b: Expr): BoolExpr = Rel(this, b, "!=")
}


object Expr {

  def _traverse[T, R](e: Any, f: PartialFunction[Any, T], g: (R, T) => R, init: R): R = {
    val fe = if (f.isDefinedAt(e)) g(init, f(e)) else init
    e match {
      case e: Product =>
        e.productIterator.foldLeft(fe)((acc, e) => _traverse(e, f, g, acc))
      case e: collection.Traversable[_] =>
        e.foldLeft(fe)((acc, e) => _traverse(e, f, g, acc))
      case _: Array[Int] | _: Array[Long] | _: Array[Float] | _: Array[Double] | _: Array[Boolean] => fe
      case e: Array[_] => 
        e.foldLeft(fe)((acc, e) => _traverse(e, f, g, acc))
      case _ => fe
    }
    
  }

  def getReferencesColumns(r: Result) =
    _traverse[Col, Set[Col]](r, {
      case c: Col => c
    }, (acc, x) => acc + x, Set())

  def getTableRefs(expr: Expr) = _traverse[TableRef, Set[TableRef]](expr, {
    case c: Col => c.table
  }, (acc, t) => acc + t, Set())

  def getOneTableRef(expr: Expr) = {
    val trs = getTableRefs(expr)
    require(trs.size == 1)
    trs.head
  }

  def colIsFrom(col: Col, tableRefs: Set[TableRef]) =
    tableRefs.exists(tr => col.table == tr)

}


case class Const(value: Int) extends Expr
//case class Param(paramidx: Int) extends Expr
case class Oid(table: TableRef) extends Expr
case class End(table: TableRef) extends Expr
case class Length(table: TableRef) extends Expr
case class Col(table: TableRef, colidx: Int)(val info: Info) extends Expr
case class Op(x: Expr, y: Expr, op: String) extends Expr
case class Func1(f: Int => Int, e: Expr) extends Expr
case class IfExpr(cond: BoolExpr, thenExpr: Expr, elseExpr: Expr) extends Expr
case class Cast(x: Expr, tpe: ColumnType) extends Expr
case class BinarySearch(col: Col, value: Expr, startIndex: Expr, findLowerBound: Boolean = false) extends Expr


sealed trait BoolExpr extends Expr {
  def unary_! = Not(this)
  def && (b: BoolExpr) = And(this, b)
  def || (b: BoolExpr) = Or(this, b)
}

case object True extends BoolExpr
case object False extends BoolExpr
case class Not(x: BoolExpr) extends BoolExpr
case class And(x: BoolExpr, y: BoolExpr) extends BoolExpr
case class Or(x: BoolExpr, y: BoolExpr) extends BoolExpr
case class Rel(x: Expr, y: Expr, op: String) extends BoolExpr




object Util {
  def binarySearch(data: IntColumn, fromIndex: Int, toIndex: Int, key: Int, findLowerBound: Boolean): Int = {
    Compile.assuming(data.info.ascending)
    val pos = java.util.Arrays.binarySearch(data.data, fromIndex, toIndex, key)
    if (findLowerBound) {
      if (pos < 0) -pos - 1 else pos
    } else {
      pos
    }
  }
}




object Compile {

  import scala.reflect.runtime.currentMirror
  import scala.tools.reflect.ToolBox
  val toolbox = currentMirror.mkToolBox()
  import toolbox.u.{ Expr => _, _}

  class RecompilationNeeded extends Exception
  def assuming(x: Boolean) = if (!x) throw new RecompilationNeeded


  def freshName(n: String) =
    toolbox.u.internal.reificationSupport.freshTermName(n)


  def compileSrc(src: Src, cond: Tree, body: Tree): Tree = src match {
    case TableScan(table, start, end) =>
      q"""
        var ${compileOid(table)} = Seq(..${start map compileExpr}).max
        var ${compileEnd(table)} = Seq(..${(Length(table) +: end) map compileExpr}).min
        
        while (${compileExpr(Oid(table))} < ${compileExpr(End(table))} && $cond) {
          $body
          ${compileExpr(Oid(table))} += 1
        }
      """

    case OidLookup(table, e) =>
      q"""
        var ${compileOid(table)} = ${compileExpr(e)}
        $body
      """

    case HashLookup(src, inner, srcExpr, innerExpr) =>
      val mapping = freshName("_mapping$")

      val itr = Expr.getOneTableRef(innerExpr)

      q"""
        val $mapping = collection.mutable.Map[Int, Int]()
        ${compileSrc(inner, q"true", q"""
          $mapping.put(${compileExpr(innerExpr)}, ${compileOid(itr)})
        """)}

        ${compileSrc(src, q"true", q"""
          if ($mapping.contains(${compileExpr(srcExpr)})) {
            val ${compileOid(itr)} = $mapping(${compileExpr(srcExpr)})
            $body
          }
        """)}
      """

    case HashMultiLookup(src, inner, srcExpr, innerExpr) =>
      val mapping = freshName("_mapping$")

      val itr = Expr.getOneTableRef(innerExpr)

      q"""
        val $mapping = collection.mutable.Map[Int, collection.mutable.Set[Int]]()
        ${compileSrc(inner, q"true", q"""
          $mapping.getOrElseUpdate(${compileExpr(innerExpr)}, collection.mutable.Set()) += ${compileOid(itr)}
        """)}

        ${compileSrc(src, q"true", q"""
          if ($mapping.contains(${compileExpr(srcExpr)})) {
            for (${compileOid(itr)} <- $mapping(${compileExpr(srcExpr)})) {
              $body
            }
          }
        """)}
      """

    case Map(src, row) =>
      compileSrc(src, cond, body)

    case FlatMap(src, inner) =>
      compileSrc(src, cond, compileSrc(inner, q"true", body))

    case Take(nested, limit, offset) =>
      val limitVar = freshName("_limit$")
      q"""
        var $limitVar = 0
        ${compileSrc(nested, q"""
          $cond && $limitVar <= ${limit+offset}
        """, 
        if (offset == 0) { q"""
          $limitVar += 1
          $body
        """ } else { q"""
          $limitVar += 1
          if ($limitVar >= $offset) {
            $body
          }
        """ }
        )}
      """

    case Filter(nested, _cond) =>
      compileSrc(nested, cond, q"""
        if (${compileExpr(_cond)}) {
          $body
        }
      """)


      /*
    case Join(src1, src2, Rel(c1 @ Col(tableIdx1, colidx1), c2 @ Col(tableIdx2, colidx2), "==")) =>
      val mappingVar = freshName("_mapping$")

      q"""
        var $mappingVar = collection.mutable.Map[Int, Int]()
        ${compileSrc(src2, cond, q"""
          $mappingVar.put(${compileExpr(c2)}, ${idx(tableIdx2)})
        """)}

        ${compileSrc(src1, cond, q"""
          val ${idx(tableIdx2)} = $mappingVar(${compileExpr(c1)})
          $body
        """)}
      """
      */
  }


  def compileResult(a: Result) = a match {
    case GroupBy(src, as, null) =>
      val counterVars = 0 until as.length map { _ => freshName("_counter$") }
      val initCounters = for (i <- 0 until as.length) yield q"var ${counterVars(i)} = ${compileAggrInit(as(i))}"

      q"""
        ..$initCounters

        ${compileSrc(src, q"true", q"""
          ..${ for (i <- 0 until as.length) yield compileAggrCounter(as(i), q"${counterVars(i)}") }
        """)}

        db.Table(Array(..${ for (i <- 0 until as.length) yield q"db.Column.make(Array(${compileAggrRes(as(i), q"${counterVars(i)}")}))" }))
      """

    case GroupBy(src, as, groupBy) =>
      val counterVars = 0 until as.length map { _ => freshName("_counter$") }
      val initCounters = for (i <- 0 until as.length) yield q"var ${counterVars(i)} = ${compileAggrInit(as(i))}"

      q"""
        class Counters { ..$initCounters }

        val m = collection.mutable.Map[Int, Counters]()

        ${compileSrc(src, q"true", q"""
          val c = m.getOrElseUpdate(${compileExpr(groupBy)}, new Counters)
          ..${ for (i <- 0 until as.length) yield compileAggrCounter(as(i), q"c.${counterVars(i)}") }
        """)}

        val len = m.size
        val arrays = Array.fill(${as.length})(new Array[Int](len))

        var i = 0
        m.values foreach { c =>
          ..${ for (i <- 0 until as.length) yield q"arrays($i)(i) = ${compileAggrRes(as(i), q"c.${counterVars(i)}")}" }
          i += 1
        }

        db.Table(arrays.map(db.Column.makeWithoutChecking))
      """

    case GetAll(src) =>
      q"""
        val builders = Array(..${0 until src.row.length map { i => q"new collection.mutable.ArrayBuilder.ofInt" }})
        ${compileSrc(src, q"true", q"""
          ..${0 until src.row.length map { i =>
            q"builders($i) += ${compileExpr(src.row(i))}"
          }}
        """)}

        db.Table(builders.map(b => db.Column.makeWithoutChecking(b.result)))
      """
  }


  def compileFunction(a: Result): Tree = {

    val cols = getReferencesColumns(a).toVector

    q"""
      (database: db.Database) => {
        ..${for { c <- cols } yield q"val ${TermName(compileColRef(c).toString)} = ${compileColRefReal(c)}"}
        ${compileResult(a)}
      }
    """
  }

  def compile(a: Result): Database => Table = {
    val qq = compileFunction(a)
    println(showCode(qq))
    toolbox.compile(qq)().asInstanceOf[Database => Table]
  }

  def show(a: Result): Unit = {
    println(showCode(compileFunction(a)))
  }

  def compileAggrCounter(a: Aggr, c: Tree) = a match {
    case Use(e)      => q"$c = ${compileExpr(e)}"
    case Sum(e)      => q"$c += ${compileExpr(e)}"
    case Count(null) => q"$c += 1"
    case Count(e)    => q"if (${compileBoolExpr(e)}) $c += 1"
    case Min(e)      => q"if (${compileExpr(e)} < $c) $c = ${compileExpr(e)}"
    case Max(e)      => q"if (${compileExpr(e)} > $c) $c = ${compileExpr(e)}"
    case CountDistinct(e) => q"$c.+=(${compileExpr(e)})"
  }

  def compileAggrInit(a: Aggr) = a match {
    case Use(_)      => q"0: ${compileType(typeOf(a.e))}"
    case Sum(_)      => q"0: ${compileType(typeOf(a.e))}"
    case Count(_)    => q"0: Int"
    case Max(_)      => q"Int.MinValue" ; ???
    case Min(_)      => q"Int.MaxValue" ; ???
    case CountDistinct(_) => q"collection.mutable.Set[${compileType(typeOf(a.e))}]()"
  }

  def compileAggrRes(a: Aggr, c: Tree) = a match {
    case CountDistinct(_) => q"$c.size"
    case _ => c
  }

  def compileExpr(e: Expr): Tree = e match {
    case be: BoolExpr => compileBoolExpr(be)
    case Const(x)     => q"$x"
    case Oid(t)       => q"${compileOid(t)}"
    case End(t)       => q"${compileEnd(t)}"

    case Length(t)      => q"${compileTableRef(t)}.length"
    case c @ Col(t, colidx) => q"${compileColRef(c)}.data(${compileOid(t)})"

    case Op(x, y, "+") => q"${compileExpr(x)} + ${compileExpr(y)}"
    case Op(x, y, "-") => q"${compileExpr(x)} - ${compileExpr(y)}"
    case Op(x, y, "*") => q"${compileExpr(x)} * ${compileExpr(y)}"
    case Op(x, y, "/") => q"${compileExpr(x)} / ${compileExpr(y)}"
    case Op(x, y, "%") => q"${compileExpr(x)} % ${compileExpr(y)}"
    case Op(x, y, _) => throw new Exception("unsupported operator")

    case Func1(f, e) => q"${reify(f)}(${compileExpr(e)})" 

    case Cast(e, tpe) => q"${compileCast(e, tpe)}"

    case IfExpr(cond, thenExpr, elseExpr) => q"if (${compileBoolExpr(cond)}) ${compileExpr(thenExpr)} else ${compileExpr(elseExpr)}"

    case BinarySearch(col, value, startIndex, findLowerBound) =>
      q"""{
        db.Util.binarySearch(${compileColRef(col)}, ${compileExpr(startIndex)}, ${compileExpr(Length(col.table))}, ${compileExpr(value)}, $findLowerBound)
      }"""
  }

  def compileCast(e: Expr, tpe: ColumnType) = tpe match {
    case IntType    => q"${compileExpr(e)}.toInt"
    case LongType   => q"${compileExpr(e)}.toLong"
    case FloatType  => q"${compileExpr(e)}.toFloat"
    case DoubleType => q"${compileExpr(e)}.toDouble"
  }

  def compileOid(t: TableRef) = TermName(s"_oid_${t.alias}")
  def compileEnd(t: TableRef) = TermName(s"_end_${t.alias}")

  def compileTableRef(t: TableRef) = t match {
    case RealTableRef(_, tableidx, alias) => q"database.tables($tableidx)"
    case TempTableRef(columns, alias)     => q"${TermName(s"_temp_table_$alias")}"
  }

  def compileColRefReal(col: Col) = col.info.tpe match {
    case IntType    => q"${compileTableRef(col.table)}.columns(${col.colidx}).asInstanceOf[db.IntColumn]"
    case LongType   => q"${compileTableRef(col.table)}.columns(${col.colidx}).asInstanceOf[db.LongColumn]"
    case FloatType  => q"${compileTableRef(col.table)}.columns(${col.colidx}).asInstanceOf[db.FloatColumn]"
    case DoubleType => q"${compileTableRef(col.table)}.columns(${col.colidx}).asInstanceOf[db.DoubleColumn]"
  }

  def compileColRef(col: Col): Tree = {
    val Col(tableRef, colidx) = col
    q"${TermName(s"_${col.table.alias}$$column$$$colidx")}"
  }


  def compileBoolExpr(e: BoolExpr): Tree = e match {
    case True      => q"true"
    case False     => q"false"
    case Not(x)    => q"!${compileBoolExpr(x)}"
    case And(x, y) => q"${compileBoolExpr(x)} && ${compileBoolExpr(y)}"
    case Or(x, y)  => q"${compileBoolExpr(x)} || ${compileBoolExpr(y)}"
    case Rel(x, y, "==") => q"${compileExpr(x)} == ${compileExpr(y)}"
    case Rel(x, y, "!=") => q"${compileExpr(x)} != ${compileExpr(y)}"
    case Rel(x, y, ">")  => q"${compileExpr(x)} > ${compileExpr(y)}"
    case Rel(x, y, "<")  => q"${compileExpr(x)} < ${compileExpr(y)}"
  }

  def compileType(tpe: ColumnType) = tpe match {
    case IntType    => tq"Int"
    case LongType   => tq"Long"
    case FloatType  => tq"Float"
    case DoubleType => tq"Double"
  }


  def typeOf(e: Expr) = _traverse[ColumnType, ColumnType](e, {
    case c: Col => c.info.tpe
    case c: Count => IntType
    case c: Const => IntType // ???
    case o: Oid   => IntType
    case o: End   => IntType
    case l: Length => IntType
  }, (tpe, thisTpe) => if (tpe == null) thisTpe else widenType(tpe, thisTpe), null)

  def widenType(a: ColumnType, b: ColumnType): ColumnType = (a, b) match {
    case (IntType, IntType) => IntType

    case (FloatType, _) => FloatType
    case (_, FloatType) => FloatType

    case (DoubleType, _) => DoubleType
    case (_, DoubleType) => DoubleType

    case (LongType, _) => FloatType
    case (_, LongType) => FloatType
  }

}


object Rules {

  val stages = Seq(

    rewrite({
      case Filter(l: TableScan, Rel(col: Col, e: Const, "==")) if col.info.oids && l.table.hasCol(col) => OidLookup(l.table, e)
      case Filter(l: TableScan, Rel(e: Const, col: Col, "==")) if col.info.oids && l.table.hasCol(col) => OidLookup(l.table, e)

      case Filter(l: TableScan, Rel(c1: Col, c2: Col, "==")) if c1.info.oids && l.table.hasCol(c1) && !l.table.hasCol(c2) => OidLookup(l.table, c2)
      case Filter(l: TableScan, Rel(c1: Col, c2: Col, "==")) if c2.info.oids && !l.table.hasCol(c1) && l.table.hasCol(c2) => OidLookup(l.table, c1)
    }),

    rewrite({
      case Filter(l: TableScan, Rel(col: Col, e, op)) if col.info.oids =>
        Filter(l, Rel(Oid(l.table), e, op))

      case Filter(l: TableScan, Rel(e, col: Col, op)) if col.info.oids =>
        Filter(l, Rel(e, Oid(l.table), op))

//
//      case Filter(l: TableScan, Rel(o: Oid, e, ">")) =>
//        l.copy(start = l.start :+ e)
//
//      case Filter(l: TableScan, Rel(e, o: Oid, ">")) =>
//        l.copy(start = l.start :+ e)

      case Filter(l: TableScan, Rel(e, o: Oid, "<")) =>
        l.copy(end = l.end :+ e)

      case Filter(l: TableScan, Rel(o: Oid, e, "<")) =>
        l.copy(end = l.end :+ e)



      case Take(l: TableScan, limit, offset) =>
        l.copy(start = l.start :+ Const(offset), end = l.end :+ Const(limit + offset))

      case Filter(l: TableScan, Rel(col: Col, e, ">")) if col.info.ascending && col.info.unique =>
        l.copy(
          start = l.start :+ BinarySearch(col, e + Const(1), Const(0), !col.info.unique)
        )

      case Filter(l: TableScan, Rel(col: Col, e, "<")) if col.info.ascending && col.info.unique =>
        l.copy(
          end = l.end :+ BinarySearch(col, e, Oid(l.table), !col.info.unique)
        )

      case Filter(l: TableScan, Rel(col: Col, Const(const), "==")) if col.info.ascending && col.info.unique =>
        l.copy(
          start = l.start :+ BinarySearch(col, Const(const), Const(0), false),
          end   = l.end   :+ BinarySearch(col, Const(const), Const(0), false)
        )
    }),

    rewrite({
      case FlatMap(src, m @ Map(Filter(inner, Rel(srcCol: Col, innerCol: Col, "==")), row)) if colIsFrom(srcCol, src.row.tableRefs) && colIsFrom(innerCol, inner.row.tableRefs)  =>
        if (innerCol.info.unique) {
          HashLookup(src, Map(inner, row), srcCol, innerCol)
        } else {
          HashMultiLookup(src, Map(inner, row), srcCol, innerCol)
        }

//      case FlatMap(src, Filter(inner, Rel(innerCol: Col, srcCol: Col, "=="))) if src.row.table.hasCol(srcCol) && inner.row.table.hasCol(innerCol) =>
//        HashLookup(src, inner, srcCol, innerCol)

      case Take(Take(src, l2, o2), l1, o1) =>
        Take(src, math.min(l2, l1 - o2), o1 + o2)

      case Filter(Filter(src, expr2), expr1) =>
        Filter(src, And(expr1, expr2))
    })
  )

  def rewrite(f: PartialFunction[Src, Src]): Src => Src = {
    lazy val r: Src => Src = rewrite(f) ;
    {
      case e if f.isDefinedAt(e) => f(e)
      case t: TableScan => t
      case Take(s, l, o) => Take(r(s), l, o)
      case Filter(src, expr) => Filter(r(src), expr)
      case Map(src, row) => Map(r(src), row)
      case FlatMap(src, inner) => FlatMap(r(src), r(inner))
      case o: OidLookup => o
      case HashLookup(src, inner, srcExpr, innerExpr) =>
        HashLookup(r(src), r(inner), srcExpr, innerExpr)
      case HashMultiLookup(src, inner, srcExpr, innerExpr) =>
        HashMultiLookup(r(src), r(inner), srcExpr, innerExpr)
    }
  }


  def rewriteAll(l: Src): Src = {

    var last = l

    for (stage <- stages) {
      var stageConverged = false
      while (!stageConverged) {
        val updated = stage(last)
        if (updated == last) {
          stageConverged = true
        }
        last = updated
      }
    }

    last

  }



  /*
  def simplifyExpr(e: Expr) = e match {
    case Op(Const(a), Const(b), "+") => Const(a+b)
    case Op(Const(a), Const(b), "-") => Const(a-b)
    case Op(Const(a), Const(b), "*") => Const(a*b)
    case Op(Const(a), Const(b), "/") => Const(a/b)
    case c: Col if c.info.oids => Oid(c.tableRef)
  }
  */
}
