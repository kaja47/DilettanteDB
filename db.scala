/* dilettanteDB - "database" from dilettantes to dilettantes */
/* more like prototype of prototype of experiment */

package db

import scala.language.postfixOps



case class Column(data: Array[Int])

case class Aggregate(a: Array[Aggr], columns: Array[Column], select: BoolExpr, groupBy: Expr = null, orderBy: Expr = null)

sealed trait Aggr {
	def init = 0
	def e: Expr
}

case class Use(e: Expr) extends Aggr
case class Sum(e: Expr) extends Aggr
case class Count(e: Expr = Const(1)) extends Aggr
case class Max(e: Expr) extends Aggr { override val init = Int.MinValue }
case class Min(e: Expr) extends Aggr { override val init = Int.MaxValue }


sealed trait Expr

case class Const(value: Int) extends Expr
case class Col(idx: Int) extends Expr
case class Plus(x: Expr, y: Expr) extends Expr
case class Minus(x: Expr, y: Expr) extends Expr
case class Times(x: Expr, y: Expr) extends Expr
case class Func1(f: Int => Int, x: Expr) extends Expr
case class Func2(f: (Int, Int) => Int, y: Expr) extends Expr


sealed trait BoolExpr

case object True extends BoolExpr
case class Not(x: BoolExpr) extends BoolExpr
case class And(x: BoolExpr, y: BoolExpr) extends BoolExpr
case class Or(x: BoolExpr, y: BoolExpr) extends BoolExpr
case class Eq(x: Expr, y: Expr) extends BoolExpr
case class Ne(x: Expr, y: Expr) extends BoolExpr
case class Gt(x: Expr, y: Expr) extends BoolExpr
case class Lt(x: Expr, y: Expr) extends BoolExpr



object Compile {

	import scala.reflect.runtime.currentMirror
	import scala.tools.reflect.ToolBox
	val toolbox = currentMirror.mkToolBox()
	import toolbox.u.{Expr => _, _}
	import toolbox.{ u => universe }

	def compileAggr(a: Aggregate): Array[Column] => Seq[Array[Int]] = {
		val Aggregate(as, columns, select, groupBy, orderBy) = a

		val counterFields = for (i <- 0 until as.length) yield q"var ${TermName(s"_counter_$i")} = ${as(i).init}"
		val columnFields  = for (i <- 0 until columns.length) yield q"val ${TermName(s"_column_$i")} = columns($i).data"
		def iter(body: universe.Tree) = q"""
				var idx = 0
				val len = columns.head.data.length
				while (idx < len) {
					if (${compileBoolExpr(select)}) {
						$body
					}
					idx += 1
				}
		"""

		def counterVar(i: Int): TermName = TermName(s"_counter_$i")

		val qq =
		if (groupBy == null) {
			q"""
				(columns: Array[db.Column]) => {

					..$counterFields
					..$columnFields

					${iter(q"""
						..${ for (i <- 0 until as.length) yield compileAggrCounter(as(i), q"${counterVar(i)}") }
					""")}

					val res = Array[Int](..${ for (i <- 0 until as.length) yield counterVar(i) })
					Seq(res)
				}
			"""
		} else {
			q"""
				(columns: Array[db.Column]) => {

					class Counters {
						..$counterFields
					}

					val m = collection.mutable.Map[Int, Counters]()
					//val m = Array.fill[Counters](100)(new Counters)

					..$columnFields

					${iter(q"""
						val c = m.getOrElseUpdate(${compileExpr(groupBy)}, new Counters)
						..${ for (i <- 0 until as.length) yield compileAggrCounter(as(i), q"c.${counterVar(i)}") }
					""")}

					val res = m.values map { c =>
						Array[Int](..${ for (i <- 0 until as.length) yield q"c.${counterVar(i)}" })
					} toSeq

					res
				}
			"""
		}

		println(showCode(qq))
		toolbox.compile(qq)().asInstanceOf[Array[Column] => Seq[Array[Int]]]
	}

	def compileAggrCounter(a: Aggr, c: universe.Tree) = a match {
		case Use(e)   => q"$c = ${compileExpr(e)}"
		case Sum(e)   => q"$c += ${compileExpr(e)}"
		case Count(e) => q"$c += 1"
		case Min(e)   => q"if (${compileExpr(e)} < $c) $c = ${compileExpr(e)}"
		case Max(e)   => q"if (${compileExpr(e)} > $c) $c = ${compileExpr(e)}"
	}

	def compileExpr(e: Expr): universe.Tree = e match {
		case Const(x)    => q"$x"
		//case Col(colidx) => q"columns($colidx).data(idx)"
		case Col(colidx) => q"${TermName(s"_column_$colidx")}(idx)"
		case Plus(x, y)  => q"${compileExpr(x)} + ${compileExpr(y)}"
		case Minus(x, y) => q"${compileExpr(x)} - ${compileExpr(y)}"
		case Times(x, y) => q"${compileExpr(x)} * ${compileExpr(y)}"
	}

	def compileBoolExpr(e: BoolExpr): universe.Tree = e match {
		case True      => q"true"
		case Not(x)    => q"!${compileBoolExpr(x)}"
		case And(x, y) => q"${compileBoolExpr(x)} && ${compileBoolExpr(y)}"
		case Or(x, y)  => q"${compileBoolExpr(x)} || ${compileBoolExpr(y)}"
		case Eq(x, y)  => q"${compileExpr(x)} == ${compileExpr(y)}"
		case Ne(x, y)  => q"${compileExpr(x)} != ${compileExpr(y)}"
		case Gt(x, y)  => q"${compileExpr(x)} > ${compileExpr(y)}"
		case Lt(x, y)  => q"${compileExpr(x)} < ${compileExpr(y)}"
	}

}


object Eval {

	def evalAggr(a: Aggregate): Array[Int] = {
		val Aggregate(as, columns, select, groupBy, orderBy) = a
		require(groupBy == null)
		val counters = as map mkAggrCounter
		val len = columns.head.data.length
		var idx = 0
		while (idx < len) {
			if (evalBoolExpr(select, columns, idx)) {
				var cidx = 0
				while (cidx < counters.length) {
					counters(cidx).add(evalExpr(as(cidx).e, columns, idx))
					cidx += 1
				}
			}
			idx += 1
		}
		counters map (_.result)
	}

	abstract class AggrCounter(init: Int) {
		protected var x: Int = init
		def add(value: Int): Unit
		def result: Int = x
	}

	def mkAggrCounter(a: Aggr) = a match {
		case a @ Sum(e)   => new AggrCounter(a.init) { def add(v: Int) = x += v }
		case a @ Count(e) => new AggrCounter(a.init) { def add(v: Int) = x += 1 }
		case a @ Min(e)   => new AggrCounter(a.init) { def add(v: Int) = if (v < x) x = v }
		case a @ Max(e)   => new AggrCounter(a.init) { def add(v: Int) = if (v > x) x = v }
	}


	def evalExpr(e: Expr, columns: Array[Column], idx: Int): Int = e match {
		case Const(x) => x
		case Col(colidx) => columns(colidx).data(idx)
		case Plus(x, y) => evalExpr(x, columns, idx) + evalExpr(y, columns, idx)
		case Minus(x, y) => evalExpr(x, columns, idx) - evalExpr(y, columns, idx)
		case Times(x, y) => evalExpr(x, columns, idx) * evalExpr(y, columns, idx)
	}

	def evalBoolExpr(e: BoolExpr, columns: Array[Column], idx: Int): Boolean = e match {
		case True => true
		case Not(x) => !evalBoolExpr(x, columns, idx)
		case And(x, y) => evalBoolExpr(x, columns, idx) && evalBoolExpr(y, columns, idx)
		case Or(x, y) => evalBoolExpr(x, columns, idx) || evalBoolExpr(y, columns, idx)
		case Eq(x, y) => evalExpr(x, columns, idx) == evalExpr(y, columns, idx)
		case Ne(x, y) => evalExpr(x, columns, idx) != evalExpr(y, columns, idx)
		case Gt(x, y) => evalExpr(x, columns, idx) > evalExpr(y, columns, idx)
		case Lt(x, y) => evalExpr(x, columns, idx) < evalExpr(y, columns, idx)
	}

}



object App extends App {

	val len = 10000000
	val columns: Array[Column] = Array(
		Column(0 until len toArray),
		Column(Array.fill(len)(util.Random.nextInt)),
		Column(Array.fill(len)(util.Random.nextInt(100))),
		Column(0 until len toArray)
	)

	println("running")


	val aggr = Aggregate(
		Array(Use(Col(2)), Count(), Sum(Col(0)), Max(Col(1)), Min(Col(2)), Sum(Plus(Col(3), Const(1000)))),
		columns,
		And(Ne(Col(2), Col(3)), Lt(Col(1), Const(10000000))),
		groupBy = Col(2)
	)


	/*
	{
		val s = System.currentTimeMillis
		for (_ <- 0 until 2) {
			val res = Eval.evalAggr(aggr)
			print(res.length)
		}

		println()
		println(Eval.evalAggr(aggr).toSeq)

		println(System.currentTimeMillis - s)
	}
	*/

	{
		val sc = System.currentTimeMillis
		val f = Compile.compileAggr(aggr)
		println("compilation time: "+(System.currentTimeMillis - sc))

		val s = System.currentTimeMillis
		for (_ <- 0 until 200) {
			val s = System.currentTimeMillis
			val res = f(columns)
			print(res.length)
			println()
			println(System.currentTimeMillis - s)
		}

		println()
		println(f(columns).map(_.toSeq))

		println(System.currentTimeMillis - s)
	}

}
