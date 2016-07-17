/**
  * Created by wyh770406 on 16/6/20.
  */
import org.scalatest._
import org.scalatest.Matchers


class TrafficTest extends FlatSpec with Matchers{

  "This test" should "pass" in {
    true should be === true

    List(1,2,3,4) should have length(4)

    List.empty should be (Nil)

    Map(1->"Value 1", 2->"Value 2") should contain key (2)
    Map(1->"Java", 2->"Scala") should contain value ("Scala")

    Map(1->"Java", 2->"Scala") get 1 should be (Some("Java"))

    Map(1->"Java", 2->"Scala") should (contain key (2) and not contain value ("Clojure"))

    3 should (be > (0) and be <= (5))

    new java.io.File(".") should (exist)
  }

}

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class TrafficTests extends FunSuite with BeforeAndAfter {

  val sql_select_str = s"SELECT 'youku' AS platform, count(*) AS count"
  val sql_where_str = s"where d>=20160616 and d<=20160616"
  val sql_group_str = s"group by 'youku'"

  test("dynasql_select_str, dynasql_where_str, dynasql_group_str should be correct") {
    val (dynasql_select_str, dynasql_where_str, dynasql_group_str) = Traffic.getDynamicSql("youku", "0, 1", "", "", "", "", "'atv'", "day,platform,placementid,channel", sql_select_str, sql_where_str, sql_group_str)
    println(dynasql_select_str)
    println(dynasql_where_str)
    println(dynasql_group_str)
    assert(dynasql_select_str == "SELECT 'youku' AS platform, count(*) AS count, d AS day, placementid, channel")
    assert(dynasql_where_str == "where d>=20160616 and d<=20160616 and device_type in (0, 1) and placementid in ('atv')")
    assert(dynasql_group_str == "group by 'youku', d, placementid, channel")
  }

}

