//package com.datasource
//
//import java.io.File
//
//import org.apache.spark.sql.catalyst.expressions.Attribute
////import java.text.AttributedCharacterIterator.Attribute
//
//import org.apache.calcite.avatica.ColumnMetaData.StructType
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, UnsafeProjection}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Strategy}
//import org.apache.spark.sql.catalyst.planning.{GenericStrategy, QueryPlanner}
//import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
//import org.apache.spark.sql.execution.{PlanLater, SparkPlan}
//import org.apache.spark.sql.execution.command.{ExecutedCommandExec, RunnableCommand}
//import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation}
//import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, SchemaRelationProvider}
//import org.apache.spark.unsafe.types.UTF8String
//
//object TestTextSql {
//  val conf = new SparkConf().setMaster("local[*]").setAppName(getClass.getCanonicalName)
//
//  val ss = SparkSession.builder().config(conf).getOrCreate()
//
//  def main(args: Array[String]): Unit = {
//    val sqlContext = ss.sqlContext
//    val sparkContext = ss.sparkContext
//    val ts = new Text4SQLContext(sparkContext, sqlContext)
//    ts.sql(
//      """create table test1(
//        |word string,
//        |num string
//        |) using external.datasource.TextSource
//        |options(
//        |path '/home/wpy/tmp/external_sql/test1'
//        |)
//      """.stripMargin)
//    ts.sql("select * from test1").show
//    print("=============================================\n")
//    ts.sql(
//      """create table test2(
//        |word string,
//        |num string
//        |) using external.datasource.TextSource
//        |options(
//        |path '/home/wpy/tmp/external_sql/test2'
//        |)
//      """.stripMargin)
//    ts.sql(
//      """
//        |insert into table test2
//        |select * from test1
//      """.stripMargin)
//    ts.sql("select * from test2 order by word").show
//  }
//
//
//  //-------------
//
//  //releation
//
//  case class TextRelation(sqlContext: SQLContext, schema: StructType, path: String) extends BaseRelation with InsertableRelation {
//    override def insert(data: DataFrame, overwrite: Boolean): Unit = {
//      if (!new File(path).exists())
//        data.rdd.map(_.mkString(",")).saveAsTextFile(path)
//    }
//  }
//
//
//  //source
//
//  class TextSource extends SchemaRelationProvider {
//    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
//      val path = parameters.getOrElse("path", "/home/wpy/tmp/external_sql/testSql")
//      TextRelation(sqlContext, schema, path)
//    }
//  }
//
//  //sql入口
//
//  class Text4SQLContext(sc: SparkContext, sqlContext: SQLContext){
//    sqlContext.experimental.extraStrategies = new TextStrategies().TextStrategy :: Nil
//    def sql(sqlText: String): DataFrame = {
//      sqlContext.sql(sqlText)
//    }
//  }
//
//
//  //-----plan
//
//  case class LogicalText(output: Seq[Attribute], path: String) extends LogicalPlan {
//    override def children: Seq[LogicalPlan] = Nil
//  }
//
//
//  case class PhysicalText(output: Seq[Attribute], path: String) extends SparkPlan {
//    override protected def doExecute(): RDD[InternalRow] = {
//      sparkContext.textFile(path).map { row =>
//        val fields = row.split(",").map(UTF8String.fromString)
//        UnsafeProjection.create(schema)(InternalRow.fromSeq(fields))
//      }
//    }
//    override def children: Seq[SparkPlan] = Nil
//  }
//
//
//  case class TextExecuteCommand(cmd: RunnableCommand) extends SparkPlan {
//    override protected def doExecute(): RDD[InternalRow] = {
//      ExecutedCommandExec(cmd).execute()
//    }
//    override def output: Seq[Attribute] = cmd.output
//    override def children: Seq[SparkPlan] = Nil
//  }
//
//
//  //Strategy
//
//  class TextStrategies extends QueryPlanner[SparkPlan] with PredicateHelper {
//
//    override def strategies: Seq[GenericStrategy[SparkPlan]] = TextStrategy :: Nil
//
//    object TextStrategy extends Strategy{
//      override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
//        plan match {
//          case LogicalText(output, path) => PhysicalText(output, path) :: Nil
//          case LogicalRelation(TextRelation(_, _, path), output, _) => PhysicalText(output, path) :: Nil
//          case i@InsertIntoTable(l@LogicalRelation(t: TextRelation, _, _), part, query, overwrite, false) if part.isEmpty =>
//            ExecutedCommandExec(InsertIntoDataSourceCommand(l, query, overwrite)) :: Nil
//          case _ => Nil
//        }
//      }
//    }
//    override protected def collectPlaceholders(plan: SparkPlan): Seq[(SparkPlan, LogicalPlan)] = {
//      plan.collect {
//        case placeholder@PlanLater(logicalPlan) => placeholder -> logicalPlan
//      }
//      // Nil
//    }
//    override protected def prunePlans(plans: Iterator[SparkPlan]): Iterator[SparkPlan] = {
//      plans
//    }
//  }
//
//
//
//
//}
