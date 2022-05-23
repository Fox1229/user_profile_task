package com.atguigu.userprofile

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.constant.ConstCode
import com.atguigu.userprofile.dao.TagInfoDAO
import com.atguigu.userprofile.util.{MyCkUtil, MyPropertiesUtil}
import com.atguigu.userprofile.constant.ConstConfig._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.collection.mutable.ListBuffer

object TaskBitmapApp {

    def main(args: Array[String]): Unit = {

        // 本次数据处理主要在clickhouse进行，但是考虑到集群监控、管理、调度，伪装spark程序
        val conf: SparkConf = new SparkConf()/*.setAppName("task_bitmap_app")*/
        val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        val taskId: String = args(0)
        val executeDate: String = args(1)

        val tagInfoList: List[TagInfo] = TagInfoDAO.getTagCodeWithOn
        val stringTagList: ListBuffer[TagInfo] = new ListBuffer[TagInfo]
        val decimalTagList: ListBuffer[TagInfo] = new ListBuffer[TagInfo]
        val longTagList: ListBuffer[TagInfo] = new ListBuffer[TagInfo]
        val dateTagList: ListBuffer[TagInfo] = new ListBuffer[TagInfo]

        // 按照标签类型分类
        for (tagInfo <- tagInfoList) {
            tagInfo.tagValueType match {
                case ConstCode.TAG_VALUE_TYPE_STRING => stringTagList.append(tagInfo)
                case ConstCode.TAG_VALUE_TYPE_DECIMAL => decimalTagList.append(tagInfo)
                case ConstCode.TAG_VALUE_TYPE_LONG => longTagList.append(tagInfo)
                case ConstCode.TAG_VALUE_TYPE_DATE => dateTagList.append(tagInfo)
            }
        }

        val prop: Properties = MyPropertiesUtil.load("config.properties")
        // 获取数据库名称
        val dbName: String = prop.getProperty(USER_PROFILE_DBNAME)

        insertClickhouse(stringTagList, dbName, "user_tag_value_string", executeDate)
        insertClickhouse(decimalTagList, dbName, "user_tag_value_decimal", executeDate)
        insertClickhouse(longTagList, dbName, "user_tag_value_long", executeDate)
        insertClickhouse(dateTagList, dbName, "user_tag_value_date", executeDate)
    }

    /**
     * 写入数据到clickhouse
     *
     * @param tagInfoList 写数集合的类型
     * @param dbName      数据库名
     * @param targetTable 目标表
     * @param executeDate 执行日期
     */
    def insertClickhouse(tagInfoList: ListBuffer[TagInfo], dbName: String, targetTable: String, executeDate: String): Unit = {

        // 处理重复数据
        val dropPartitionSql: String = s"alter table $targetTable delete where dt = '$executeDate'"
        MyCkUtil.executeSql(dropPartitionSql)

        if (tagInfoList.nonEmpty) {

            val tagCodeValueSql: String =
                tagInfoList.map(tagInfo => s"('${tagInfo.tagCode.toLowerCase}', ${tagInfo.tagCode.toLowerCase})").mkString(", ")
            // 源表
            val srcTableName: String = s"user_tag_merge_${executeDate.replace("-", "")}"

            val insertSql: String =
                s"""
                   |insert into $dbName.$targetTable
                   |select
                   |    kv.1 k,
                   |    kv.2 v,
                   |    groupBitmapState(cast(uid as UInt64)) uids,
                   |    '$executeDate'
                   |from
                   |(
                   |    select
                   |         uid,
                   |         arrayJoin([$tagCodeValueSql]) kv
                   |    from $dbName.$srcTableName
                   |) t1
                   |where kv.2 <> '' and kv.2 is not null
                   |group by kv.1, kv.2
                   |""".stripMargin

            // println(insertSql)
            MyCkUtil.executeSql(insertSql)
        }
    }
}
