package com.atguigu.userprofile.app

import com.atguigu.userprofile.bean.{TagInfo, TaskInfo, TaskTagRule}
import com.atguigu.userprofile.constant.ConstCode
import com.atguigu.userprofile.dao.{TagInfoDAO, TaskInfoDAO, TaskTagRuleDAO}
import com.atguigu.userprofile.util.MyPropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties
import com.atguigu.userprofile.constant.ConstConfig._

object TaskSQLApp {

    def main(args: Array[String]): Unit = {

        // 创建spark环境
        // 集群执行注释master
        val conf: SparkConf = new SparkConf().setAppName("user_task_app")/*.setMaster("local[*]")*/
        val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        // 程序怎么知道现在要计算哪个标签？
        // 平台会把每个任务的任务id ，放到json中，远程提交器会把该任务id作为spark-submit的参数,传递到主函数中,
        // 可以通过取得主函数的args的第一个参数作为task_id 来查询对应的标签和定义
        val taskId: String = args(0)
        // 任务执行时间
        val executeDate: String = args(1)

        // tagInfo信息
        val tagInfo: TagInfo = TagInfoDAO.getTagInfoByTaskId(taskId)
        // taskInfo信息
        val taskInfo: TaskInfo = TaskInfoDAO.getTaskInfoById(taskId)
        // taskTagRule的集合
        val taskTagRuleList: List[TaskTagRule] = TaskTagRuleDAO.getTaskTagRuleByTaskId(taskId)

        // 读取配置信息
        val properties: Properties = MyPropertiesUtil.load("config.properties")
        // 数仓库
        val dwDbName: String = properties.getProperty(DATA_WAREHOUSE_DBNAME)
        // 用户画像库
        val userProfileDbName: String = properties.getProperty(USER_PROFILE_DBNAME)
        // 表存储位置
        val userProfileLocation: String = properties.getProperty(HDFS_STORE_PATH)
        // 表名
        val tableName: String = tagInfo.tagCode.toLowerCase
        // 标签等级
        val tagValueType: String = tagInfo.tagValueType match {
            case ConstCode.TAG_VALUE_TYPE_LONG => "BIGINT"
            case ConstCode.TAG_VALUE_TYPE_DECIMAL => "DECIMAL(16,2)"
            case ConstCode.TAG_VALUE_TYPE_STRING => "STRING"
            case ConstCode.TAG_VALUE_TYPE_DATE => "STRING"
        }

        // 删除表SQL
        val dropTableSql: String = s"drop table if exists $userProfileDbName.$tableName"
        // 创建画像表
        val createTableSql: String =
            s"""
               |create table if not exists $userProfileDbName.$tableName
               |(
               |     uid String,
               |     query_value $tagValueType
               |)
               |partitioned by(dt String)
               |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
               |location '$userProfileLocation/$userProfileDbName/$tableName'
               |""".stripMargin
        sparkSession.sql(dropTableSql)
        sparkSession.sql(createTableSql)

        val taskSql: String = taskInfo.taskSql.replaceAll("executeDate", executeDate)
        // 从数仓查询的SQL
        var selectSql: String = null
        if (taskTagRuleList.nonEmpty) {
            // 有四级标签
            // 查询的SQL
            if (tableName == "tag_person_base_cons") {
                // 星座表
                // 范围值的SQL
                val caseWhenList: List[String] = taskTagRuleList.map(
                    taskTagRule => s"when query_value ${taskTagRule.queryValue} then '${taskTagRule.subTagValue}'"
                )
                val caseWhen: String = caseWhenList.mkString(" ")
                val caseWhenSql: String = s"case $caseWhen end as tag_value"
                // 从数仓查询数据
                selectSql = s"select uid, $caseWhenSql from ($taskSql) t1"
            } else {
                // 普通值SQL
                val caseWhenList: List[String] = taskTagRuleList.map(
                    taskTagRule => s"when '${taskTagRule.queryValue}' then '${taskTagRule.subTagValue}'"
                )
                val caseWhen: String = caseWhenList.mkString(" ")
                val caseWhenSql: String = s"case query_value $caseWhen end as tag_value"
                // 从数仓查询数据
                selectSql = s"select uid, $caseWhenSql from ($taskSql) t1"
            }
        } else {
            // 没有四级标签
            selectSql = taskSql
        }

        // 写入的SQL
        val insertSql: String =
            s"""
               |insert overwrite table $userProfileDbName.$tableName
               |partition(dt = '$executeDate')
               |$selectSql
               |""".stripMargin

          // println(selectSql)
          // println(insertSql)

        // 默认从数仓库查询数据
        sparkSession.sql(s"use $dwDbName")
        // 写数画像库指定的表中
        sparkSession.sql(insertSql)
    }
}
