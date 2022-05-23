package com.atguigu.userprofile.app

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.dao.{TagInfoDAO, TaskInfoDAO}
import com.atguigu.userprofile.util.{MyCkUtil, MyPropertiesUtil}
import com.atguigu.userprofile.constant.ConstConfig._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties

/**
 * 将画像库数据写入clickhouse
 */
object TaskExportCk {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("task_export_ck_app")/*.setMaster("local[*]")*/
        val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val taskId: String = args(0)
        val executeDate: String = args(1)

        val tagInfoList: List[TagInfo] = TagInfoDAO.getTagCodeWithOn
        val tagInfo: String = tagInfoList.map(tagInfo => s"${tagInfo.tagCode.toLowerCase} String").mkString(",")
        val prop: Properties = MyPropertiesUtil.load("config.properties")
        // 数据库url
        val clickUrl: String = prop.getProperty(CLICKHOUSE_URL)
        // 数据库名
        val ckAndUpDbName: String = prop.getProperty(USER_PROFILE_DBNAME)
        // 表名
        val tableName: String = s"user_tag_merge_${executeDate.replace("-", "")}"

        // 创建表SQL
        val createTableSql: String =
            s"""
               |create table if not exists $ckAndUpDbName.$tableName
               |(
               |    uid String,
               |    $tagInfo
               |)
               |engine=MergeTree()
               |order by uid
               |""".stripMargin

        // 删除表SQL
        val dropTableSql: String = s"drop table if exists $ckAndUpDbName.$tableName"

        // 执行SQL
        MyCkUtil.executeSql(dropTableSql)
        MyCkUtil.executeSql(createTableSql)

        // 写入数据
        val df: DataFrame = sparkSession.sql(s"select * from $ckAndUpDbName.$tableName")

        // 利用jdbc 算子写入clickhouse
        df.write
          .mode(SaveMode.Append)
          .option("batchsize", "1000")
          .option("isolationLevel", "NONE") // 关闭事务
          .option("numPartitions", "4") // 设置并发
          .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc(clickUrl, tableName, new Properties())
    }
}
