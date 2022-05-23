package com.atguigu.userprofile.app

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.constant.ConstConfig._
import com.atguigu.userprofile.dao.TagInfoDAO
import com.atguigu.userprofile.util.MyPropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Properties

/**
 * 标签宽表
 */
object TaskMergeApp {

    def main(args: Array[String]): Unit = {

        // 创建运行环境
        val conf: SparkConf = new SparkConf().setAppName("task_merge_app")/*.setMaster("local[*]")*/
        val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val taskId: String = args(0)
        val executeDate: String = args(1)

        // 查询标签信息
        val tagInfoList: List[TagInfo] = TagInfoDAO.getTagCodeWithOn
        val tagCode: String = tagInfoList.map(tagInfo => s"${tagInfo.tagCode.toLowerCase()} String").mkString(",")

        val prop: Properties = MyPropertiesUtil.load("config.properties")
        // 画像库名
        val userProfileDbName: String = prop.getProperty(USER_PROFILE_DBNAME)
        // 画像库路径
        val userProfilePath: String = prop.getProperty(HDFS_STORE_PATH)
        // 画像库表名。hive中表明不能包含"-"
        val tableName: String = s"user_tag_merge_${executeDate.replace("-", "")}"

        // 创建标签宽表
        val createMergeTableSql: String =
            s"""
               |create table if not exists $userProfileDbName.$tableName
               |(
               |    uid String,
               |    $tagCode
               |)
               |row format delimited fields terminated by '\t'
               |location '$userProfilePath/$userProfileDbName/$tableName'
               |""".stripMargin
        // 创建之前先进行删除
        val dropMergeTableSql: String = s"drop table if exists $userProfileDbName.$tableName"
        sparkSession.sql(dropMergeTableSql)
        sparkSession.sql(createMergeTableSql)

        val tagWideSql: String = genInsertTableSql(tagInfoList, userProfileDbName, tableName, executeDate)
        sparkSession.sql(tagWideSql)
    }

    /**
     * 获取标签库宽表写入的SQL
     * @param tagInfoList 所有启动任务的标签信息
     * @param upDbName 画像库名
     * @param tableName 写入的表名
     * @param taskDate 执行时间
     */
    def genInsertTableSql(tagInfoList: List[TagInfo], upDbName: String, tableName: String, taskDate: String): String = {

        val tagValueTableList: List[String] = tagInfoList.map(
            tagInfo =>
                s"""
                   |select
                   |    uid,
                   |    query_value,
                   |    '${tagInfo.tagCode}' tag_name
                   |from $upDbName.${tagInfo.tagCode.toLowerCase}
                   |where dt = '$taskDate'
                   |""".stripMargin
        )
        val tagsSql: String = tagValueTableList.mkString(" union all ")

        // 旋转列
        val tagNameList: List[String] = tagInfoList.map(tagInfo => s"'${tagInfo.tagCode}'")
        val tagNameSql: String = tagNameList.mkString(",")

        val tagWideSql: String =
            s"""
               |insert overwrite table $upDbName.$tableName
               |select *
               |from ($tagsSql) t1
               |pivot(max(query_value) query_value for tag_name in ($tagNameSql))
               |""".stripMargin
        tagWideSql
    }
}
