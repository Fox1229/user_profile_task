package com.atguigu.userprofile.dao

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.util.MyJdbcUtil

object TagInfoDAO {

    /**
     * 根据标签开启的状态，查询所有的标签值
     */
    def getTagCodeWithOn: List[TagInfo] = {

        val sql: String =
            """
              |select t1.*
              |from tag_info t1
              |join task_info t2
              |on t1.tag_task_id = t2.id
              |where t2.task_status = '1'
              |""".stripMargin

        val tagInfoList: List[TagInfo] = MyJdbcUtil.queryList(sql, classOf[TagInfo], true)
        tagInfoList
    }

    /**
     * 通过taskId查询TagInfo信息
     */
    def getTagInfoByTaskId(taskId: String): TagInfo = {

        val sql: String =
            s"""select
               |    id,
               |    tag_code,
               |    tag_name,
               |    parent_tag_id,
               |    tag_type,
               |    tag_value_type,
               |    tag_value_limit,
               |    tag_task_id,
               |    tag_comment,
               |    create_time
               |from tag_info
               |where tag_task_id = $taskId
               |""".stripMargin
        val task: Option[TagInfo] = MyJdbcUtil.queryOne(sql, classOf[TagInfo], true)
        // 查询到任务信息
        if (task != null) {
            val tagInfo: TagInfo = task.get
            tagInfo
        } else {
            null
        }
    }
}
