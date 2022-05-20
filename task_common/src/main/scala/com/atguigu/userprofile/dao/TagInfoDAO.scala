package com.atguigu.userprofile.dao

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.util.MyJdbcUtil

object TagInfoDAO {

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
