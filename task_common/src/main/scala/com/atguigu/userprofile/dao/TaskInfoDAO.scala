package com.atguigu.userprofile.dao

import com.atguigu.userprofile.bean.TaskInfo
import com.atguigu.userprofile.util.MyJdbcUtil

object TaskInfoDAO {

    /**
     * 根据taskId获取TaskInfo表信息
     */
    def getTaskInfoById(taskId: String): TaskInfo = {

        val sql: String =
            s"""
               |select
               |   id,
               |   task_name,
               |   task_status,
               |   task_comment,
               |   task_time ,
               |   task_type,
               |   exec_type,
               |   main_class,
               |   file_id,
               |   task_args,
               |   task_sql,
               |   task_exec_level,
               |   create_time
               |from task_info where id=$taskId
               |""".stripMargin
        val task: Option[TaskInfo] = MyJdbcUtil.queryOne(sql, classOf[TaskInfo], true)
        if (task != null) {
            val taskInfo: TaskInfo = task.get
            taskInfo
        } else {
            null
        }
    }
}
