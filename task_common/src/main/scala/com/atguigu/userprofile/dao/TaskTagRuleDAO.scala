package com.atguigu.userprofile.dao

import com.atguigu.userprofile.bean.TaskTagRule
import com.atguigu.userprofile.util.MyJdbcUtil

object TaskTagRuleDAO {

    /**
     * 根据taskId获取TaskTagRule表信息
     */
    def getTaskTagRuleByTaskId(taskId: String): List[TaskTagRule] = {

        val sql: String =
            s"""select
               |    tr.id,
               |    tr.tag_id,
               |    tr.task_id,
               |    tr.query_value,
               |    sub_tag_id,
               |    ti.tag_name as sub_tag_value
               |from
               |    task_tag_rule tr,
               |    tag_info ti
               |where tr.sub_tag_id = ti.id and tr.task_id = $taskId
               |""".stripMargin
        val taskTagRuleList: List[TaskTagRule] = MyJdbcUtil.queryList(sql, classOf[TaskTagRule], true)
        taskTagRuleList
    }
}
