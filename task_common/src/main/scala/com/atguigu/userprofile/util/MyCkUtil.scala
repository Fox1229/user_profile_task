package com.atguigu.userprofile.util

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties
import com.atguigu.userprofile.constant.ConstConfig._

object MyCkUtil {

    private val properties: Properties = MyPropertiesUtil.load("config.properties")
    val clickUrl: String = properties.getProperty(CLICKHOUSE_URL)

    def executeSql(sql: String): Unit = {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
        val connection: Connection = DriverManager.getConnection(clickUrl, null, null)
        val statement: Statement = connection.createStatement()
        statement.execute(sql)
        connection.close()
    }
}
