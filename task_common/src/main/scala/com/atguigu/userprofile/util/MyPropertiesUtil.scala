package com.atguigu.userprofile.util

import java.io.InputStreamReader
import java.util.Properties

object MyPropertiesUtil {

    def main(args: Array[String]): Unit = {
        val properties: Properties = MyPropertiesUtil.load("config.properties")
        println(properties.getProperty("mysql.url"))
    }

    def load(propertiesName: String): Properties = {
        val prop: Properties = new Properties();
        prop.load(
            new InputStreamReader(
                Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
                "UTF-8")
        )
        prop
    }
}
