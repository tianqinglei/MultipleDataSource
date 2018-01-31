package com.cti.multipledatasource


import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

object QueryRegister extends Logging {

    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("QueryRegister")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)

        parse(sqlContext)
        sc.stop()
    }

    def getMySQLMetaData(sqlContext: SQLContext) = {
        val url = sqlContext.getConf("spark.sql.query.datasource.url",
            "jdbc:mysql://localhost:3306/ext_source?user=root&password=123456")

        val dataSourceTable = sqlContext.getConf("spark.sql.query.datasource.table",
            "ExDataSource")

        val dataSources = new ListBuffer[DataSource]()

        var conn: Connection = null
        var stat: Statement = null
        var rs: ResultSet = null

        try {
            conn = DriverManager.getConnection(url)
            stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
            rs = stat.executeQuery("SELECT * FROM " + dataSourceTable)
            while (rs.next()) {
                val url = rs.getString("URL")
                val username = rs.getString("USERNAME")
                val password = rs.getString("PASSWORD")
                val sourceTable = rs.getString("SOURCE_TABLE")
                val targetTable = rs.getString("TARGET_TABLE")
                val dataSourceType = rs.getString("DATASOURCE_TYPE")

                dataSources += DataSource(dataSourceType.toInt, url, sourceTable, targetTable, username, password)
            }
        } finally {
            rs.close()
            stat.close()
            conn.close()
        }
        dataSources
    }


    def parse(sqlContext: SQLContext): Unit = {
        val dataSources = getMySQLMetaData(sqlContext)

        for (dataSource <- dataSources) {
            dataSource.dataSourceType match {
                case 2 => {
                    logInfo("load mysql:" + dataSource.sourceTable + " to " + dataSource.targetTable + " start...")
                    sqlContext.read.format("jdbc").options(
                        Map(
                            "url" -> dataSource.url,
                            "dbtable" -> dataSource.sourceTable,
                            "driver" -> "com.mysql.jdbc.Driver",
                            "user" -> dataSource.username,
                            "password" -> dataSource.password))
                        .load.registerTempTable(dataSource.targetTable)
                    logInfo("load mysql:" + dataSource.sourceTable + " to " + dataSource.targetTable + " success...")
                }

                //json
                case 5 =>
                    logInfo("load json:" + dataSource.url + " to " + dataSource.targetTable + " start...")
                    sqlContext.read.format("json").load(dataSource.url).registerTempTable(dataSource.targetTable)
                    logInfo("load json:" + dataSource.url + " to " + dataSource.targetTable + " success...")

                case _ =>
            }
        }
    }
}