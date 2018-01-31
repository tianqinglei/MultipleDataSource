package com.cti.multipledatasource

case class DataSource(dataSourceType: Int,
                      url: String,
                      sourceTable: String,
                      targetTable: String,
                      username: String,
                      password: String)
