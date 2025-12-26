package cre.db

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}


object Database {

  def dataSource(
      host: String,
      port: Int,
      db: String,
      user: String,
      password: String
  ): HikariDataSource = {

    val config = new HikariConfig()
    config.setJdbcUrl(s"jdbc:postgresql://$host:$port/$db")
    config.setUsername(user)
    config.setPassword(password)
    config.setMaximumPoolSize(10)
    config.setConnectionTimeout(3000)

    new HikariDataSource(config)
  }
}
