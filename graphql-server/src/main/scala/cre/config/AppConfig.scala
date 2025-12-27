package cre.config

import com.typesafe.config.{Config, ConfigFactory}


case class DbConfig(
    host: String,
    port: Int,
    name: String,
    user: String,
    password: String
)

case class RedisConfig(
    host: String,
    port: Int
)

case class KafkaConfig(
    bootstrapServers: String
)

case class AppConfig(
    db: DbConfig,
    redis: RedisConfig,
    kafka: KafkaConfig,
    graphqlPath: String
)

object AppConfig {

  private val config: Config = ConfigFactory.load()

  val dbConfig: DbConfig = DbConfig(
    host = config.getString("db.host"),
    port = config.getInt("db.port"),
    name = config.getString("db.name"),
    user = config.getString("db.user"),
    password = config.getString("db.password")
  )

  val redisConfig: RedisConfig = RedisConfig(
    host = config.getString("redis.host"),
    port = config.getInt("redis.port")
  )

  val kafkaConfig: KafkaConfig = KafkaConfig(
    bootstrapServers = config.getString("kafka.bootstrapServers")
  )

  val graphqlPort: Option[Int] =
    if (config.hasPath("server.port")) Some(config.getInt("server.port"))
    else None
}
