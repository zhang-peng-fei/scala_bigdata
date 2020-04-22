package flink

import java.util.Properties

import org.apache.log4j.Logger

object CommonPro {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def loadProperties: Properties = {
    val prop: Properties = loadSysProperties

    val properties = new Properties()
    if (prop.getProperty("profiles.active").equals("dev")) {
      properties.load(CommonPro.getClass.getClassLoader.getResourceAsStream("application-dev.properties"))
    } else if (prop.getProperty("profiles.active").equals("pro")) {
      properties.load(CommonPro.getClass.getClassLoader.getResourceAsStream("application-pro.properties"))
    }
    return properties
  }


  def loadSysProperties = {
    val prop = new Properties()
    val inputStream = CommonPro.getClass.getClassLoader.getResourceAsStream("application.properties")
    prop.load(inputStream);
    prop
  }

  def main(args: Array[String]): Unit = {
    val prop: Properties = loadSysProperties;
    logger.error(prop.getProperty("profiles.active"))
    val properties = new Properties()

    if (prop.getProperty("profiles.active").equals("dev")) {
      properties.load(CommonPro.getClass.getClassLoader.getResourceAsStream("application-dev.properties"))
    } else if (prop.getProperty("profiles.active").equals("pro")) {
      properties.load(CommonPro.getClass.getClassLoader.getResourceAsStream("application-pro.properties"))
    }

    val str = properties.getProperty("sasl.jaas.config")
    logger.info(str)
  }

}
