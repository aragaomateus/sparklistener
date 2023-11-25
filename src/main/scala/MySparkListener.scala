
import org.apache.logging.log4j.LogManager
import org.apache.spark.scheduler._
import org.apache.spark.SparkEnv

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import org.json4s._
import org.json4s.native.JsonMethods._
// import org.json4s.JsonMethods._




case class MyData(
  app_name: String,
  user_name: String, 
  app_id: String,
  start_time: String  
)
 
class MySparkListener extends SparkListener{
  private val logger = LogManager.getLogger(getClass.getName)
  private val httpClient = HttpClient.newHttpClient()

  private def sendDataToDruid(data: String): Unit = {
    // val jsonObj = JsonMethods.parse(data)


    val spec = s"""{
              "type": "index",
              "spec": {
                "dataSchema": {
                  "dataSource": "spark_logs",
                  "timestampSpec": {
                    "column": "time",
                    "format": "auto"
                  },
                  "dimensionsSpec": {
                    "dimensions": [
                      "app_name",
                      "user_name",
                      "app_id",
                      "start_time"
                    ]
                  },
                  "metricsSpec": [],
                  "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "rollup": false
                  }
                },
                "ioConfig": {
                      "type": "index",
                      "inputSource": {
                        "type": "inline",
                        "data": "name,user,1234,4321"
                      },
                      "inputFormat": {
                        "type": "csv",
                        "columns": ["app_name","user_name","id","attempt_id"],
                        "findColumnsFromHeader": true,
                      "skipHeaderRows": 1}  
                    },
                "tuningConfig": {
                  "type": "index"
                }
              }
            }"""


    val request = HttpRequest.newBuilder()
    .uri(URI.create("http://localhost:8081/druid/indexer/v1/task")) // Replace with your Druid ingestion endpoint
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(spec, StandardCharsets.UTF_8))
      .build()

    try {
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() == 200) {
        logger.info("Data sent to Druid successfully.")
      } else {
        logger.error(s"Failed to send data to Druid: ${response.body()}")
      }
    } catch {
      case e: Exception => logger.error("Error sending data to Druid", e)
    }
  }

  var location = ""
  override def onApplicationStart(appStart:SparkListenerApplicationStart): Unit = {
    val sparkConf = SparkEnv.get.conf
    location = sparkConf.get("spark.druidLocation")
    println("--------------   IT Started -----------------")
    val information = s"""{"app_name": "${appStart.appName}",
                          "user_name": "${appStart.sparkUser}", 
                          "app_id": "${appStart.appId.toString()}",
                          "start_time": ${appStart.time}}
                          """.stripMargin
    logger.info(information)
    // sendDataToDruid(information)


  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit ={
    print(location)
    logger.info(s"=============${taskEnd.taskInfo.taskId}=============")
    println(taskEnd.taskInfo.taskId)
    println(taskEnd.taskInfo.attemptNumber)
    println(taskEnd.taskInfo)


    println("info:", taskEnd.taskInfo)
    
      
    // taskEnd.taskInfo.attemptNumber
    // taskEnd.taskInfo.finishTime
    
    
  }

  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd): Unit = {
    println("--------------   IT ENDED -----------------")
    logger.info(appEnd.time)
    // appStart.driverAttributes.foreach(item => println(item))

  }

}