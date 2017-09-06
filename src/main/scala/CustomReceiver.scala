import java.sql.{DriverManager, ResultSet}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  override def onStart(): Unit = {
    Future {
      val url = "jdbc:postgresql://localhost:5432/knoldus_db"
      val driver = "org.postgresql.Driver"
      val username = "postgres"
      val password = "aakash06"
      val connection = try {
        Class.forName(driver)
        DriverManager.getConnection(url, username, password)
      }
      catch {
        case exception: Exception => throw new Exception(s"Connection Interrupted $exception")
      }
      val statement = connection.createStatement()

      val tableData: ResultSet = statement.executeQuery("select * from streaming")

      while (tableData.next()) {
        store(tableData.getString("name"))
      }
      connection.close()
      restart("Restarting Custom Receiver ")

    }
  }
  override def onStop(): Unit = {

  }
}