import java.lang.Runtime
import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.sellmerfud.optparse.{OptionParser, OptionParserException}

object JDBCExport {
    var nCores = Runtime.getRuntime().availableProcessors() - 2
    case class Config(
        conn: String= "",
        file: String= "",
        writeMode: String = "append",
        tableName: String= "",
        user: String= "",
        password: String= "",
        nPartition: Int = nCores
    )

    def main(args: Array[String]) {
        // Not focusing on cluster execution yet. Using local!
        val spark = SparkSession.builder.appName("JDBC Exporter").master("local[*]").config("spark.executor.cores", nCores).getOrCreate()
        val config = try {
            new OptionParser[Config] {
                reqd[String]("-c", "--conn=<conn>", "Provide a jdbc connection string.")
                { (v, c) => c.copy(conn = v) }

                reqd[String]("-f", "--file=<file>", "Provide path to parquet file.")
                { (v, c) => c.copy(file = v) }

                reqd[String]("-t", "--table=<table>", "Table name on database connection.")
                { (v, c) => c.copy(tableName = v) }

                reqd[String]("-u", "--user=<user>", "Database username.")
                { (v, c) => c.copy(user = v) }

                reqd[String]("-p", "--password=<password>", "Database user password.")
                { (v, c) => c.copy(password = v) }

                optl[String]("-m", "--mode=<mode>", List("overwrite", "append"), "Overwrite or append to tabel?")
                { (v, c) => c.copy(writeMode = v getOrElse "append") }
            }.parse(args, Config())
        } catch {
            case e: OptionParserException => println(e.getMessage); sys.exit(1)
        }
        var extra_prop = new Properties()
        var df_write = spark.read.parquet(config.file).repartition(nCores).write
        if (config.writeMode == "overwrite") df_write = df_write.mode(SaveMode.Overwrite)
        extra_prop.put("user", config.user)
        extra_prop.put("password", config.password)
        df_write.option("numPartitions", config.nPartition).jdbc(config.conn, config.tableName, extra_prop)
    }
}
