import java.io.File
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Try

object App extends App {
  val directory = "files/"
  val parsed = Parser.parseDirectory(directory)

  println(s"Number of processed files: ${parsed.size}")
  val processed = Parser.process(parsed.iterator.flatten)
  println(s"Number of processed measurements: ${processed.numberOfReadings}")
  println(s"Number of failed measurements: ${processed.numberOfFailed}")
  println()
  println("Sensors with highest avg humidity:")
  println()
  println("sensor-id,min,avg,max")
  processed.stats.toVector
    .sorted(Ordering.by[(String, Stats), (Boolean, Byte)] { ord =>
      (ord._2.average.isDefined, ord._2.average.getOrElse(0))
    }.reverse)
    .foreach { case (name, stats) =>
      if (stats.average.isEmpty)
        println(s"$name,NaN,NaN,NaN")
      else println(s"$name,${stats.min},${stats.average},${stats.max}")
    }
}

case class Reading(sensorName: String, valueOpt: Option[Byte])

case class Counters(var min: Option[Byte] = None,
                    var sum: Long = 0,
                    var max: Option[Byte] = None,
                    var numFailed: Long = 0,
                    var numTotal: Long)

case class Stats(var min: Option[Byte],
                 var average: Option[Byte],
                 var max: Option[Byte])

case class Result(stats: Map[String, Stats], numberOfReadings: Long, numberOfFailed: Long)

object Parser {

  def process(it: Iterator[Reading]): Result = {
    val counters = collection.mutable.Map[String, Counters]()
    var totalNumber = 0L
    var failedNumber = 0L
    it.foreach { reading =>
      counters.updateWith(reading.sensorName) { oldCountersOpt =>
        Some(
          oldCountersOpt.fold {
            // no counter exists for a sensorName
            reading.valueOpt.fold {
              Counters(numFailed = 1, numTotal = 1)
            } { readingValue =>
              Counters(min = Some(readingValue), sum = readingValue, max = Some(readingValue), numTotal = 1)
            }
          } { oldCounters =>
            reading.valueOpt.fold {
              // reading is NaN
              oldCounters.numFailed += 1
              oldCounters.numTotal += 1
              oldCounters
            } { readingValue =>
              oldCounters.min = oldCounters.min.fold {
                // all previous readings were NaN
                Some(readingValue)
              } { oldMin => Some(Math.min(oldMin, readingValue).toByte) }
              oldCounters.sum += readingValue
              oldCounters.max = oldCounters.max.fold {
                // all previous readings were NaN
                Some(readingValue)
              } { oldMax => Some(Math.max(oldMax, readingValue).toByte) }

              oldCounters.numTotal += 1
              oldCounters
            }
          })
      }
      if (reading.valueOpt.isEmpty)
        failedNumber += 1
      totalNumber += 1
    }
    val stats = counters.map { case (key, value) =>
      key -> {
        Stats(
          min = value.min,
          average = if (value.numTotal > value.numFailed) Some(
            (value.sum / (value.numTotal - value.numFailed)).toByte
          ) else None,
          max = value.max
        )
      }
    }
    Result(stats.toMap, totalNumber, failedNumber)
  }

  def parseDirectory(directoryName: String): Iterable[Iterator[Reading]] = {
    Files.list(Path.of(directoryName))
      .toList().asScala
      .map(_.toFile)
      .filter { file =>
        file.isFile && file.getName.endsWith(".csv")
      }
      .map(parseFile)
  }

  def parseFile(file: File): Iterator[Reading] = {
    scala.io.Source.fromFile(file, "UTF-8")
      .getLines()
      .drop(1) // dropping header
      .map(parseLine)
  }

  def parseLine(line: String): Reading = {
    val (name :: valueStr :: _) = line.split(',').toList
    val valueOpt = Try {
      java.lang.Byte.parseByte(valueStr)
    }.toOption
    Reading(name, valueOpt)
  }
}

