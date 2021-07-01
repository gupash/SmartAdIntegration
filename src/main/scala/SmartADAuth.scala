package com.imvu.smartad

import S3Ops.{downloadObject, listS3ObjectsRecursively}
import SmartAdModels._

import akka.actor.{ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.model.{headers, _}
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import spray.json.DefaultJsonProtocol._
import spray.json.{RootJsonFormat, enrichAny}

import java.io.File
import javax.net.ssl.SSLContext
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object SmartADAuth {

  val log: Logger = LoggerFactory.getLogger(SmartADAuth.getClass)

  val localS3FilePath: String = "/Users/agupta/IdeaProjects/SmartAdIntegration/s3files/"
  val dmpUrl: String = "https://dmp.smartadserverapis.com/segmentproviders"
  val imvuSegmentProvidersId: String = "183a1aed-351d-4d7e-9953-27fd9090a2dd"
  val s3SegmentDataBucket: String = "imvudata"
  val s3SegmentDataPrefix: String = "user/hive/warehouse/marketing.db/smartad_segment_data/"

  val customConf: Config = ConfigFactory.parseString("""
  akka {
    http.host-connection-pool {
      max-connections = 4
    }
    http.client.idle-timeout = 60s
  }""".stripMargin)

  val localS3FilesDir = new java.io.File(localS3FilePath)

  implicit val profile: RootJsonFormat[Profile] = jsonFormat2(Profile)
  implicit val profiles: RootJsonFormat[Profiles] = jsonFormat1(Profiles)
  implicit val system: ActorSystem = ActorSystem("SmartAd", ConfigFactory.load(customConf))
  implicit val mat: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContextExecutor = mat.executionContext

  // Setting https protocol
  val sslCtx: HttpsConnectionContext = ConnectionContext.httpsClient(SSLContext.getDefault)
  Http().setDefaultClientHttpsContext(sslCtx)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) sys.error("Please provide client_id and client_secret as program input")
    val smartADActor: ActorRef = system.actorOf(Props[SmartAdAuthActor], "smart-ad-auth-actor")
    log.info("Started Application")

    val clientId = args(0)
    val clientSecret = args(1)

    // Downloading segment files from s3
    //downloadSegmentData()

    log.info("Old files cleared from s3 files download dir")

    val txtFiles: Iterator[File] = localS3FilesDir.listFiles.filter(_.getName.endsWith(".txt")).iterator
    val data: Iterator[String] = txtFiles.map(scala.io.Source.fromFile).flatMap(_.getLines())
    val dataGroup: data.GroupedIterator[String] = data.grouped(1000)

    val groupedProfile: Iterator[Profiles] = for {
      singleLoad <- dataGroup
    } yield Profiles(singleLoad.map { x =>
      val splitData = x.split(";")
      Profile(splitData(0), splitData(1).split(",").map(_.toInt).toList)
    })

    val segmentJson: Iterator[String] = groupedProfile.map(_.toJson.toString)

    // Timeout for the smart AD auth actor to return response within
    implicit val timeout: Timeout = Timeout(2.second)

    // Scheduler which runs the update bearer token actor every 10 minutes
    system.scheduler.scheduleWithFixedDelay(
      delay = 10.minutes,
      initialDelay = 0.nanoseconds,
      receiver = smartADActor,
      message = UpdateBearerToken(clientId, clientSecret)
    )

    def bearer: Future[String] = (smartADActor ? GetToken).mapTo[Token].map(_.token)
    log.info("Starting Upload (it may take around 15-20 minutes) ...")

    /* Main part where we are creating akka source from file iterator and uploading 1 row (1000 profiles)
     * in 1 future as there is a limit on smartAd side to upload max 1000 profiles per second.
     * Finally we are counting the total operations
     */
    val processStatus: Future[Int] = Source
      .fromIterator(() => segmentJson)
      .throttle(1, 1.seconds)
      .mapAsyncUnordered(parallelism = 4)(segment => updateSegmentForProfile(segment, bearer))
      .map(_ => 1)
      .runFold(0)(_ + _)

    // Captures time taken by the upload segment future for all profiles
    timedFuture(processStatus)

    processStatus.onComplete { res =>
      res match {
        case Success(count) =>
          log.info(s"Processed $count rows (each row contains 1000 profiles except may be the last one)")
          log.info("Upload finished successfully!!")
        case Failure(exception) =>
          log.error(exception.getMessage)
      }
      shutDownHook
    }
  }

  private def updateSegmentForProfile(segmentJson: String, bearer: Future[String]): Future[HttpResponse] = {
    // This step is needed as the json being passed to smartAD
    // is not a well formed json and smarts with `[` and ends on `]`
    val truncatedJson = segmentJson.drop(12).dropRight(1)

    val updateProfileFuture: Future[HttpResponse] = bearer.flatMap { token =>
      Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.POST,
            uri = s"$dmpUrl/$imvuSegmentProvidersId/profiles",
            entity = HttpEntity(ContentTypes.`application/json`, truncatedJson),
            headers = Seq(headers.RawHeader("Authorization", s"Bearer $token"))
          )
        )
        .map {
          case res: HttpResponse if res.status.isSuccess => res
          case res: HttpResponse if res.status.isFailure =>
            throw new Exception(s"Cannot process request due to ${res.status.intValue} status")
        }
    }

    updateProfileFuture
  }

  def downloadSegmentData(): Unit = {
    try {
      localS3FilesDir.listFiles().map(_.delete())
    } catch {
      case ex: Exception => log.error(s"Couldn't clear the s3 files download dir\n${ex.getMessage}")
    }
    downloadObject(listS3ObjectsRecursively(S3Ops.listObjects(s3SegmentDataBucket, s3SegmentDataPrefix)))
  }

  def timedFuture[T](future: Future[T]): Future[T] = {
    val start = System.currentTimeMillis()
    future.onComplete(_ => log.info(s"Future took ${System.currentTimeMillis() - start} ms"))
    future
  }

  def shutDownHook: Future[Terminated] =
    Http().shutdownAllConnectionPools().flatMap { _ =>
      mat.shutdown()
      system.terminate()
    }
}
