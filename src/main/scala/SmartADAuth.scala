package com.imvu.smartad

import S3Ops.{downloadObject, listS3ObjectsRecursively}

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.{headers, _}
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import SmartAdModels.{GetToken, Profile, Profiles, Token, UpdateBearerToken}

import spray.json.DefaultJsonProtocol._
import spray.json.{RootJsonFormat, enrichAny}

import java.io.File
import javax.net.ssl.SSLContext
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object SmartADAuth {

  val localS3FilePath: String = "/Users/agupta/IdeaProjects/SmartAdIntegration/s3files/"
  val dmpUrl: String = "https://dmp.smartadserverapis.com/segmentproviders"
  val imvuSegmentProvidersId: String = "183a1aed-351d-4d7e-9953-27fd9090a2dd"
  val s3SegmentDataBucket: String = "imvudata"
  val s3SegmentDataPrefix: String = "user/hive/warehouse/marketing.db/smartad_segment_data/"

  val localS3FilesDir = new java.io.File(localS3FilePath)

  implicit val profile: RootJsonFormat[Profile] = jsonFormat2(Profile)
  implicit val profiles: RootJsonFormat[Profiles] = jsonFormat1(Profiles)
  implicit val system: ActorSystem = ActorSystem("SmartAd")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val sslCtx: HttpsConnectionContext = ConnectionContext.httpsClient(SSLContext.getDefault)
  Http().setDefaultClientHttpsContext(sslCtx)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) sys.error("Please provide client_id and client_secret as program input")

    val smartADActor: ActorRef = system.actorOf(Props[SmartAdAuthActor], "smart-ad-auth-actor")

    println("Started Application")

    val clientId = args(0)
    val clientSecret = args(1)

    // Downloading segment files from s3
    downloadSegmentData()

    println("Old files cleared from s3 files download dir")

    val txtFiles: Iterator[File] = localS3FilesDir.listFiles.filter(_.getName.endsWith(".txt")).iterator

    val data = txtFiles.map(scala.io.Source.fromFile).flatMap(_.getLines())

    val dataGroup: data.GroupedIterator[String] = data.grouped(1000)

    val groupedProfile: Iterator[Profiles] = for {
      singleLoad <- dataGroup
    } yield Profiles(singleLoad.map { x =>
      val splitData = x.split(";")
      Profile(splitData(0), splitData(1).split(",").map(_.toInt).toList)
    })

    val segmentJson: Iterator[String] = groupedProfile.map(_.toJson.toString)

    implicit val timeout: Timeout = Timeout(2.second)

    system.scheduler.scheduleWithFixedDelay(
      delay = 10.minutes,
      initialDelay = 0.nanoseconds,
      receiver = smartADActor,
      message = UpdateBearerToken(clientId, clientSecret)
    )

    def bearer: Future[String] = (smartADActor ? GetToken).mapTo[Token].map(_.token)

    println("Starting Upload (it make take around 15-20 minutes) ...")

    val processStatus: Future[Done] = Source
      .fromIterator(() => segmentJson)
      .throttle(1, 1.second)
      .mapAsync(parallelism = 1)(segment => updateSegmentForProfile(segment, bearer))
      .run()

    processStatus.onComplete {
      case Success(_) =>
        println("Upload finished successfully!!")
        system.terminate()
      case Failure(exception) =>
        println(exception.getMessage)
        system.terminate()
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
      case ex: Exception => println(s"Couldn't clear the s3 files download dir\n${ex.getMessage}")
    }
    downloadObject(listS3ObjectsRecursively(S3Ops.listObjects(s3SegmentDataBucket, s3SegmentDataPrefix)))
  }
}
