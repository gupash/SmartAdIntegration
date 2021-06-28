package com.imvu.smartad

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{headers, _}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import S3Ops.{downloadObject, listS3ObjectsRecursively}

import spray.json.DefaultJsonProtocol.{jsonFormat3, _}
import spray.json.{RootJsonFormat, enrichAny}

import java.io.File
import javax.net.ssl.SSLContext
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object SmartADAuth {

  val grantType: String = "client_credentials"
  val localS3FilePath: String = "/Users/agupta/IdeaProjects/SmartAdIntegration/s3files/"
  val authTokenUrl: String = "https://auth.smartadserverapis.com/oauth/token"
  val dmpUrl: String = "https://dmp.smartadserverapis.com/segmentproviders"
  val imvuSegmentProvidersId: String = "183a1aed-351d-4d7e-9953-27fd9090a2dd"
  val s3SegmentDataBucket: String = "imvudata"
  val s3SegmentDataPrefix: String = "user/hive/warehouse/marketing.db/smartad_segment_data/"

  val localS3FilesDir = new java.io.File(localS3FilePath)
  // Please don't change the naming convention for the fields in this class,
  // it's required in this format by the Unmarshaller
  case class OAuthResp(access_token: String, token_type: String, expires_in: Int)

  case class Profile(profileId: String, segmentIdsToSet: List[Int])

  case class Profiles(profiles: List[Profile])

  implicit val oAuthResp: RootJsonFormat[OAuthResp] = jsonFormat3(OAuthResp)
  implicit val profile: RootJsonFormat[Profile] = jsonFormat2(Profile)
  implicit val profiles: RootJsonFormat[Profiles] = jsonFormat1(Profiles)

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "SingleRequest")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  val sslCtx: HttpsConnectionContext = ConnectionContext.httpsClient(SSLContext.getDefault)
  Http().setDefaultClientHttpsContext(sslCtx)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) sys.error("Please provide client_id and client_secret as program input")

    val clientId = args(0)
    val clientSecret = args(1)

    val oAuth2 = List(
      ("client_id", clientId),
      ("client_secret", clientSecret),
      ("grant_type", grantType)
    )

    // Downloading segment files from s3
    downloadSegmentData()

    val txtFiles: List[File] = localS3FilesDir.listFiles.filter(_.getName.endsWith(".txt")).toList

    val data = txtFiles.map(scala.io.Source.fromFile).flatMap(_.getLines().toList).take(999)
    val profiles: Profiles = Profiles(data.map { x =>
      val splitData = x.split(";")
      Profile(splitData(0), splitData(1).split(",").map(_.toInt).toList)
    })

    val segmentJson = profiles.toJson.toString

    val successCode: Future[String] = for {
      bearer <- getAuthHeader(oAuth2)
      resp <- updateSegmentForProfile(segmentJson, bearer)
    } yield resp

    successCode onComplete {
      case Success(value) => println(value)
      case Failure(ex) => println(ex.getMessage)
    }
  }

  private def getAuthHeader(auth: List[(String, String)]): Future[String] = {
    val oAuthResponseFuture: Future[HttpResponse] =
      Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = authTokenUrl,
          entity =
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, auth.map(x => s"${x._1}=${x._2}").mkString("&"))
        )
      )

    oAuthResponseFuture.flatMap(x => Unmarshal(x).to[OAuthResp].map(_.access_token))
  }

  private def updateSegmentForProfile(segmentJson: String, bearer: String): Future[String] = {

    // This step is needed as the json being passed to smartAD
    // is not a well formed json and smarts with `[` and ends on `]`
    val truncatedJson = segmentJson.drop(12).dropRight(1)

    val updateProfileFuture: Future[HttpResponse] =
      Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri =
            s"$dmpUrl/$imvuSegmentProvidersId/profiles",
          entity = HttpEntity(ContentTypes.`application/json`, truncatedJson),
          headers = Seq(headers.RawHeader("Authorization", s"Bearer $bearer"))
        )
      )

    val resp = updateProfileFuture.flatMap(_.entity.dataBytes.runReduce(_ ++ _)).map(_.utf8String)
    for {
      res <- resp
      status <- updateProfileFuture.map(_.status.intValue())
    } yield res + "\n" + status
  }

  def downloadSegmentData(): Unit = {
    try {
      localS3FilesDir.listFiles().map(_.delete())
      println("Old Files Cleared from s3 files download Dir")
    } catch {
      case ex: Exception => println(s"Couldn't clear the s3 files download dir\n${ex.getMessage}")
    }
    downloadObject(
      listS3ObjectsRecursively(
        S3Ops.listObjects(s3SegmentDataBucket, s3SegmentDataPrefix)
      )
    )
  }
}
