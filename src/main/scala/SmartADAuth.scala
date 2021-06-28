package com.imvu.smartad

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{headers, _}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import spray.json.DefaultJsonProtocol.{jsonFormat3, _}
import spray.json.{RootJsonFormat, enrichAny}

import java.io.File
import javax.net.ssl.SSLContext
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object SmartADAuth {

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

  val oAuth2 = List(
    ("client_id", "c4a60357-b54a-40ee-8981-c578618ca4d1"),
    ("client_secret", "xI5hSZOBF/Jmujn12yNtIA=="),
    ("grant_type", "client_credentials")
  )

  def main(args: Array[String]): Unit = {

    val txtFiles: List[File] =
      new java.io.File("/Users/agupta/Desktop/input").listFiles
        .filter(_.getName.endsWith(".txt"))
        .toList

    val data = txtFiles.map(scala.io.Source.fromFile).flatMap(_.getLines().toList).take(999)
    val profiles: Profiles = Profiles(data.map { x =>
      val splitData = x.split(";")
      Profile(splitData(0), splitData(1).split(",").map(_.toInt).toList)
    })

    val segmentJson = profiles.toJson.toString

    val successCode: Future[String] = for {
      bearer <- getAuthHeader
      resp <- updateSegmentForProfile(segmentJson, bearer)
    } yield resp

    successCode onComplete {
      case Success(value) => println(value)
      case Failure(ex) => println(ex.getMessage)
    }
  }

  private def getAuthHeader: Future[String] = {
    val oAuthResponseFuture: Future[HttpResponse] =
      Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = "https://auth.smartadserverapis.com/oauth/token",
          entity =
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, oAuth2.map(x => s"${x._1}=${x._2}").mkString("&"))
        )
      )

    oAuthResponseFuture.flatMap(x => Unmarshal(x).to[OAuthResp].map(_.access_token))
  }

  private def updateSegmentForProfile(segmentJson: String, bearer: String): Future[String] = {

    val truncatedJson = segmentJson.drop(12).dropRight(1)
    //println(truncatedJson)

    val updateProfileFuture: Future[HttpResponse] =
      Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri =
            "https://dmp.smartadserverapis.com/segmentproviders/183a1aed-351d-4d7e-9953-27fd9090a2dd/profiles",
          entity = HttpEntity(ContentTypes.`application/json`, truncatedJson),
          headers = Seq(headers.RawHeader("Authorization", s"Bearer $bearer"))
        )
      )

    val resp = updateProfileFuture.flatMap(_.entity.dataBytes.runReduce(_ ++ _)).map(_.utf8String)
    for {
      res <- resp
      status <- updateProfileFuture.map(_.status.intValue())
    } yield (res + "\n" + status)
  }
}
