package com.imvu.smartad

import SmartAdModels.{GetToken, OAuthResp, Token, UpdateBearerToken}

import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.{Http, HttpExt}
import akka.pattern.pipe
import akka.stream.Materializer
import spray.json.DefaultJsonProtocol.{jsonFormat3, _}
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContextExecutor, Future}

class SmartAdAuthActor extends Actor with ActorLogging {

  val grantType: String = "client_credentials"
  val authTokenUrl: String = "https://auth.smartadserverapis.com/oauth/token"
  implicit val oAuthResp: RootJsonFormat[OAuthResp] = jsonFormat3(OAuthResp)
  implicit val system: ActorSystem = context.system
  implicit val mat: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContextExecutor = mat.executionContext
  val http: HttpExt = Http(system)

  var token: Future[Token] = Future.failed(UninitializedFieldError("Invalid Token"))

  override def receive: Receive = {

    case UpdateBearerToken(id, secret) =>
      val oAuth2 = List(("client_id", id), ("client_secret", secret), ("grant_type", grantType))
      val oAuthResponseFuture: Future[HttpResponse] =
        http.singleRequest(
          HttpRequest(
            method = HttpMethods.POST,
            uri = authTokenUrl,
            entity =
              HttpEntity(ContentTypes.`text/plain(UTF-8)`, oAuth2.map(x => s"${x._1}=${x._2}").mkString("&"))
          )
        )

      token = oAuthResponseFuture.flatMap(x => Unmarshal(x).to[OAuthResp].map(_.access_token)).map(Token)

    case GetToken => token pipeTo sender
    case _ => throw new Exception("Invalid Request!!")
  }
}
