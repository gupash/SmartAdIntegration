package com.imvu.smartad

import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.server.Directives.{complete, get, path}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import spray.json.DefaultJsonProtocol.{jsonFormat1, jsonFormat2}
import spray.json.RootJsonFormat
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.io.StdIn

object HttpServerWithActorInteraction {

  object Auction {
    sealed trait Message
    case class Bid(userId: String, offer: Int) extends Message
    case class GetBids(replyTo: ActorRef[Bids]) extends Message
    case class Bids(bids: List[Bid])

    def apply: Behaviors.Receive[Message] = apply(List.empty[Bid])
    def apply(bids: List[Bid]): Behaviors.Receive[Message] = Behaviors.receive {
      case (ctx, bid @ Bid(userId, offer)) =>
        ctx.log.info(s"Bids complete: $userId, $offer")
        apply(bids :+ bid)
      case (_, GetBids(replyTo)) => replyTo ! Bids(bids)
        Behaviors.same
    }
  }

  implicit val bidFormat: RootJsonFormat[Auction.Bid] = jsonFormat2(Auction.Bid)
  implicit val bidsFormat: RootJsonFormat[Auction.Bids] = jsonFormat1(Auction.Bids)

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem[Auction.Message] = ActorSystem(Auction.apply, "auction")
    implicit val executionContext: ExecutionContext = system.executionContext

    val auction: ActorRef[Auction.Message] = system

    import Auction._

    val route: Route = path("auction") {
      concat(
        put {
          parameters("bid".as[Int], "user") {
            (bid, user) => auction ! Bid(user, bid)
              complete(StatusCodes.Accepted, "bid placed")
          }
        },
        get {
          implicit val timeout: Timeout = 5.seconds
          val bids: Future[Bids] = auction ? GetBids
          complete(bids)
        }
      )
    }

    val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)
    StdIn.readLine()
    bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
  }
}
