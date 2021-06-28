package com.imvu.smartad

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives.{as, complete, concat, entity, get, onSuccess, path, pathPrefix, post}
import akka.http.scaladsl.server.PathMatchers.LongNumber
import spray.json.RootJsonFormat
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.PathMatcher._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.StdIn

object SprayJsonExample {

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "SprayExample")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  var orders: List[Item] = Nil

  final case class Item(name: String, id: Long)
  final case class Order(items: List[Item])

  implicit val itemFormat: RootJsonFormat[Item] = jsonFormat2(Item)
  implicit val orderFormat: RootJsonFormat[Order] = jsonFormat1(Order)

  def fetchItem(id: Long): Future[Option[Item]] = Future {
    orders.find(_.id == id)
  }

  def saveOrder(order: Order): Future[Done] = {
    orders = order match {
      case Order(item) => item ::: orders
      case _ => orders
    }

    Future(Done)
  }

  def main(args: Array[String]): Unit = {

    val route: Route = concat(
      get {
        pathPrefix("item" / LongNumber) { id =>
          val mayBeItem: Future[Option[Item]] = fetchItem(id)
          onSuccess(mayBeItem) {
            case Some(item) => complete(item)
            case None => complete(StatusCodes.NotFound)
          }
        }
      },
      post {
        path("create-order") {
          entity(as[Order]) { order =>
            val saved: Future[Done] = saveOrder(order)
            onSuccess(saved)(_ => complete("order created"))
          }
        }
      }
    )

    val bindingFuture: Future[Http.ServerBinding] = Http().newServerAt("localhost", 8080).bind(route)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return

    bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
  }
}

