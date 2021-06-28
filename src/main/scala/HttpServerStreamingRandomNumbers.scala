package com.imvu.smartad

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, get, path}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn
import scala.util.Random

object HttpServerStreamingRandomNumbers {

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "RandomNumbers")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val numbers: Source[Int, NotUsed] = Source.fromIterator(() => Iterator.continually(Random.nextInt()))

    val route: Route = path("random") {
     get {
       complete(
         HttpEntity(ContentTypes.`text/plain(UTF-8)`, numbers.map(n => ByteString(s"$n\n")))
       )
     }
    }

    val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine()

    bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
  }
}
