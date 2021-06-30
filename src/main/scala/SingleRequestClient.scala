package com.imvu.smartad

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

case class RetryConfig(
    attempts: Int,
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double
)

class SingleRequestClient(implicit system: ActorSystem, ex: ExecutionContext) {

  def request(request: HttpRequest): Future[HttpResponse] =
    Http().singleRequest(request).map {
      case res: HttpResponse if res.status.isSuccess => res
      case res: HttpResponse if res.status.isFailure =>
        throw new Exception(s"Cannot process request due to ${res.status.intValue} status")
    }
}

object SingleRequestClient {

  def apply(implicit system: ActorSystem, ex: ExecutionContext): SingleRequestClient =
    new SingleRequestClient

  def apply(config: RetryConfig)(implicit system: ActorSystem, ex: ExecutionContext): SingleRequestClient =
    new SingleRequestClient {
      override def request(request: HttpRequest): Future[HttpResponse] =
        akka.pattern.retry(
          attempt = () => super.request(request),
          attempts = config.attempts,
          minBackoff = config.minBackoff,
          maxBackoff = config.maxBackoff,
          randomFactor = config.randomFactor
        )(ex, system.scheduler)
    }
}
