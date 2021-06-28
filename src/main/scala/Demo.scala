package com.imvu.smartad

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import java.util.Calendar
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object Demo {

  case class Info(msg: String)
  case class Res(msg: String)

  class MyActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case Info(value) =>
        this.supervisorStrategy
        Thread.sleep(4000)
        println(s"Processed $value")
        sender ! Res("I am Back")
      case _ =>
        println("Unknown value")
        sender ! Res("Please check again")
    }
  }

  class Message extends Thread {
    override def run(): Unit = {
      System.out.println(Calendar.getInstance().getTime)
    }
  }

  def main(args: Array[String]): Unit = {

    implicit val timeout: Timeout = Timeout(2.seconds)
    val system = ActorSystem("MyActorSystem")
    val myActor = system.actorOf(Props[MyActor], "MyActor")

    val output: Future[Res] = (myActor ? Info("Vanilla")).mapTo[Res]
    output.foreach { x =>
      println(x.msg)
    }
    system.terminate()
  }
}
