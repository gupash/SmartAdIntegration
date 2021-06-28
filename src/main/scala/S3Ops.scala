package com.imvu.smartad

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.s3.model.{
  GetObjectRequest,
  ListObjectsV2Request,
  ListObjectsV2Result,
  S3ObjectSummary
}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId}
import java.util.Date
import scala.collection.JavaConverters.asScalaBufferConverter

object S3Ops {

  val S3_DEL_OBJ_LIMIT = 999
  val retryPolicy = new ClientConfiguration()
  retryPolicy.setMaxErrorRetry(10)

  val s3: AmazonS3 = AmazonS3ClientBuilder.standard.withClientConfiguration(retryPolicy).build

  def listObjects(s3bucket: String, prefix: String): ListObjectsV2Result =
    s3.listObjectsV2(s3bucket, prefix)

  def downloadObject(items: List[S3ObjectSummary]): Unit =
    items.foreach(
      x =>
        s3.getObject(
          new GetObjectRequest(x.getBucketName, x.getKey),
          new File(s"s3files/${x.getKey.split("/").last}")
        )
    )

  def listS3ObjectsRecursively(obj: ListObjectsV2Result): List[S3ObjectSummary] =
    if (obj.isTruncated) {
      val objects = obj.getObjectSummaries.asScala.toList
      objects ++ listS3ObjectsRecursively(
        s3.listObjectsV2(
          new ListObjectsV2Request()
            .withBucketName(obj.getBucketName)
            .withPrefix(obj.getPrefix)
            .withContinuationToken(obj.getNextContinuationToken)
        )
      )
    } else obj.getObjectSummaries.asScala.toList
}
