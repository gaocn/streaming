package org.apache.spark.streaming.ui

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.streaming.SContext
import org.apache.spark.streaming.scheduler.SListener

class SJobProgressListener(scontext: SContext) extends SListener with SparkListener{

}