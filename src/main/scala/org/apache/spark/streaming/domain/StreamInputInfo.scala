package org.apache.spark.streaming.domain

/**
	* 用于记录每次batch时的输入流信息
	* @param inputStreamId 输入流的idi
	* @param numRecords 当前batch中的记录数
	* @param metadata 用于在UI中显示，其中至少要包含字段'description'
	*/
case class StreamInputInfo(
		inputStreamId: Int,
		numRecords: Long,
		metadata: Map[String, Any] = Map.empty
		){

	def metadataDescription: Option[String] = {
		metadata.get(StreamInputInfo.METADATA_KEY_DESCRIPTION)
  		.map(_.toString)
	}
}

object StreamInputInfo {
	val METADATA_KEY_DESCRIPTION = "Description"
}