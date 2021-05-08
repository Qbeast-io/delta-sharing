package io.delta.sharing.spark.model

import com.fasterxml.jackson.annotation.JsonInclude
import io.delta.sharing.spark.util.JsonUtils
import org.codehaus.jackson.annotate.JsonRawValue

case class DeltaTableMetadata(version: Long, protocol: Protocol, metadata: Metadata)

case class DeltaTableFiles(version: Long, protocol: Protocol, metadata: Metadata, files: Seq[AddFile])

case class Share(name: String)

case class Schema(name: String, share: String)

case class Table(name: String, schema: String, share: String)

case class SingleAction(
    file: AddFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null) {

  def unwrap: Action = {
    if (file != null) {
      file
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else {
      null
    }
  }
}

case class Format(provider: String = "parquet")

case class Metadata(
    id: String = null,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil) extends Action {
  override def wrap: SingleAction = SingleAction(metaData = this)
}

sealed trait Action {
  def wrap: SingleAction
  def json: String = JsonUtils.toJson(wrap)
}

case class Protocol(minReaderVersion: Int) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
}

case class AddFile(
  url: String,
  id: String,
  @JsonInclude(JsonInclude.Include.ALWAYS)
  partitionValues: Map[String, String],
  size: Long,
  @JsonRawValue
  stats: String = null) extends Action {

  require(url.nonEmpty)

  override def wrap: SingleAction = SingleAction(file = this)
}