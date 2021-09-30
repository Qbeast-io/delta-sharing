/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal

import io.delta.standalone.internal.actions.AddFile

import io.delta.sharing.server.protocol.SampleHint

/**
 * Sampler filters the data files according the sample hint.
 */
object Sampler {
  /**
   * Samples given data according to the specified sample hint.
   *
   * @param files the data files
   * @param hint  the sample hint
   * @return the sample files
   */
  def sample(files: Seq[AddFile], hint: SampleHint): Seq[AddFile] = {
    val minSampleWeight = getSampleWeight(hint.precisionFrom)
    val maxSampleWeight = getSampleWeight(hint.precisionTo)
    files.filter { file =>
      val minFileWeight = getFileWeight(file, "minWeight")
      val maxFileWeight = getFileWeight(file, "maxWeight")
      (minSampleWeight <= minFileWeight && minFileWeight <= maxSampleWeight) ||
        (minFileWeight <= minSampleWeight && minSampleWeight <= maxFileWeight)
    }
  }

  private def getSampleWeight(precision: Option[Double]): Int = {
    if (precision.isEmpty) {
      return Int.MinValue
    }
    val value = precision.get
    if (value < 0) {
      return Int.MinValue
    }
    if (value > 1) {
      return Int.MaxValue
    }
    Int.MinValue + (value * (Int.MaxValue.toDouble - Int.MinValue.toDouble)).toInt
  }

  private def getFileWeight(file: AddFile, tag: String): Int = {
    file.tags.get(tag).map(_.toInt).getOrElse(0)
  }
}
