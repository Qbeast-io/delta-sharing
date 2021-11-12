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

/**
 * Sampler filters the data files according to the precision range.
 */
object Sampler {
  /**
   * Samples given data according to the specified precision range.
   *
   * @param files the data files
   * @param precisionFrom  the minimum precision
   * @param precisionTo  the mxiimum precision
   * @return the sample files
   */
  def sample(files: Seq[AddFile], precisionFrom: Double, precisionTo: Double): Seq[AddFile] = {
    if (precisionFrom > precisionTo) {
      return Seq.empty
    }
    val minSampleWeight = getSampleWeight(precisionFrom)
    val maxSampleWeight = getSampleWeight(precisionTo)
    files.filter { file =>
      val minFileWeight = getFileWeight(file, "minWeight")
      val maxFileWeight = getFileWeight(file, "maxWeight")
      (minSampleWeight <= minFileWeight && minFileWeight <= maxSampleWeight) ||
        (minFileWeight <= minSampleWeight && minSampleWeight <= maxFileWeight)
    }
  }

  private def getSampleWeight(precision: Double): Int = {
    if (precision < 0) {
      return Int.MinValue
    }
    if (precision > 1) {
      return Int.MaxValue
    }
    Int.MinValue + (precision * (Int.MaxValue.toDouble - Int.MinValue.toDouble)).toInt
  }

  private def getFileWeight(file: AddFile, tag: String): Int = {
    file.tags.get(tag).map(_.toInt).getOrElse(0)
  }
}
