/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.util.ParquetLogger
import org.kohsuke.args4j.{ Argument }
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import java.util.logging.Level
import scala.io._
import org.bdgenomics.adam.rdd.RegionJoin
import org.bdgenomics.adam.rich.ReferenceMappingContext._

object BeaconReads extends ADAMCommandCompanion {
  val commandName = "beaconreads"
  val commandDescription = "Creates read counts per allele"

  def apply(cmdLine: Array[String]) = {
    new BeaconReads(Args4j[BeaconReadsArgs](cmdLine))
  }
}

class BeaconReadsArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "INPUTBEACON", usage = "Input allele count file", index = 1)
  var inputPathBeacon: String = null
}

class BeaconReads(protected val args: BeaconReadsArgs) extends ADAMSparkCommand[BeaconReadsArgs] {
  val companion = BeaconReads

  def run(sc: SparkContext, job: Job) {

    val adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.inputPath)
    val mappedRecords = adamRecords.filter(_.getReadMapped)

    val seqDict = adamRecords.adamGetSequenceDictionary()

    val variantInfo: RDD[(ReferenceRegion, String, Int)] = loadPositions(sc, seqDict, args.inputPathBeacon)
    val referenceRegions: RDD[ReferenceRegion] = variantInfo.map(_._1)

    val joinedRecords: RDD[(ReferenceRegion, ADAMRecord)] = RegionJoin.partitionAndJoin(sc, seqDict, referenceRegions, mappedRecords)
    val reducedRecords = joinedRecords.groupBy(_._2).map(_._1)

    val pileUps = reducedRecords.adamRecords2Pileup()

    //val mappedPileUps1 = pileUps.map(x => (ReferenceRegion(x.getContig.getContigName, x.getPosition, x.getPosition + 1), x.getReadBase))
    val mappedPileUps1 = pileUps.map(x => x.getReadBase)
    println("!!!mp1 " + mappedPileUps1.collect().length)
    /*val groupedPileups = mappedPileUps1.groupBy(identity)
    println("!!!gP " + groupedPileups.collect().length)
    val mappedGroupPileups = groupedPileups.map(x => (x._1, x._2.size))
    val collectedGRoups = mappedGroupPileups.collect
    val mappedPileUps = collectedGRoups.toMap

    variantInfo
      .collect
      .flatMap(x => getPileUpFreq(mappedPileUps, x))
      .foreach(println)

    println("PileUp size: " + pileUps.collect().length)*/

  }

  def getPileUpFreq(mappedPileUps: Map[(ReferenceRegion, String), Int], element: (ReferenceRegion, String, Int)) = {
    element match {
      case x if mappedPileUps.contains((element._1, element._2)) =>
        Some((element._1, element._2, element._3 + mappedPileUps((element._1, element._2))))
      case _ => None
    }
  }

  /**
   * This is a convenience method, for loading positions from tab-delimited files
   * which have a format: Chromosome  Position  Allele  AlleleReadFrequency (at least for their first four columns).
   *
   * @param sc A SparkContext
   * @param seqDict The sequence dictionary containing the refIds for all the contig/chromosomes listed
   *                in the file named in 'path'.
   * @param path The filesystem location fo the VCF-like file to load
   * @return An RDD of ReferenceRegion,String pairs where each ReferenceRegion is the location of a
   *         variant and each paired String is the display name of that variant.
   *
   * @throws IllegalArgumentException if the file contains a chromosome name that is not in the
   *                                  SequenceDictionary
   */
  private def loadPositions(sc: SparkContext, seqDict: SequenceDictionary, path: String): RDD[(ReferenceRegion, String, Int)] = {
    sc.textFile(path).filter(!_.startsWith("#")).map {
      //sc.parallelize(Source.fromFile(path).getLines().filter(!_.startsWith("#")).map {
      line =>
        {
          val array = line.split("\t")
          val chrom = array(0)
          if (!seqDict.containsRefName(chrom)) {
            throw new IllegalArgumentException("chromosome name \"%s\" wasn't in the sequence dictionary (%s)".format(
              chrom, seqDict.records.map(_.name).mkString(",")))
          }
          val start = array(1).toLong
          val allele = array(2)
          val end = start + 1
          val cnt = array(3).toInt
          (ReferenceRegion(chrom, start, end), allele, cnt)
        }
    }
  }
}

