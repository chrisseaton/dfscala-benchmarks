/*

DFScala

Copyright (c) 2010-2012, The University of Manchester
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of The University of Manchester nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE UNIVERSITY OF MANCHESTER BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package eu.teraflux.uniman.dataflow.benchmark.kmeans

import eu.teraflux.uniman.dataflow._
import scala.collection.mutable.ListBuffer
import java.io.BufferedReader
import java.io.FileReader
import scala.util.Random
import eu.teraflux.uniman.dataflow.benchmark.Timer

object KMeans extends DFApp{
  private var noThreads:Int = 1
  private var noClusters:Int = 16
  private var filename:String = null
  var updated:Boolean = true
  
  var points:Array[Point] = null
   

  def DFMain(args:Array[String]) {
    getArgs(args)
    points = setPoints()
    DFManager.setThreadNumber(noThreads)
    println("\nThreads: " + noThreads)
    val clusters:Array[List[Double]] = setClusters(points, noClusters)
    val k = new KMeansInstance(points, clusters)
    k.computePoints()
  }

  def getArgs (args:Array[String]) = {
    var error:Boolean = false

    var i:Int = 0
    while (i < args.length) {
      if (args(i).equals("-c")) {noClusters=Integer.parseInt(args(i+1)); i+=1 }
      else if (args(i).equals("-f")) {filename=args(i+1); i+=1 }
      else if (args(i).equals("-t")) {noThreads=Integer.parseInt(args(i+1)); i+=1 }
      else error = true
      i+=1
    }

    if (filename == null)
      error = true
    
    if (error)
      displayUsage("KMeans")
  }

  def displayUsage (appName:String) {
    println("Usage: " + appName + " [options]\n")
    println("\nOptions:                              ( defaults )")
    println("    -t <UINT>     Number of [t]hreads   ( " + noThreads + " )")
    println("    -c <UINT>     Number of [c]usters   ( " + noClusters + " )")
    println("    -f <String>   [f]ile name of input  ( " + filename + " )")
    System.exit(1)
  }

  def setClusters(points:Array[Point], noClusters:Int):Array[List[Double]] = {
    val clusters = new Array[List[Double]](noClusters)
    val random = new Random
    for(i <- 0 to noClusters-1)
      clusters(i) = points(i).location
      //clusters(i) = points(random.nextInt(points.length)).location
    clusters
  }

  def setPoints():Array[Point] = {
    val in = new BufferedReader(new FileReader(filename))
    val l = new ListBuffer[Point]()

    while (in.ready())
      l + new Point(in.readLine().split(" ").toList.tail.map(s=>java.lang.Double.parseDouble(s)))

    in.close()

    val points:Array[Point] = new  Array[Point](l.length)
    var i = 0;
    for(p <- l.toList) {
      points(i) = p
      i += 1
    }
    points
  }

  class Point (l:List[Double]) {
    val location:List[Double] = l
    var cluster:Int = -1

    def calculateCluster(clusters:Array[List[Double]]):Int = {
      val c = findCluster(clusters)
      if(c != cluster) {
        cluster = c
        updated = true
      }
      c
    }
    
    def findCluster(clusters:Array[List[Double]]):Int = {
      var cluster:Int = 0
      var dist:Double = distance(clusters(0))
      val noClusters = clusters.length
      for (i<- 1 until noClusters) {
        val d2:Double = distance(clusters(i))
        if(d2 < dist) {
          dist = d2
          cluster = i
        }
      }
      cluster
    }

    def distance(cluster:List[Double]):Double = {
       var sum:Double = 0;
       for((a,b) <- location zip cluster)
         sum += (a-b)*(a-b)
       sum
    }
  }

  private class KMeansInstance(points:Array[Point], clusters:Array[List[Double]]) {
    var iterations:Int = 0
    val noDimensions:Int = clusters(0).length
    val noPoints:Int = points.length
    val noClusters:Int = clusters.length
    val pointsPerThread:Int = 1 + (noPoints-1)/noThreads
    
    def computePoints() {
      Timer.start()
      //println("StartCP")
      iterations += 1;
      var start = 0
      var end = pointsPerThread
    //  var n = 0
      
      val collector = DFManager.createCollectorThread[(Array[List[Double]], Array[Int])](noThreads)
      val reduction = DFManager.createThread(computeClusters _)
      collector.addListener(reduction.token1)

      while (end < noPoints) {
        val thread = DFManager.createThread(computePointsSection _)
        thread.arg1  = start
        thread.arg2 = end
       // DFManager.passToken(thread, n, 3)
        thread.arg3 = collector.token1

        start = end
        end += pointsPerThread
        //n += 1
      }

      val thread = DFManager.createThread(computePointsSection _)
      thread.arg1 = start
      thread.arg2 = noPoints
      //DFManager.passToken(thread, n, 3)
      thread.arg3 = collector.token1

      //println("endCP")
    }

    def computeClusters(input:List[(Array[List[Double]], Array[Int])])
    {
     //println("Start Compute Clusters: " + updated) 
     if(updated) {
        updated = false
        generateNewClusters(input)
        println(Timer.stop())
        computePoints()
      }
      else 
      {
        println(Timer.stop())
      }
    }

    def computePointsSection(start:Int, end:Int, collector:Token[(Array[List[Double]], Array[Int])]) {
      //println("Start Compute points " + start + ", " + end)
      val PC = new Array[List[Double]](noClusters)
      val PCC = new Array[Int](noClusters)

      val l = new ListBuffer[Double]()
      for(i<- 1 to noDimensions)
        l + 0
      val zero:List[Double] = l.toList
      for(j<-0 until noClusters)
        PC(j) = zero

      for (i<- start until end) {
        val cluster:Int = points(i).calculateCluster(clusters)
        PC(cluster) = List.map2(PC(cluster),points(i).location) ((x,y) => x + y)
        PCC(cluster) += 1
      }

      collector((PC, PCC))
      //println("End Compute points " + start + ", " + end)
    }

    def generateNewClusters(partialClusters:List[(Array[List[Double]], Array[Int])]) {
      val completeClusters:(Array[List[Double]], Array[Int]) = partialClusters reduceLeft sumClusters
      for(i <- 0 to noClusters-1)
        if(completeClusters._2(i)!=0)
          clusters(i) =  completeClusters._1(i) map (y => y/completeClusters._2(i))
    }    

    def sumClusters(clusters1:(Array[List[Double]],Array[Int]),clusters2:(Array[List[Double]],Array[Int])):(Array[List[Double]],Array[Int]) = {
      (clusters1._1 zip clusters2._1 map (x => List.map2(x._1, x._2)((y,z) => y + z)), clusters1._2 zip clusters2._2 map (x => x._1 + x._2))
    }

    def printCluster()
    {
      println("Clusters after " + iterations + " iterations")
      for(c<-clusters) {
        print("(" + (c.head))
        for(v<- c.tail)
          print(", " + v)
        println(")")
      }
    }
  }
}



