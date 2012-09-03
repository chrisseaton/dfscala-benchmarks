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

package eu.teraflux.uniman.dataflow

trait DFLogger {
  def threadCreated(child:DFThread, parent:DFThread)
  def threadCreated(child:DFThread, manager:DFManager)
  def barrierCreated(bar:DFBarrier, sC:Int)
  def tokenPassed(from:DFThread, to:DFThread, argNo:Int)
  def tokenPassed(from:DFManager, to:DFThread, argNo:Int)
  def nullTokenPassed(from:DFThread)
  def threadStarted(thread:DFThread)
  def threadFinished(thread:DFThread)
  def threadToBarrier(thread:DFThread, barrier:DFBarrier)
  def threadFromBarrier(thread:DFThread, barrier:DFBarrier)
  def barrierActivated(barrier:DFBarrier)
  def barrierSatisfied(barrier:DFBarrier)
  def createManager(manager:DFManager, parent:DFThread)
  def managerStart(manager:DFManager)
  def managerFinish(manager:DFManager)
  def getThreadID:Int
  def getManagerID:Int
  def getBarrierID:Int
}

object DFLogger extends DFLogger {
  private val logger:DFLogger = {
			val className:String = System.getProperty( "eu.teraflux.uniman.dataflow.logger");
			if( className != null){
				try {
					Class.forName(className).newInstance() match {
					  case logger : DFLogger => logger
					}
				} catch {
				  case e:ClassNotFoundException => {
				    e.printStackTrace();
				    new NullLogger
			    }
			  }
			}
			else
			  new NullLogger
		}
			
  def threadCreated(child:DFThread, parent:DFThread)   = logger.threadCreated(child, parent)
  def threadCreated(child:DFThread, manager:DFManager) = logger.threadCreated(child, manager)
  def barrierCreated(bar:DFBarrier, sC:Int)            = logger.barrierCreated(bar,sC)
  def tokenPassed(from:DFThread, to:DFThread, argNo:Int)           = logger.tokenPassed(from, to, argNo)
  def tokenPassed(from:DFManager, to:DFThread, argNo:Int)          = logger.tokenPassed(from, to, argNo)
  def nullTokenPassed(from:DFThread)                               = logger.nullTokenPassed(from)
  def threadStarted(thread:DFThread)                               = logger.threadStarted(thread)
  def threadFinished(thread:DFThread)                              = logger.threadFinished(thread)
  def threadToBarrier(thread:DFThread, barrier:DFBarrier)          = logger.threadToBarrier(thread, barrier)
  def threadFromBarrier(thread:DFThread, barrier:DFBarrier)        = logger.threadFromBarrier(thread, barrier)
  def barrierActivated(barrier:DFBarrier)                          = logger.barrierActivated(barrier)
  def barrierSatisfied(barrier:DFBarrier)                          = logger.barrierSatisfied(barrier)
  def createManager(manager:DFManager, parent:DFThread)            = logger.createManager(manager, parent)
  def managerStart(manager:DFManager)                              = logger.managerStart(manager)
  def managerFinish(manager:DFManager)                             = logger.managerFinish(manager)
  def getThreadID:Int                                              = logger.getThreadID
  def getManagerID:Int                                             = logger.getManagerID
  def getBarrierID:Int                                             = logger.getBarrierID
}

class NullLogger extends DFLogger {
  def threadCreated(child:DFThread, parent:DFThread) {}
  def threadCreated(child:DFThread, manager:DFManager) {}
  def barrierCreated(bar:DFBarrier, sC:Int) {}
  def tokenPassed(from:DFThread, to:DFThread, argNo:Int) {}
  def tokenPassed(from:DFManager, to:DFThread, argNo:Int) {}
  def nullTokenPassed(from:DFThread) {}
  def threadStarted(thread:DFThread) {}
  def threadFinished(thread:DFThread) {}
  def threadToBarrier(thread:DFThread, barrier:DFBarrier) {}
  def threadFromBarrier(thread:DFThread, barrier:DFBarrier) {}
  def barrierActivated(barrier:DFBarrier) {}
  def barrierSatisfied(barrier:DFBarrier) {}
  def createManager(manager:DFManager, parent:DFThread) {}
  def disposeManager(manager:DFManager) {}
  def managerStart(manager:DFManager) {}
  def managerFinish(manager:DFManager) {}
  def getThreadID():Int = 0
  def getManagerID():Int = 0
  def getBarrierID():Int = 0
}

class FunctionalDFLogger extends DFLogger {
  import scala.collection.mutable.HashMap
  import scala.xml
  import eu.teraflux.uniman.transactions.TMLib._
  
  protected class Thread(manager:Int, parent:Int, noArgs:Int, startthread:Boolean, callStack:Array[StackTraceElement]){
    var start:Option[Long]=None
    var end:Option[Long]=None
    var argsIn:List[ArgIn] = List()
    var argsOut:List[ArgOut] = List()
    var barsOut:List[Int] = List()
    var barsIn:List[Int] = List()
    def starttime(id:Int) = start match {
		      case Some(time) => time
		      case None => throw new Exception("Internal error in the DFLogger, start time not initialised for thread " + id )
		    }
    def endtime (id:Int)=   end match {
		      case Some(time) => time
		      case None => throw new Exception("Internal error in the DFLogger, end time not initialised for thread " + id)
		    }

    def toXML(id: Int) = 
      <DFThread><id>{id}</id>
        <starttime>{starttime(id)}</starttime>
        <endtime>{endtime(id)}</endtime>
	<startthread>{startthread}</startthread>
	<inputs>{noArgs + barsIn.length}</inputs>
        <outputs>{for {a <- argsOut} yield <dest>{a.dest}</dest> }
		  {for {b <- barsOut} yield <dest>{b}</dest>}</outputs>
      </DFThread> 

 }
  
  protected class ArgIn(time:Long, callStack:Array[StackTraceElement], from:Int, pos:Int)
  protected class ArgOut(to:Int, pos:Int) {
    def dest = to
  }
  
  protected class Barrier (sC:Int, callStack:Array[StackTraceElement]){
    var toThread:List[(Int, Array[StackTraceElement])] = List()
    var fromThread:List[(Int, Array[StackTraceElement])] = List()
    var active:Boolean = false
    var satisfied:Boolean = false
    var signalTime : Option[Long]=None
    def sigtime = signalTime match {
		      case Some(time) => time
		      case None => throw new Exception("Internal error in the DFLogger, signal time not initialised for barrier")
		    }

    def addToThread(thread:Int, callStack:Array[StackTraceElement]) { toThread = (thread, callStack)::toThread }
    def addFromThread(thread:Int, callStack:Array[StackTraceElement]) { fromThread = (thread, callStack)::fromThread }
    def toXML(id:Int) = <DFBarrier>
			    <id>{id}</id>
			    <sC>{sC}</sC>
			    <signaltime>{sigtime}</signaltime>
			    <inputs>{toThread.length}</inputs>
			    <outputs>{for{b <- fromThread} yield <dest>{b._1}</dest>}</outputs>
			</DFBarrier>
  }
  
  protected class Manager(val parent:Int) {
    //Start and end times for the execution of a manager. 
    //Normally managers will only execute once, but in 
    //DFCollections etc they may be reused
    var start:List[Long] = List()
    var end:List[Long] = List()
  }
  
  private val threadLookup = new HashMap[Int, Thread]
  private var threadID = 0
  private var iter = 1
  //Create a null thread to store arguments from null tokens
  threadLookup(0) = new Thread(0, 0, 0, true, new Array[StackTraceElement](0))
  
  private val barrierLookup = new HashMap[Int, Barrier]
  /*Removed when barriers where given the same ID as threads, possibly restore*/
  //private var barrierID = 0
  
  private val managerLookup = new HashMap[Int, Manager]
  private var managerID = 0
  
  def clear {
    threadID=0
    managerID=0
    threadLookup.clear
    barrierLookup.clear
 }

  def threadCreated(child:DFThread, parent:DFThread)
  {
    threadLookup(child.logID) = new Thread(parent.manager.logID, 
                                           parent.logID, 
                                           child.no_args,
					   child.startthread,
                                           getCallStack)
  }
  
  def threadCreated(child:DFThread, manager:DFManager)
  {
    threadLookup(child.logID) = new Thread(manager.logID, 
                                           getManager(manager.logID).parent, 
                                           child.no_args, 
					   child.startthread,
                                           getCallStack)
  }
  
  def barrierCreated(bar:DFBarrier, sC:Int)
  {
    bar match  {
      case vb: DFVoidBarrier => {} // not doing anything for void barrier 
      case tb : DFBarrier => barrierLookup(tb.logID) = new Barrier(sC, 
                                           getCallStack)
    }
  }
  
  def tokenPassed(from:DFThread, to:DFThread, argNo:Int)
  {
    val threadto:Thread = getThread(to.logID)
    val threadfrom:Thread = getThread(from.logID)
    val argin = new ArgIn(System.nanoTime, getCallStack, from.logID, argNo)
    val argout = new ArgOut(to.logID, argNo)
    threadfrom.argsOut = argout :: threadfrom.argsOut
    threadto.argsIn = argin :: threadto.argsIn
  }
  
  
  def nullTokenPassed(from:DFThread)
  {
    val threadto:Thread = getThread(0)
    val threadfrom:Thread = getThread(from.logID)
    val argin = new ArgIn(System.nanoTime, getCallStack, from.logID, 0)
    val argout = new ArgOut(0, 0)
    threadfrom.argsOut = argout :: threadfrom.argsOut
    threadto.argsIn = argin :: threadto.argsIn
  }
  
  def tokenPassed(from:DFManager, to:DFThread, argNo:Int)
  {
    val thread:Thread = getThread(to.logID)
    val arg = new ArgIn(System.nanoTime, getCallStack, getManager(from.logID).parent, argNo)
    thread.argsIn = arg :: thread.argsIn
  }
  
  def threadStarted(thread:DFThread)
  {
    getThread(thread.logID).start = Some(System.nanoTime)
  }
  
  def threadFinished(thread:DFThread)
  {
    getThread(thread.logID).end = Some(System.nanoTime)
  }
  
  def threadToBarrier(thread:DFThread, barrier:DFBarrier)
  {
    getBarrier(barrier.logID).addToThread(thread.logID, getCallStack)
  }
  
  def threadFromBarrier(thread:DFThread, barrier:DFBarrier)
  {
    getBarrier(barrier.logID).addFromThread(thread.logID, getCallStack)
  }
  
  def barrierActivated(barrier:DFBarrier)
  {
    getBarrier(barrier.logID).active = true
  }
  
  def barrierSatisfied(barrier:DFBarrier)
  {
    val bar = getBarrier(barrier.logID)
    bar.satisfied = true
    bar.signalTime = Some(System.nanoTime)
    /*
     * Both of these are redundant if the DFLibrary is functioning correctly 
     * and they are only included for convenience.
     */
    //Added to record the barriers that have completed to allow this thread to start
    for {t <- bar.fromThread} {
      var th:Thread = getThread(t._1)
      th.barsIn = barrier.logID :: th.barsIn
    }
    
    //Added to record the barriers that have been signalled by this thread.
    for {t <- bar.toThread} {
      var th:Thread = getThread(t._1)
      th.barsOut = barrier.logID :: th.barsOut
    }
  }
  
  def createManager(manager:DFManager, parent:DFThread) 
  {
    if(parent != null)
      managerLookup(manager.logID) = new Manager(parent.logID)
    else
      managerLookup(manager.logID) = new Manager(0)
  }
  
  def managerStart(manager:DFManager) { 
    val managerRecold = getManager(manager.logID)
    managerRecold.start = System.nanoTime :: managerRecold.start
  }
  
  def managerFinish(manager:DFManager) { 
    val managerRecold = getManager(manager.logID)
    managerRecold.end = System.nanoTime :: managerRecold.end
    cleanup
  }

  def cleanup =  {
    println("Logger cleanup")
    scala.xml.XML.save("dfgraph." + iter + ".xml", toXML)
    clear
    iter += 1
  }
  
  private def getThread(threadId:Int):Thread =
  {
    threadLookup.get(threadId) match {
      case Some(thread) => thread
      case None => throw new Exception("Internal error in the DFLogger, thread instance is missing for thread id " + threadId)
    }
  }
  
  def getThreadID:Int = {
    atomic{
          threadID += 1
          threadID
       }
  }
  
  private def getBarrier(barrierId:Int):Barrier =
  {
    barrierLookup.get(barrierId) match {
      case Some(barrier) => barrier
      case None => {
	throw new Exception("Trying to access barrier that doesn't exist")
//        val barrier = new Barrier
//        barrierLookup(barrierId) = barrier
//        barrier
      }
    }
  }
  /*
   * Removed by Salman when Barries where given the same id's as threads, possible this should be 
   * restored.
   */
  /*
  def getBarrierID:Int = {
    atomic{
      barrierID += 1
      barrierID
    }
  }
  */
  
    def getBarrierID:Int = getThreadID
  
  private def getManager(managerId:Int):Manager =
  {
    managerLookup.get(managerId) match {
      case Some(manager) => manager
      case None => throw new Exception("Internal error in the DFLogger, manager instance is missing for id " + managerId)
    }
  }
  
  def getManagerID:Int = {
    atomic{ 
      managerID += 1
      managerID
    }
  }
  
  private def getCallStack():Array[StackTraceElement] = {
    val trace = new Exception().getStackTrace
    val toReturn = new Array[StackTraceElement](trace.length - 2)
    trace.copyToArray(toReturn,0,trace.length - 2)
    toReturn
  }
  
  private def nameFunction(function:_ => _):String = function.getClass.toString
  
  private def toXML = <DFGraph>{for {tr <- threadLookup} yield tr._2.toXML(tr._1)}
		       {for {br <- barrierLookup} yield br._2.toXML(br._1)}</DFGraph>
  
  override def finalize:Unit = {
    scala.xml.XML.save("dfgraph.xml", toXML)
    super.finalize
  }  
}

/*
  To use DataflowGraphLogger, set the property:
  
    -Deu.teraflux.uniman.dataflow.logger=eu.teraflux.uniman.dataflow.DataflowGraphLogger
  
  By default you will get a dataflow graph. You can also get a 'swimming lane'
  style graph by setting the property:
  
    -Deu.teraflux.uniman.dataflow.DataflowGraphLogger.swimminglane=true
  
  This class could easily be made an offline tool later on using the XML
  output.
*/

class DataflowGraphLogger extends DFLogger {
  import scala.collection.mutable.Map
  import scala.collection.mutable.Set
  
  import eu.teraflux.uniman.transactions.TMLib.atomic
  
  val threadLogMap = Map[DFThread, ThreadLog]()
  
  class ThreadLog(val dfthread : DFThread, val parent : ThreadLog) {
    val dependents = Set[ThreadLog]()
    
    def addDependent(dependent : ThreadLog) {
      dependents += dependent
    }
    
    var runningThread : String = "unknown"
    var startTime : Long = -1
    var endTime : Long = -1
  }
  
  val managerThread = new ThreadLog(null, null)
  val nullThread = new ThreadLog(null, null)
  
  def threadCreated(child:DFThread, parent:DFThread) {
    atomic {
      threadLogMap += (child -> new ThreadLog(child, threadLogMap(parent)))
    }
  }
  
  def threadCreated(child:DFThread, manager:DFManager) {
    atomic {
      threadLogMap += (child -> new ThreadLog(child, null))
    }
  }
  
  def tokenPassed(from:DFThread, to:DFThread, argNo:Int) {
    atomic {
      threadLogMap(from).addDependent(threadLogMap(to))
    }
  }
  
  def tokenPassed(from:DFManager, to:DFThread, argNo:Int) {
    atomic {
      managerThread.addDependent(threadLogMap(to))
    }
  }
  
  def nullTokenPassed(from:DFThread) {
    atomic {
      threadLogMap(from).addDependent(nullThread)
    }
  }
  
  def threadStarted(thread:DFThread) {
    val threadLog = atomic{threadLogMap(thread)}
    threadLog.runningThread = Thread.currentThread.getName
    threadLog.startTime = System.nanoTime
  }
  
  def threadFinished(thread:DFThread) {
    atomic {
      threadLogMap(thread).endTime = System.nanoTime
    }
  }
  
  def threadToBarrier(thread:DFThread, barrier:DFBarrier) {}
  def threadFromBarrier(thread:DFThread, barrier:DFBarrier) {}
  def barrierCreated(bar:DFBarrier, sC:Int) {}
  def barrierActivated(barrier:DFBarrier) {}
  def barrierSatisfied(barrier:DFBarrier) {}
  def createManager(manager:DFManager, parent:DFThread) {}
  def disposeManager(manager:DFManager) {}
  def managerStart(manager:DFManager) {}
  
  def managerFinish(manager:DFManager) {
    drawGraph(System.err)
  }
  
  def getThreadID():Int = 0
  def getManagerID():Int = 0
  def getBarrierID():Int = 0
  
  def drawGraph(out : java.io.PrintStream) {
    val swimmingLanes = System.getProperty("eu.teraflux.uniman.dataflow.DataflowGraphLogger.swimminglane") == "true"
    
    out.println("/*")
    out.println("  This is the output of DataflowGraphLogger.")
    out.println("  ")
    out.println("  Install GraphViz, write this to a file dag.dot and run:")
    out.println("  ")
    
    if (swimmingLanes)
      out.println("    neato -Tpdf -odag.pdf dag.dot")
    else
      out.println("    dot -Tpdf -odag.pdf dag.dot")
    
    out.println("*/")
    out.println()
    out.println("digraph Dataflow {")
    
    var threadLogs = List[ThreadLog]()
    threadLogs ::= managerThread
    threadLogs ::= nullThread
    threadLogs ++= threadLogMap.values
    
    val runningThreads = threadLogs.map(_.runningThread).distinct
    
    val startTime = threadLogMap.values.map(_.startTime).min
    val endTime = threadLogMap.values.map(_.endTime).max
    val duration = endTime - startTime
    
    managerThread.startTime = startTime
    managerThread.endTime = startTime
    
    nullThread.startTime = endTime
    nullThread.endTime = endTime
    
    for (threadLog <- threadLogs) {
      val shape = if (threadLog == managerThread)
          "doublecircle"
        else if (threadLog == nullThread)
          "invtriangle"
        else if (isPrimitive(threadLog.dfthread))
          if (swimmingLanes)
            "box"
          else
            "ellipse"
        else
          if (swimmingLanes)
            "box"
          else
            "diamond"
      
      val height = ((threadLog.endTime - threadLog.startTime) / duration.toDouble) * 20.0
      
      val x = runningThreads.indexOf(threadLog.runningThread) * 3.0
      val y = 10.0 - ((threadLog.startTime - startTime) / duration.toDouble) * 20.0 - (height / 2.0)
      
      if (swimmingLanes)
        out.println("  " + threadLog.hashCode.toString + " [pos=\"" + "%f".format(x) + "," + "%f".format(y) + "!\",height=" + "%f".format(height) + ",width=2,label=" + quote(name(threadLog)) + ",shape=" + shape + "];")
      else
        out.println("  " + threadLog.hashCode.toString + " [label=" + quote(name(threadLog)) + ",shape=" + shape + "];")
      
      if (threadLog.parent != null)
        out.println("  " + threadLog.parent.hashCode.toString + " -> " + threadLog.hashCode.toString + " [style=dotted,penwidth=0.1,weight=0,arrowsize=0.75,arrowhead=open];")
      
      for (dependent <- threadLog.dependents)
        out.println("  " + threadLog.hashCode.toString + " -> " + dependent.hashCode.toString + " [penwidth=2];")
    }
    
    out.println("}")
  }
  
  def isPrimitive(thread : DFThread) : Boolean =
    /* Note that I must do a textual comparison here, as DFReducerThread is
    parameterised and so I can't do classOf[DFReducerThread]. */
    
    thread.getClass.getName match {
      case "eu.teraflux.uniman.dataflow.DFReducerThread" => true
      case _ => false
    }
  
  def name(threadLog : ThreadLog) : String =
    if (threadLog == managerThread)
      "DFManager"
    else if (threadLog == nullThread)
      "null"
    else
      threadLog.dfthread.name

  def quote(s : String) : String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
