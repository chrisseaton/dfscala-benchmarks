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
import scala.collection.mutable.ArrayBuffer
import scala.reflect.Manifest
import scala.concurrent.Lock
import java.util.concurrent._

// Dataflow Library Programming Interface
// 1. Implement DFApp.DFMain. This is the first thread to be run.
//
// 2. DFManager.createThread (DFFunc : T => Unit) : DFThread
//		DFFunc is executed as the dataflow thread. The arguments for DFFunc are treated 
//		as the input tokens to the thread. There can be any number of arguments (Currently 
//		implemented only up to four). DFFunc must not return a value (return type Unit)


class DFManager {
  val logID:Int = DFLogger.getManagerID
  private var DFWaiting: ArrayBuffer[DFThread] = new ArrayBuffer[DFThread]
  private var DFRunning: ArrayBuffer[DFThread] = new ArrayBuffer[DFThread]
  private val lock: AnyRef = new Object()
  private var e: Option[Throwable] = None
  private var timeout: Long = 0
  
  DFLogger.createManager(this, DFManager.currentThread.get)

  def registerDFThread(DFT: DFThread): Unit = {
    lock.synchronized {
      DFWaiting += DFT
    }
    DFT.activate
  }

  def wake() {
    lock.synchronized {
      lock.notify()
    }
  }

  private def timeOut() {
    if (DFRunning.size != 0) {
      sendException(new TimeoutException("Dataflow graph exceeded " + timeout + " milliseconds to run"))
    }
  }

  def setTimeOut(time: Long) {
    timeout = time;
  }

  def sendException(exception: Throwable) {
    lock.synchronized {
      e = Some(exception)
      lock.notify()
    }
  }

  def runningThreadCount() = DFRunning.count(thread => thread.isRunning)
  def readyThreadCount() = DFWaiting.count(thread => thread.isReady)
  def waitingThreadCount() = DFWaiting.count(thread => thread.isWaiting)
  def finishedThreadCount() = DFRunning.count(thread => thread.isFinished)

  def run() {
    DFManager.addManager()
    DFLogger.managerStart(this)
    lock.synchronized {
      if (timeout != 0) {
        val timer = new timer(timeout, this)
        timer.start
      }
      
        //Start all ready threads
        var DFReady = DFWaiting.filter(t => t.isReady)
        DFWaiting --= DFReady
        for (cDF:DFThread <- DFReady) {
          //Adjust the state here to prevent multiple calls of cDF.start
          cDF.state = DFState.Running
          DFManager.pool.execute(cDF)
        }
        DFRunning ++= DFReady
      
      while (DFRunning.size != 0) {
        //Nothing to do, wait till we are notified of a change
        lock.wait()
        e match {
          case Some(exception) => {
            DFManager.removeManager()
            throw exception
          }
          case None => {           
            //Check which threads are finished
            DFRunning = DFRunning.filter(t => !t.isFinished).asInstanceOf[ArrayBuffer[DFThread]]
        
            //Start all ready threads
            DFReady = DFWaiting.filter(t => t.isReady)
            DFWaiting --= DFReady
            for (cDF:DFThread <- DFReady) {
              //Adjust the state here to prevent multiple calls of cDF.start
              cDF.state = DFState.Running
              DFManager.pool.execute(cDF)
            }
            DFRunning ++= DFReady
          }
        }
      }
        DFLogger.managerFinish(this)
        DFManager.removeManager()
        if(DFWaiting.size!=0) {
          for (thread <- DFWaiting) {
            System.err.println(thread)
          }

          throw new DAGHungException("The Dag has hung: Somewhere a token is not being sent or not being sent to the correct thread.")
        }
    }
  }

   private class timer(timeout: Long, manager: DFManager) extends Thread {
    override def run() {
      Thread.sleep(timeout)
      manager.timeOut
    }
  }

  class TimeoutException(message: String) extends Exception(message)
  class DAGHungException(message: String) extends Exception(message)
}

object DFManager {
  
  private var pool:ExecutorService = if(System.getProperty( "threads") == null) 
                                         Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors * 2)
                                       else 
                                         Executors.newFixedThreadPool(Integer.parseInt(System.getProperty( "threads")))
  private var managers:Int = 0
  private val lock = new Object()
  
  def setThreadNumber(threads:Int) {
     stoppool
     requiredThreads = threads
     startpool                                                                    
  }

  private var requiredThreads = Runtime.getRuntime.availableProcessors * 2
  
  private def startpool() {
    pool = Executors.newFixedThreadPool(requiredThreads)
  }
  
  private def stoppool = pool.shutdown()
  
  private def addManager()
  {
   lock.synchronized{
     if(managers==0)
       startpool
     managers += 1
   }
  }
  
  private def removeManager()
  {
   lock.synchronized{
     managers -= 1
     if(managers==0)
       stoppool
   }
  }
                                                                               
  val currentThread: ThreadLocal[DFThread] = new ThreadLocal[DFThread]()

  private[dataflow] def registerDFThread(dft: DFThread): Unit = {
    currentThread.get().manager.registerDFThread(dft)
  }
  
  private def registerDFThread(manager:DFManager, dft: DFThread): Unit = {
    DFLogger.threadCreated(dft, manager)
    manager.registerDFThread(dft)
  }

  //DFThread0
  def createThread[R](f: () => R): DFThread0[R] = {
    createThread[R](f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[R](f: () => R, bIn: DFBarrier, bOut: DFBarrier): DFThread0[R] = {
    createThread[R](f, List(bIn), List(bOut))
  }

  def createThread[R](f: () => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread0[R] = {
    val thread = currentThread.get()
    val dt = new DFThread0Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }

  //DFThread1
  def createThread[T1, R](f: (T1) => R): DFThread1[T1, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, R](f: (T1) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread1[T1, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, R](f: (T1) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread1[T1, R] = {
    val thread = currentThread.get()
    val dt = new DFThread1Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }

  //DFThread2
  def createThread[T1, T2, R](f: (T1, T2) => R): DFThread2[T1, T2, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, R](f: (T1, T2) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread2[T1, T2, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, R](f: (T1, T2) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread2[T1, T2, R] = {
    val thread = currentThread.get()
    val dt = new DFThread2Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }

  //DFThread3
  def createThread[T1, T2, T3, R](f: (T1, T2, T3) => R): DFThread3[T1, T2, T3, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, R](f: (T1, T2, T3) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread3[T1, T2, T3, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, R](f: (T1, T2, T3) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread3[T1, T2, T3, R] = {
    val thread = currentThread.get()
    val dt = new DFThread3Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }

  //DFThread4
  def createThread[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R): DFThread4[T1, T2, T3, T4, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread4[T1, T2, T3, T4, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread4[T1, T2, T3, T4, R] = {
    val thread = currentThread.get()
    val dt = new DFThread4Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }

  //DFThread5
  def createThread[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R): DFThread5[T1, T2, T3, T4, T5, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread5[T1, T2, T3, T4, T5, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread5[T1, T2, T3, T4, T5, R] = {
    val thread = currentThread.get()
    val dt = new DFThread5Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //DFThread6
  def createThread[T1, T2, T3, T4, T5, T6, R](f: (T1, T2, T3, T4, T5, T6) => R): DFThread6[T1, T2, T3, T4, T5, T6, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, T5, T6, R](f: (T1, T2, T3, T4, T5, T6) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread6[T1, T2, T3, T4, T5, T6, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, T5, T6, R](f: (T1, T2, T3, T4, T5, T6) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread6[T1, T2, T3, T4, T5, T6, R] = {
    val thread = currentThread.get()
    val dt = new DFThread6Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //DFThread7
  def createThread[T1, T2, T3, T4, T5, T6, T7, R](f: (T1, T2, T3, T4, T5, T6, T7) => R): DFThread7[T1, T2, T3, T4, T5, T6, T7, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, R](f: (T1, T2, T3, T4, T5, T6, T7) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread7[T1, T2, T3, T4, T5, T6, T7, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, R](f: (T1, T2, T3, T4, T5, T6, T7) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread7[T1, T2, T3, T4, T5, T6, T7, R] = {
    val thread = currentThread.get()
    val dt = new DFThread7Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //DFThread8
  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, R](f: (T1, T2, T3, T4, T5, T6, T7, T8) => R): DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, R](f: (T1, T2, T3, T4, T5, T6, T7, T8) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, R](f: (T1, T2, T3, T4, T5, T6, T7, T8) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R] = {
    val thread = currentThread.get()
    val dt = new DFThread8Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //DFThread9
  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R): DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] = {
    val thread = currentThread.get()
    val dt = new DFThread9Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //DFThread10
  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) => R): DFThread10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] = {
    val thread = currentThread.get()
    val dt = new DFThread10Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //DFThread11
  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) => R): DFThread11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R] = {
    createThread(f, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) => R, bIn: DFBarrier, bOut: DFBarrier): DFThread11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R] = {
    createThread(f, List(bIn), List(bOut))
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) => R, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R] = {
    val thread = currentThread.get()
    val dt = new DFThread11Runable(f, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //List threads : Collector
  def createCollectorThread[E](no_args: Int): DFThread1[E,List[E]] = {
    createCollectorThread[E](no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createCollectorThread[E](no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFThread1[E,List[E]] = {
    createCollectorThread[E](no_args, List(bIn), List(bOut))
  }

  def createCollectorThread[E](no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread1[E,List[E]] = {
    val thread = currentThread.get()
    val dt = new DFCollectorThread[E](no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //List threads : Reducer
    def createReducerThread[E](f: (E,E) => E, no_args: Int): DFThread1[E,E] = {
    createReducerThread[E](f, no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createReducerThread[E](f: (E, E) => E, no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFThread1[E,E] = {
    createReducerThread[E](f, no_args, List(bIn), List(bOut))
  }

  def createReducerThread[E](f: (E, E)=> E, no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFThread1[E,E] = {
    val thread = currentThread.get()
    val dt = new DFReducerThread[E](f, no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  def createStartReducerThread[E](f: (E, E)=> E, no_args: Int, manager:DFManager): DFThread1[E,E] = {
    val dt = new DFReducerThread[E](f, no_args, List(): List[DFBarrier], List(): List[DFBarrier], true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  //List threads : Foldl
  def createFoldlThread[E, R](f: (R, E) => R, no_args: Int): DFListFoldThread[E,R] = {
    createFoldlThread[E, R](f, no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createFoldlThread[E, R](f: (R, E) => R, no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFListFoldThread[E,R] = {
    createFoldlThread[E, R](f, no_args, List(bIn), List(bOut))
  }

  def createFoldlThread[E,R](f: (R, E)=> R, no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFListFoldThread[E,R] = {
    val thread = currentThread.get()
    val dt = new DFFoldLeftThread[E,R](f, no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //List threads : Foldr
  def createFoldrThread[E, R](f: (E, R) => R, no_args: Int): DFListFoldThread[E,R] = {
    createFoldrThread[E, R](f, no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createFoldrThread[E, R](f: (E, R) => R, no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFListFoldThread[E,R] = {
    createFoldrThread[E, R](f, no_args, List(bIn), List(bOut))
  }

  def createFoldrThread[E,R](f: (E, R)=> R, no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFListFoldThread[E,R] = {
    val thread = currentThread.get()
    val dt = new DFFoldRightThread[E,R](f, no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //List threads : Collector
    def createOrderedCollectorThread[E:Manifest](no_args: Int): DFOrderedListThread[E, List[E]] = {
    createOrderedCollectorThread[E](no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createOrderedCollectorThread[E:Manifest](no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFOrderedListThread[E, List[E]] = {
    createOrderedCollectorThread[E](no_args, List(bIn), List(bOut))
  }

  def createOrderedCollectorThread[E:Manifest](no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFOrderedListThread[E, List[E]] = {
    val thread = currentThread.get()
    val dt: DFOrderedListThread[E, List[E]] = new DFOrderedCollectorThread[E](no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //List threads : Reducer
    def createOrderedReducerThread[E:Manifest](f: (E,E) => E, no_args: Int): DFOrderedListThread[E, E] = {
    createOrderedReducerThread[E](f, no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createOrderedReducerThread[E:Manifest](f: (E, E) => E, no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFOrderedListThread[E, E] = {
    createOrderedReducerThread[E](f, no_args, List(bIn), List(bOut))
  }

  def createOrderedReducerThread[E:Manifest](f: (E, E)=> E, no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFOrderedListThread[E, E] = {
    val thread = currentThread.get()
    val dt = new DFOrderedReducerThread[E](f, no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //List threads : Foldl
  def createOrderedFoldlThread[E:Manifest, R](f: (R, E) => R, no_args: Int): DFOrderedListFoldThread[E,R] = {
    createOrderedFoldlThread[E, R](f, no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createOrderedFoldlThread[E:Manifest, R](f: (R, E) => R, no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFOrderedListFoldThread[E,R] = {
    createOrderedFoldlThread[E, R](f, no_args, List(bIn), List(bOut))
  }

  def createOrderedFoldlThread[E:Manifest,R](f: (R, E)=> R, no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFOrderedListFoldThread[E,R] = {
    val thread = currentThread.get()
    val dt = new DFOrderedFoldLeftThread[E,R](f, no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //List threads : Foldr
  def createOrderedFoldrThread[E:Manifest, R](f: (E, R) => R, no_args: Int): DFOrderedListFoldThread[E,R] = {
    createOrderedFoldrThread[E, R](f, no_args, List(): List[DFBarrier], List(): List[DFBarrier])
  }

  def createOrderedFoldrThread[E:Manifest, R](f: (E, R) => R, no_args: Int, bIn: DFBarrier, bOut: DFBarrier): DFOrderedListFoldThread[E,R] = {
    createOrderedFoldrThread[E, R](f, no_args, List(bIn), List(bOut))
  }

  def createOrderedFoldrThread[E:Manifest,R](f: (E, R)=> R, no_args: Int, barIn: List[DFBarrier], barOut: List[DFBarrier]): DFOrderedListFoldThread[E,R] = {
    val thread = currentThread.get()
    val dt = new DFOrderedFoldRightThread[E,R](f, no_args, barIn, barOut, false, thread.manager)
    thread.addThread(dt)
    dt
  }
  
  //A thread that can run on a manager without having a DF thread to start it.
  private[dataflow] def createStartThread[R](f: () => R, manager: DFManager): DFThread0[R] = {
    val dt = new DFThread0Runable(f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }

  private[dataflow] def createStartThread[T1, R](f: (T1) => R, manager: DFManager): DFThread1[T1, R] = {
    val dt = new DFThread1Runable[T1, R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }

  private[dataflow] def createStartThread[T1, T2, R](f: (T1, T2) => R, manager: DFManager): DFThread2[T1, T2, R] = {
    val dt = new DFThread2Runable[T1, T2, R](f, List(), List(), true, manager)
   registerDFThread(manager, dt)
    dt
  }

  private[dataflow] def createStartThread[T1, T2, T3, R](f: (T1, T2, T3) => R, manager: DFManager): DFThread3[T1, T2, T3, R] = {
    val dt = new DFThread3Runable[T1,T2,T3,R](f, List(), List(), true, manager)
   registerDFThread(manager, dt)
    dt
  }

  private[dataflow] def createStartThread[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R, manager: DFManager): DFThread4[T1, T2, T3, T4, R] = {
    val dt = new DFThread4Runable[T1,T2,T3,T4,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  private[dataflow] def createStartThread[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R, manager: DFManager): DFThread5[T1, T2, T3, T4, T5, R] = {
    val dt = new DFThread5Runable[T1,T2,T3,T4,T5,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  private[dataflow] def createStartThread[T1, T2, T3, T4, T5, T6, R](f: (T1, T2, T3, T4, T5, T6) => R, manager: DFManager): DFThread6[T1, T2, T3, T4, T5, T6, R] = {
    val dt = new DFThread6Runable[T1,T2,T3,T4,T5,T6,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  private[dataflow] def createStartThread[T1, T2, T3, T4, T5, T6, T7, R](f: (T1, T2, T3, T4, T5, T6, T7) => R, manager: DFManager): DFThread7[T1, T2, T3, T4, T5, T6, T7, R] = {
    val dt = new DFThread7Runable[T1,T2,T3,T4,T5,T6,T7,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  private[dataflow] def createStartThread[T1, T2, T3, T4, T5, T6, T7, T8, R](f: (T1, T2, T3, T4, T5, T6, T7, T8) => R, manager: DFManager): DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R] = {
    val dt = new DFThread8Runable[T1,T2,T3,T4,T5,T6,T7,T8,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  private[dataflow] def createStartThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R, manager: DFManager): DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] = {
    val dt = new DFThread9Runable[T1,T2,T3,T4,T5,T6,T7,T8,T9,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  private[dataflow] def createStartThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) => R, manager: DFManager): DFThread10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] = {
    val dt = new DFThread10Runable[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
  
  private[dataflow] def createStartThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11) => R, manager: DFManager): DFThread11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, R] = {
    val dt = new DFThread11Runable[T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,R](f, List(), List(), true, manager)
    registerDFThread(manager, dt)
    dt
  }
}

trait DFApp {
    def main(args: Array[String]): Unit = {
        val manager = new DFManager
        var dft = DFManager.createStartThread(DFMain _, manager)
        dft.arg1 = args
        dft = null
        manager.run
    }
    
    def DFMain(args: Array[String]):Unit
}