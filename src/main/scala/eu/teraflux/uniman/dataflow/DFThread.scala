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

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArraySeq
import eu.teraflux.uniman.transactions.TMLib._

object DFState extends Enumeration {
  type DFState = Value
  val Initalising, Waiting, Ready, Running, Finished = Value
}

import DFState._

class Token[-T](f:Function[T,Unit])
{
  def apply(t:T) = f(t)
}

object NullToken extends Token[Any](_ => ())
{
  override def apply(t:Any) = DFLogger.nullTokenPassed(DFManager.currentThread.get)
}

abstract class DFThread(val manager: DFManager, val args: Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], val startThread:Boolean) extends Runnable{
  val logID:Int = DFLogger.getThreadID
  private val syncBits: BitMap = new BitMap(args)
  var state: DFState = Initalising
  private val inBarrierBits: BitMap = new BitMap(inBarriers.size)
  private val tokenList = new ListBuffer[(DFThread, Int)]
  private val threadList = new ListBuffer[DFThread]
  def nullToken:Token[Any] = NullToken
  def startthread = startThread
  def no_args = args

  for (b <- inBarriers)
    b.registerOutThread(this)
  for (b <- outBarriers)
    b.registerInThread(this)

  def addThread(thread: DFThread) {
    DFLogger.threadCreated(thread, this)
    threadList += thread
  }

  def activate() {
    state = Waiting
    checkReady()
  }

  // Passing tokens at end of thread
  def cleanup: Unit = {
    for (token <- tokenList) token._1.receiveToken(token._2)
    for (thread <- threadList) DFManager.registerDFThread(thread)
  }

  def signalFromBarrier(barr: DFBarrier): Unit = {
    var ind = inBarriers.indexWhere((d) => d == barr)
    inBarrierBits.set(ind + 1)
    checkReady
  }

  def runX()

  def checkReady() = {
    atomic{
      if (syncBits.isSet && inBarrierBits.isSet && state == Waiting) 
        state = Ready
      manager.wake()
    }
  }

  def passToken(dft: DFThread, id: Int) {
     DFLogger.tokenPassed(this, dft, id)
    //tokens don't actually escape thread till cleanup at the end
    atomic {
      tokenList += ((dft, id))
    }
  }

  def receiveToken(id: Int) = {
      require(!syncBits.isSet(id), "Received same input twice")
      syncBits.set(id)
      checkReady
  }

  def run = {
    try {
      DFManager.currentThread.set(this)
      DFLogger.threadStarted(this)
      runX
      DFLogger.threadFinished(this)
      cleanup
      for (b <- outBarriers)
        b.signal
      state = Finished
      manager.wake()
    } catch {
      case e: Throwable => { manager.sendException(e) }
    }
  }

  def isReady() = { state == Ready }
  def isRunning() = { state == Running }
  def isFinished() = { state == Finished }
  def isWaiting() = { state == Waiting }
  
  override def toString(): String = {
    val string = new StringBuilder()
    string.append("DFThread in state " + (if (state == DFState.Initalising) "Initalised\n"
    else if (state == DFState.Waiting) "Waiting\n"
    else if (state == DFState.Ready) "Ready\n"
    else if (state == DFState.Running) "Running\n"
    else "Finished\n"))
    string.append("Token State " + syncBits + " \n")
    string.append("Identity " + System.identityHashCode(this) + "\n")
    string.append("Name " + name + "\n")
    string.toString
  }
  
  var name : String = hashCode.toString
}

//Traits for passing basic threads
trait DFThread0[R] extends DFThread {  
  private val listenerList = new ListBuffer[Token[R]]
  protected var rval: R = _

  def notifyListeners = {
    for (l <- listenerList) l(rval)
  }
  
  def addListener(pass:Token[R]) = {
    atomic {
      if (state == Finished)
        pass(rval)
      else
        listenerList += (pass)
    }
  }
}

trait DFThread1[T1, R] extends DFThread0[R]{
  def arg1:T1
  def arg1_= (value:T1):Unit
  def token1:Token[T1] = new Token(arg1_= _)
}

trait DFThread2[T1, T2, R] extends DFThread1[T1, R]{
  def arg2:T2
  def arg2_= (value:T2):Unit
  def token2:Token[T2] = new Token(arg2_= _)
}

trait DFThread3[T1, T2, T3, R] extends DFThread2[T1, T2, R]{
  def arg3:T3
  def arg3_= (value:T3):Unit
  def token3:Token[T3] = new Token(arg3_= _)
}

trait DFThread4[T1, T2, T3, T4, R] extends DFThread3[T1, T2, T3, R]{
  def arg4:T4
  def arg4_= (value:T4):Unit
  def token4:Token[T4] = new Token(arg4_= _)  
}

trait DFThread5[T1, T2, T3, T4, T5, R] extends DFThread4[T1, T2, T3, T4, R]{
  def arg5:T5
  def arg5_= (value:T5):Unit
  def token5:Token[T5] = new Token(arg5_= _)  
}

trait DFThread6[T1, T2, T3, T4, T5, T6, R] extends DFThread5[T1, T2, T3, T4, T5, R]{
  def arg6:T6
  def arg6_= (value:T6):Unit
  def token6:Token[T6] = new Token(arg6_= _)  
}

trait DFThread7[T1, T2, T3, T4, T5, T6, T7, R] extends DFThread6[T1, T2, T3, T4, T5, T6, R]{
  def arg7:T7
  def arg7_= (value:T7):Unit
  def token7:Token[T7] = new Token(arg7_= _)  
}

trait DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R] extends DFThread7[T1, T2, T3, T4, T5, T6, T7, R]{
  def arg8:T8
  def arg8_= (value:T8):Unit
  def token8:Token[T8] = new Token(arg8_= _)  
}

trait DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] extends DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R]{
  def arg9:T9
  def arg9_= (value:T9):Unit
  def token9:Token[T9] = new Token(arg9_= _)  
}

trait DFThread10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] extends DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]{
  def arg10:T10
  def arg10_= (value:T10):Unit
  def token10:Token[T10] = new Token(arg10_= _)  
}

abstract class DFThread0X[R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread(manager, args, inBarriers, outBarriers, startThread)
  with DFThread0[R]

abstract class DFThread1X[T1, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread0X[R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread1[T1,R] {
  private var _arg1: Option[T1] = None
  def arg1:T1 = {
    _arg1 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  
  def arg1_= (value:T1):Unit = {
      _arg1 match {
      case None => { _arg1 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 1)
                       receiveToken(1)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,1)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread2X[T1, T2, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread1X[T1, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread2[T1, T2, R] {
  private var _arg2: Option[T2] = None
  
  def arg2:T2 = {
    _arg2 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg2_= (value:T2):Unit = {
    _arg2 match {
      case None => { _arg2 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 2)
                       receiveToken(2)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,2)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread3X[T1, T2, T3, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean)
  extends DFThread2X[T1, T2, R](manager, args, inBarriers, outBarriers, startThread)
  with DFThread3[T1, T2, T3, R]{
  
  private var _arg3: Option[T3] = None
  
  def arg3:T3 = {
    _arg3 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg3_= (value:T3):Unit = {
    _arg3 match {
      case None => { _arg3 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 3)
                       receiveToken(3)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,3)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread4X[T1, T2, T3, T4, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread3X[T1, T2, T3, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread4[T1, T2, T3, T4, R] {
  
  private var _arg4: Option[T4] = None
  
  def arg4:T4 = {
    _arg4 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg4_= (value:T4):Unit = {
    _arg4 match {
      case None => { _arg4 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 3)
                       receiveToken(4)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,4)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread5X[T1, T2, T3, T4, T5, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread4X[T1, T2, T3, T4, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread5[T1, T2, T3, T4, T5, R] {
  
  private var _arg5: Option[T5] = None
  
  def arg5:T5 = {
    _arg5 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg5_= (value:T5):Unit = {
    _arg5 match {
      case None => { _arg5 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 5)
                       receiveToken(5)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,5)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread6X[T1, T2, T3, T4, T5, T6, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread5X[T1, T2, T3, T4, T5, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread6[T1, T2, T3, T4, T5, T6, R] {
  
  private var _arg6: Option[T6] = None
  
  def arg6:T6 = {
    _arg6 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg6_= (value:T6):Unit = {
    _arg6 match {
      case None => { _arg6 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 6)
                       receiveToken(6)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,6)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread7X[T1, T2, T3, T4, T5, T6, T7, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread6X[T1, T2, T3, T4, T5, T6, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread7[T1, T2, T3, T4, T5, T6, T7, R] {
  
  private var _arg7: Option[T7] = None
  
  def arg7:T7 = {
    _arg7 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg7_= (value:T7):Unit = {
    _arg7 match {
      case None => { _arg7 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 7)
                       receiveToken(7)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,7)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread8X[T1, T2, T3, T4, T5, T6, T7, T8, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread7X[T1, T2, T3, T4, T5, T6, T7, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R] {
  
  private var _arg8: Option[T8] = None
  
  def arg8:T8 = {
    _arg8 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg8_= (value:T8):Unit = {
    _arg8 match {
      case None => { _arg8 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 8)
                       receiveToken(8)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,8)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread9X[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread8X[T1, T2, T3, T4, T5, T6, T7, T8, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] {
  
  private var _arg9: Option[T9] = None
  
  def arg9:T9 = {
    _arg9 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg9_= (value:T9):Unit = {
    _arg9 match {
      case None => { _arg9 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 9)
                       receiveToken(9)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,9)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

abstract class DFThread10X[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier],  startThread:Boolean) 
  extends DFThread9X[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](manager, args, inBarriers, outBarriers, startThread) 
  with DFThread10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] {
  
  private var _arg10: Option[T10] = None
  
  def arg10:T10 = {
    _arg10 match {
      case None => throw new Exception("Tried to read an unset argument")
      case Some(arg) => arg
    }
  }
  def arg10_= (value:T10):Unit = {
    _arg10 match {
      case None => { _arg10 = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 10)
                       receiveToken(10)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,10)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
}

class DFThread0Runable[R](f: () => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread0X[R](manager, 0, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f()
    notifyListeners
  }
}

class DFThread1Runable[T1, R](f: (T1) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread1X[T1, R](manager, 1, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1)
    notifyListeners
  }
}

class DFThread2Runable[T1, T2, R](f: (T1, T2) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread2X[T1, T2, R](manager, 2, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2)
    notifyListeners
  }
}

class DFThread3Runable[T1, T2, T3, R](f: (T1, T2, T3) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread3X[T1, T2, T3, R](manager, 3, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3)
    notifyListeners
  }
}

class DFThread4Runable[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread4X[T1, T2, T3, T4, R](manager, 4, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3, arg4)
    notifyListeners
  }
}

class DFThread5Runable[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread5X[T1, T2, T3, T4, T5, R](manager, 5, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3, arg4, arg5)
    notifyListeners
  }
}

class DFThread6Runable[T1, T2, T3, T4, T5, T6, R](f: (T1, T2, T3, T4, T5, T6) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread6X[T1, T2, T3, T4, T5, T6, R](manager, 6, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3, arg4, arg5, arg6)
    notifyListeners
  }
}

class DFThread7Runable[T1, T2, T3, T4, T5, T6, T7, R](f: (T1, T2, T3, T4 ,T5, T6, T7) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread7X[T1, T2, T3, T4, T5, T6, T7, R](manager, 7, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3, arg4, arg5, arg6, arg7)
    notifyListeners
  }
}

class DFThread8Runable[T1, T2, T3, T4, T5, T6, T7, T8, R](f: (T1, T2, T3, T4 ,T5, T6, T7, T8) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread8X[T1, T2, T3, T4, T5, T6, T7, T8, R](manager, 8, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
    notifyListeners
  }
}

class DFThread9Runable[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](f: (T1, T2, T3, T4 ,T5, T6, T7, T8, T9) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread9X[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](manager, 9, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)
    notifyListeners
  }
}

class DFThread10Runable[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](f: (T1, T2, T3, T4 ,T5, T6, T7, T8, T9, T10) => R, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFThread10X[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](manager, 10, inBarriers, outBarriers, startThread) {
  def runX() = {
    rval = f(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10)
    notifyListeners
  }
}

abstract class DFLThread[E, R](manager: DFManager, args:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean)
  extends DFThread(manager, args, inBarriers, outBarriers, startThread)
  with DFThread0[R] {
  protected def listArg:List[E]
}

trait DFFold[E,R] extends DFLThread[E,R] {
    private var _accumulator: Option[R] = None
  
  def accumulator:R = {
    _accumulator match {
      case None => throw new Exception("Tried to read an unset accumulator")
      case Some(arg) => arg
    }
  }
  def accumulator_= (value:R):Unit = {
    _accumulator match {
      case None => { _accumulator = Some(value)
                     if(startThread)
                     {
                       DFLogger.tokenPassed(manager, this, 2)
                       receiveToken(2)
                     }
                     else
                       DFManager.currentThread.get().passToken(this,2)
                   }
      case Some(arg) => throw new Exception("Tried to write an already written argument")
    }
  }
  
  def accumulatorToken():Token[R] = new Token(accumulator_= _)
}

trait DFListFoldThread[E,R] extends DFListThread[E,R] with DFFold[E,R]

trait DFFoldLeft[E, R] extends DFFold[E,R]{
  protected val f:(R,E) => R
  def runX() = {
    rval = listArg.foldLeft(accumulator) (f)
    notifyListeners
  }
}

trait DFFoldRight[E, R] extends DFFold[E,R]{
  protected val f:(E,R) => R
  def runX() = {
    rval = listArg.foldRight(accumulator) (f)
    notifyListeners
  }
}

trait DFReducer[E] extends DFLThread[E,E] {
  protected val f:(E,E) => E
  def runX() = {
    val arg = listArg
    rval = listArg.reduce(f)
    notifyListeners
  }
}

trait DFCollector[E] extends DFLThread[E, List[E]]{
  def runX() = {
    rval = listArg 
    notifyListeners
  }
}

abstract class DFListThread[E,R](manager: DFManager, args:Int,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean) 
extends DFLThread[E,R](manager, args, inBarriers, outBarriers, startThread)
  with DFThread1[E,R]
{
  private var _arg1: List[E] = List()
  private var received_inputs = 0
  protected def listArg:List[E] = _arg1
  
  def arg1_= (value:E):Unit = {
	atomic{
	  _arg1 = value::_arg1
	  if(startThread)
	  {
            DFLogger.tokenPassed(manager, this, 1)
	    receiveToken(1)
	  }
	  else
	  DFManager.currentThread.get().passToken(this,1)  
	}
  }
  
  def arg1:E = throw new Exception("Attempted to read indvidual elements out of a collector thread")

  override def no_args = {
    no_inputs + args - 1;
  }

  override def receiveToken(id: Int) = {
    id match {
      case 1 => atomic { 
      assert(received_inputs != no_inputs, "Too many inputs passed: " + received_inputs + " != " + no_inputs /* + " " + (received_inputs != no_inputs).toString */)
      received_inputs += 1
      if(received_inputs == no_inputs)
        super.receiveToken(id)
    }
      case _ => super.receiveToken(id)
    }
  }
}

class DFCollectorThread[E] (no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFListThread[E,List[E]](manager: DFManager, 1,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean)
  with DFCollector[E]

class DFReducerThread[E] (protected val f:(E,E) => E,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFListThread[E,E](manager: DFManager, 1,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean) 
  with DFReducer[E]

class DFFoldLeftThread[E,R](protected val f:(R,E) => R,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFListThread[E,R](manager,  2, no_inputs, inBarriers, outBarriers, startThread)
  with DFListFoldThread[E,R]
  with DFFoldLeft[E,R]

class DFFoldRightThread[E,R](protected val f:(E,R) => R,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFListThread[E,R](manager,  2, no_inputs, inBarriers, outBarriers, startThread)
  with DFListFoldThread[E,R]
  with DFFoldRight[E,R]

abstract class DFOrderedListThread[E, R](manager: DFManager, args:Int,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean) 
  extends DFLThread[E, R](manager, args, inBarriers, outBarriers, startThread)
  {
  private val _arg1:ArraySeq[E] = new ArraySeq[E](no_inputs)
  private val arrived:ArraySeq[Boolean] = new ArraySeq[Boolean](no_inputs)
  
  private var recieved_inputs = 0
  def listArg:List[E] = _arg1.toList
  
  def update(pos:Int, value:E):Unit = {
	atomic{ 
	  assert(!arrived(pos), "Value has alread been set")
	  arrived(pos) = true
	  _arg1(pos) = value
	  if(startThread)
	  {
	    DFLogger.tokenPassed(manager, this, 1)
	    receiveToken(1)
	  }
	  else
	    DFManager.currentThread.get().passToken(this,1)
	}
  }
  
  def token(pos:Int):Token[E] = new Token(update(pos, _))
  
  override def receiveToken(id: Int) = {
    id match {
      case 1 => atomic { 
      recieved_inputs += 1
      if(recieved_inputs == no_inputs)
        super.receiveToken(id)
    }
      case _ => super.receiveToken(id)
    }
  }
}

abstract class DFOrderedListFoldThread[E, R] (manager: DFManager, args:Int,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean)
  extends DFOrderedListThread[E, R] (manager, args,  no_inputs, inBarriers, outBarriers, startThread)
  with DFFold[E,R]

class DFOrderedFoldLeftThread[E,R](protected val f:(R,E) => R,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFOrderedListFoldThread[E,R](manager,  2, no_inputs, inBarriers, outBarriers, startThread) 
  with DFFoldLeft[E,R]

class DFOrderedFoldRightThread[E,R](protected val f:(E,R) => R,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFOrderedListFoldThread[E,R](manager,  2, no_inputs, inBarriers, outBarriers, startThread)
  with DFFoldRight[E,R]

class DFOrderedCollectorThread[E] (no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFOrderedListThread[E, List[E]](manager: DFManager, 1,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean)
  with DFCollector[E]

class DFOrderedReducerThread[E] (protected val f:(E,E) => E,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean, manager: DFManager) 
  extends DFOrderedListThread[E, E](manager: DFManager, 1,  no_inputs:Int, inBarriers: List[DFBarrier], outBarriers: List[DFBarrier], startThread:Boolean) 
  with DFReducer[E]
