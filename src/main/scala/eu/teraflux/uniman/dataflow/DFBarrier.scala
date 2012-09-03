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
import eu.teraflux.uniman.transactions.TMLib._

object DFBarrier {
  val NoBarrier:DFBarrier = new DFVoidBarrier
 
}
//A class used as a singleton to implement a null barrier
class DFVoidBarrier extends DFBarrier {
    override def registerOutThread(dft: DFThread) { dft.signalFromBarrier(this)	}
	override def registerInThread(dft:DFThread) {}
	override def signal() {}
	override def makeReady() {}
}

class DFBarrier(sC: Int) {
  val logID:Int = DFLogger.getBarrierID
  DFLogger.barrierCreated(this, sC)
  private var syncCount: Int = if (sC == -1) 0 else sC
  private var ready: Boolean = (sC != -1)
  private var outThreads: ListBuffer[DFThread] = new ListBuffer[DFThread]
  
  if(ready) DFLogger.barrierActivated(this)
  
  def this() = this(-1)

  def registerOutThread(dft: DFThread) {
    DFLogger.threadFromBarrier(dft, this)
    atomic {
    if (syncCount == 0 && ready)
      dft.signalFromBarrier(this)
    else
      outThreads += dft
    }
  }

  def registerInThread(dft: DFThread) {
    DFLogger.threadToBarrier(dft, this)
    if (sC == -1)
      syncCount += 1
  }

  def signal() {
    var signal: Boolean = false

    atomic {
      syncCount -= 1
      signal = (syncCount == 0 && ready)
    }

    if (signal)
      signalOutThreads
  }

  def makeReady() {
    var signal: Boolean = false

    atomic {
      ready = true
      signal = (syncCount == 0)
    }
   
    DFLogger.barrierActivated(this)
    
    if (signal)
      signalOutThreads
  }

  private def signalOutThreads() {
    DFLogger.barrierSatisfied(this)
    for (l <- outThreads)
      l.signalFromBarrier(this)
  }
}
