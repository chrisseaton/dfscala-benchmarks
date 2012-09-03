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

class NestedGraph[T] {
  private var value: Option[T] = None
  private val manager = new DFManager
  val token1:Token[T] = new Token(setResult _)

  def resultReady: Boolean = {
    value match {
      case None => false
      case Some(result) => true
    }
  }

  private def setResult(result: T) = {
    value match {
      case None => value = Some(result)
      case Some(x) => throw new Exception("Attempted to set already set result")
    }
  }

  private def getResult(): T = {
    value match {
      case None => throw new Exception("The result has not been set yet")
      case Some(result) => result
    }
  }

  def createThread[R](f: Unit => R): T = {
    val dft: DFThread0[R] = DFManager.createStartThread(f, manager)
    manager.run
    getResult()
  }

  def createThread[T1, R](f: (T1) => R, arg1: T1): T = {
    val dft: DFThread1[T1, R] = DFManager.createStartThread(f, manager)
    dft.arg1 = arg1
    manager.run
    getResult()
  }

  def createThread[T1, T2, R](f: (T1, T2) => R, arg1: T1, arg2: T2): T = {
    val dft: DFThread2[T1, T2, R] = DFManager.createStartThread(f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, R](f: (T1, T2, T3) => R, arg1: T1, arg2: T2, arg3: T3): T = {
    val dft: DFThread3[T1, T2, T3, R] = DFManager.createStartThread(f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R, arg1: T1, arg2: T2, arg3: T3, arg4: T4): T = {
    val dft: DFThread4[T1, T2, T3, T4, R] = DFManager.createStartThread[T1, T2, T3, T4, R](f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    dft.arg4 = arg4
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R, arg1: T1, arg2: T2, arg3: T3, arg4: T4, arg5: T5): T = {
    val dft: DFThread5[T1, T2, T3, T4, T5, R] = DFManager.createStartThread[T1, T2, T3, T4, T5, R](f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    dft.arg4 = arg4
    dft.arg5 = arg5
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, T4, T5, T6, R](f: (T1, T2, T3, T4, T5, T6) => R, arg1: T1, arg2: T2, arg3: T3, arg4: T4, arg5: T5, arg6: T6): T = {
    val dft: DFThread6[T1, T2, T3, T4, T5, T6, R] = DFManager.createStartThread[T1, T2, T3, T4, T5, T6, R](f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    dft.arg4 = arg4
    dft.arg5 = arg5
    dft.arg6 = arg6
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, R](f: (T1, T2, T3, T4, T5, T6, T7) => R, arg1: T1, arg2: T2, arg3: T3, arg4: T4, arg5: T5, arg6: T6, arg7: T7): T = {
    val dft: DFThread7[T1, T2, T3, T4, T5, T6, T7, R] = DFManager.createStartThread[T1, T2, T3, T4, T5, T6, T7, R](f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    dft.arg4 = arg4
    dft.arg5 = arg5
    dft.arg6 = arg6
    dft.arg7 = arg7
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, R](f: (T1, T2, T3, T4, T5, T6, T7, T8) => R, arg1: T1, arg2: T2, arg3: T3, arg4: T4, arg5: T5, arg6: T6, arg7: T7, arg8 : T8): T = {
    val dft: DFThread8[T1, T2, T3, T4, T5, T6, T7, T8, R] = DFManager.createStartThread[T1, T2, T3, T4, T5, T6, T7, T8, R](f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    dft.arg4 = arg4
    dft.arg5 = arg5
    dft.arg6 = arg6
    dft.arg7 = arg7
    dft.arg8 = arg8
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R, arg1: T1, arg2: T2, arg3: T3, arg4: T4, arg5: T5, arg6: T6, arg7: T7, arg8 : T8, arg9 : T9): T = {
    val dft: DFThread9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] = DFManager.createStartThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    dft.arg4 = arg4
    dft.arg5 = arg5
    dft.arg6 = arg6
    dft.arg7 = arg7
    dft.arg8 = arg8
    dft.arg9 = arg9
    manager.run
    getResult()
  }

  def createThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) => R, arg1: T1, arg2: T2, arg3: T3, arg4: T4, arg5: T5, arg6: T6, arg7: T7, arg8 : T8, arg9 : T9, arg10 : T10): T = {
    val dft: DFThread10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R] = DFManager.createStartThread[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](f, manager)
    dft.arg1 = arg1
    dft.arg2 = arg2
    dft.arg3 = arg3
    dft.arg4 = arg4
    dft.arg5 = arg5
    dft.arg6 = arg6
    dft.arg7 = arg7
    dft.arg8 = arg8
    dft.arg9 = arg9
    dft.arg10 = arg10
    manager.run
    getResult()
  }
}