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

//Rewrite efficiently.
class BitMap(nBits: Int) {
  var bMap = new Array[Boolean](nBits)
  val size = nBits
  def set(i: Int): Unit = { bMap(i - 1) = true }
  def clear(i: Int): Unit = { bMap(i - 1) = false }
  def isSet(): Boolean = if (nBits == 0) true else { bMap.reduceLeft[Boolean]({ (acc, n) => acc && n }) }
  def isSet(i: Int): Boolean = bMap(i - 1)
  def setCount(): Int = if (nBits == 0) 0 else { bMap.foldLeft(0)((acc, n) => if (n) acc + 1 else acc) }

  override def toString(): String = {
    val string = new StringBuilder()
    if (nBits == 0)
      string.append("No Imputs")
    else {
      string.append("0: ")
      string.append(if (bMap(0)) "received" else "waiting")
      var i = 1
      while (i < nBits) {
        string.append(", " + i + ": " + (if (bMap(i)) "received" else "waiting"))
        i += 1
      }
    }
    string.toString
  }
}
