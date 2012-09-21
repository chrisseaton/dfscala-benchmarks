/*

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

package eu.teraflux.uniman.dataflow.benchmark

import eu.teraflux.uniman.dataflow._

object Fibonacci2 extends DFApp{

  def DFMain(args:Array[String]):Unit = {
     var t1 = DFManager.createThread(fib2 _)
     t1.arg1 = 10
     t1.arg2 = new Token((x:Int) => { println("result = " + x)})
   }

   def fib2(n :Int , out:Token[Int]){
      if(n <= 2)
         out(1)
      else {
        var t1 = DFManager.createThread((x:Int, y:Int, out:Token[Int]) => out(x + y))
        var t2 = DFManager.createThread(fib2 _)
        var t3 = DFManager.createThread(fib2 _)

        t2.arg1 = n - 1
        t2.arg2 = t1.token1

        t3.arg1 = n - 2
        t3.arg2 = t1.token2

        t1.arg3 = out
     }
   }
}//class Fibonacci


