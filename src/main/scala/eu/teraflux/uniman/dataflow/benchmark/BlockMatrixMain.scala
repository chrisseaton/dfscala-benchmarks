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

object BlockMatrixMain extends DFApp{
  
  val total_runs = 10
  var runs = total_runs
  var size = 256
  val increment = 256
  val max_size = 2048
  def DFMain(args: Array[String]){
    
    //Matrix(y,x) 


    //matrixA.printMatrix()
    //matrixB.printMatrix()
    
    //non parallel matrix multiplication
    //var result = matrixA.multiplyMatrix(matrixB)
    //print("Result "); result.printMatrix()
    
    //parallel block matrix multiplication 

    val threads = Integer.parseInt(args(0))
    DFManager.setThreadNumber(threads)
    println("Threads " + threads)
    allocateMatrices()
  }
  
  def allocateMatrices()
  {
    if(size <= max_size)
    {
    println("\nMatrix size: " + size + ", ")
    size += increment
    var matrixA :Matrix = new Matrix(size,size); 
    var matrixB :Matrix = new Matrix(size,size); 
    testMatrix(matrixA, matrixB)
    }
  }
  
  def testMatrix(matrixA :Matrix,matrixB :Matrix)
  {
    matrixA.fillMatrix()
    matrixB.fillMatrix()
    runs -= 1
    matrixA.blockMatrixMultiply(matrixB)
  }
  
  def printTime(matrixA :Matrix,matrixB :Matrix) {
    print(Timer.stop() + ", ")
    if(runs > 0) {
      val t = DFManager.createThread(testMatrix _)
      t.arg1 = matrixA
      t.arg2 = matrixB
    }
    else {
      runs = total_runs
      allocateMatrices()
    }
  }
    
}//class BlockMatrix