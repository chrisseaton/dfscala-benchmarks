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
import scala.util.Random
import java.util.Calendar

object Matrix {
  private val randomNumber = new Random(Calendar.getInstance().get(Calendar.DATE))
}

class Matrix(sizeX :Int , sizeY :Int) {
  
  private var matrix :Array[Array[Int]] = Array.ofDim[Int](sizeX,sizeY)
  private var resultBlockSize = 256 //matrix A block row size and matrix B column size 
  private var blockColSizeA = 256 //matrix A's block column size
  private var blockRowSizeB = 256 //matrix B's block row size
  
  def fillMatrix() :Unit = {

    for(i<-0 until this.matrix.length){
      for(j<-0 until this.matrix(0).length){
        this.matrix(i)(j) = Matrix.randomNumber.nextInt(10)  
      }
    }
  }
 
  def multiplyMatrix(other :Matrix) :Matrix = {
    var result :Matrix = new Matrix(this.matrix.length , other.matrix(0).length)
    var sum :Int = 0
    for(r<-0 until this.matrix.length){
      for(c<-0 until other.matrix(0).length){
        sum = 0
        for(k<-0 until this.matrix(0).length){
          sum += this.matrix(r)(k) * other.matrix(k)(c)
        }
        result.matrix(r)(c) = sum
      }
    }
    return result
  }
   
  def blockMatrixMultiply(other :Matrix) :Unit = {
    var blockMatrixA : BlockMatrix = new BlockMatrix()
    blockMatrixA.createBlockMatrix(this, resultBlockSize,blockColSizeA)
    var blockMatrixB : BlockMatrix = new BlockMatrix()
    blockMatrixB.createBlockMatrix(other, blockRowSizeB,resultBlockSize)
    var blockResult : BlockMatrix = new BlockMatrix()
    blockResult.createResultMatrix(blockMatrixA.getRowSize() , blockMatrixB.getColumnSize(), blockMatrixA.getExtraRow(), blockMatrixB.getExtraCol())
    Timer.start()
    var d1 :DFBarrier = new DFBarrier
    for(r<-0 until blockResult.getRowSize()){
      for(c<-0 until blockResult.getColumnSize()){
        var t1 = DFManager.createThread(calculateBlockResult _, DFBarrier.NoBarrier, d1)
        t1.arg1 = r 
        t1.arg2 = c
        t1.arg3 = blockMatrixA
        t1.arg4 = blockMatrixB
        t1.arg5 = blockResult
      }
    }
    d1.makeReady()
    //print("Block Multiply Result ")
    var t2 = DFManager.createThread(BlockMatrixMain.printTime _, d1, DFBarrier.NoBarrier)
    t2.arg1 = this
    t2.arg2 = other
  }
  

       
  private def calculateBlockResult(resultRow :Int, resultCol :Int, blockMatrixA :BlockMatrix, blockMatrixB :BlockMatrix, blockResult :BlockMatrix) :Unit = {
    var matrixRowSize = blockMatrixA.getMatrix(0,0).getRowSize()
    var matrixColSize = blockMatrixB.getMatrix(0,0).getColumnSize()
    var sum :Matrix = new Matrix(matrixRowSize,matrixColSize)  
    var r = resultRow
    var c = resultCol
    for(k<-0 until blockMatrixA.getColumnSize()){
      sum = sum.addMatrix(blockMatrixA.getMatrix(r,k).multiplyMatrix(blockMatrixB.getMatrix(k,c)))
    }
    blockResult.mapMatrix(resultRow,resultCol,sum)
  }
    
  def addMatrix(other :Matrix) :Matrix = {
    var result :Matrix = new Matrix(this.matrix.length , this.matrix(0).length)
    for(r<-0 until this.matrix.length){
      for(c<-0 until this.matrix(0).length){
        result.matrix(r)(c) = this.matrix(r)(c) + other.matrix(r)(c)       
      }
    }
    return result
  }
  
  def subtractMatrix(other :Matrix) :Matrix = {
    var result :Matrix = new Matrix(this.matrix.length , this.matrix(0).length)
    for(r<-0 until this.matrix.length){
      for(c<-0 until this.matrix(0).length){
        result.matrix(r)(c) = this.matrix(r)(c) - other.matrix(r)(c)       
      }
    }
    return result
  }
  
  def getRowSize() :Int = {
    return this.matrix.length
  }
  
  def getColumnSize() :Int = {
    return this.matrix(0).length
  }
  
  def setValue(row :Int, col :Int, value :Int) :Unit = {
    this.matrix(row)(col) = value
  }
  
  def getValue(row :Int, col :Int) :Int = {
    return this.matrix(row)(col)
  }
  
  def printMatrix() :Unit = {
    println("Matrix :")
    for(i<-0 until this.matrix.length){
      for(j<-0 until this.matrix(0).length){
        print(this.matrix(i)(j) + " ")
      }
      println("")
    }
    println("") 
  }
  
}//class Matrix

