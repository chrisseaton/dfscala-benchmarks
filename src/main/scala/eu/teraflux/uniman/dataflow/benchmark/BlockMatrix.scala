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

class BlockMatrix() {

  private var blockMatrix :Array[Array[Matrix]] = null
  private var extraRow :Int = 0
  private var extraCol :Int = 0
  
  def createBlockMatrix(matrix :Matrix, blockRowSize :Int, blockColSize :Int){
    var rowReminder :Int = matrix.getRowSize() % blockRowSize
    var colReminder :Int = matrix.getColumnSize() % blockColSize
    if(rowReminder != 0){
      extraRow = blockRowSize - rowReminder
    }
    if(colReminder != 0){
      extraCol = blockColSize - colReminder
    }
    var blockMatrixRowSize :Int = (matrix.getRowSize + extraRow)/blockRowSize
    var blockMatrixColSize :Int = (matrix.getColumnSize + extraCol)/blockColSize
    this.blockMatrix = Array.ofDim[Matrix](blockMatrixRowSize,blockMatrixColSize)
    for(r<-0 until blockMatrixRowSize){
      for(c<-0 until blockMatrixColSize){
        blockMatrix(r)(c) = new Matrix(blockRowSize, blockColSize) 
      }
    }
    var r2 :Int = 0
    var c2 :Int = 0
    var blockR :Int = 0
    var blockC :Int = 0
    var count1 :Int = 0
    var count2 :Int = 0
    var value :Int = 0
    for(r<-0 until matrix.getRowSize() + extraRow){
      for(c<-0 until matrix.getColumnSize() + extraCol){
        if(c < matrix.getColumnSize() && r < matrix.getRowSize())
          value = matrix.getValue(r,c)
        else
          value = 0
        
        this.blockMatrix(blockR)(blockC).setValue(r2,c2,value)
        count1 += 1
        c2 += 1
        if(count1 == blockColSize){
          count1 = 0
          blockC += 1
          c2 = 0
        }
      }
      r2 += 1
      count2 += 1
      if(count2 == blockRowSize){
        r2 = 0
        count2 = 0
        blockR += 1
      }
      blockC = 0
    }
  }//createBlockMatrix
  
  def printResult() :Unit = {
    var blockMatrixRowSize = this.blockMatrix.length
    var blockMatrixColSize = this.blockMatrix(0).length
    var blockRowSize = this.blockMatrix(0)(0).getRowSize()
    var blockColSize = this.blockMatrix(0)(0).getColumnSize()
    var resultMatrixRowSize = (blockRowSize * blockMatrixRowSize) - extraRow
    var resultMatrixColSize = (blockColSize * blockMatrixColSize) - extraCol
    var resultMatrix :Matrix = new Matrix(resultMatrixRowSize, resultMatrixColSize)
    var r2 :Int = 0
    var c2 :Int = 0
    var blockR :Int = 0
    var blockC :Int = 0
    var count1 :Int = 0
    var count2 :Int = 0
    for(r<-0 until resultMatrixRowSize){
      for(c<-0 until resultMatrixColSize){
        resultMatrix.setValue(r,c, this.getMatrix(blockR,blockC).getValue(r2,c2))
        count1 += 1
        c2 += 1
        if(count1 == blockColSize){
          count1 = 0
          blockC += 1
          c2 = 0
        }
      }
      r2 += 1
      count2 += 1
      if(count2 == blockRowSize){
        r2 = 0
        count2 = 0
        blockR += 1
      }
      blockC = 0
    }
    var t1 = DFManager.createThread(resultMatrix.printMatrix _)
  }//printResult()
  
  def createResultMatrix(row :Int , col :Int, extraR :Int, extraC :Int){
    blockMatrix = Array.ofDim[Matrix](row,col)
    this.extraRow = extraR
    this.extraCol = extraC
  }
  
  def getMatrix(row :Int , col :Int) :Matrix = {
    return blockMatrix(row)(col)
  }
  
  def mapMatrix(row :Int, col :Int, matrix :Matrix) :Unit = {
    blockMatrix(row)(col) = matrix
  }
   
  def getRowSize() :Int = {
    return this.blockMatrix.length
  }
  
  def getColumnSize() :Int = {
    return this.blockMatrix(0).length
  }
  
  def getExtraRow() :Int = {
    return this.extraRow
  }
  
  def getExtraCol() :Int = {
    return this.extraCol
  }
  
  def setExtraRow(row :Int) :Unit = {
    extraRow = row
  }
  
  def setExtraCol(col :Int) :Unit = {
    extraCol = col
  }
  
  def printBlockMatrix() :Unit = {
    for(r<-0 until this.getRowSize()){
      for(c<-0 until this.getColumnSize()){
        this.blockMatrix(r)(c).printMatrix() 
      }
    }
  }

}//class BlockMatrix



