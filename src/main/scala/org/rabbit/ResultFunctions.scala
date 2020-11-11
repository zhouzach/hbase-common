package org.rabbit

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

object ResultFunctions {

  val rowKeyAndResult = (t:(ImmutableBytesWritable, Result)) => {//(rowKey,result)
    val (rowKey,result) = t

    val it = result.listCells().iterator() //取出某个rowKey的所有qualifier
    val b = new StringBuilder

    b.append(Bytes.toString(rowKey.get())+":")

    while (it.hasNext) {
      val cell = it.next()
      val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)) //取出列名
      if (qualifier.equals("counter")) { // 根据不同的列，选择不同的反序列化方法
        b.append("(" + qualifier + "," + Bytes.toLong(CellUtil.cloneValue(cell)) + ")")
      } else {
        b.append("(" + qualifier + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")
      }
    }
    b.toString()
  }

  val resultHandler= (result: Result) => {//某个rowKey的result

    val it = result.listCells().iterator() //取出某个rowKey的所有qualifier
    val b = new StringBuilder

    b.append(Bytes.toString(result.getRow) + ":")

    while (it.hasNext) {
      val cell = it.next()
      val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)) //取出列名
      if (qualifier.equals("counter")) { // 根据不同的列，选择不同的反序列化方法
        b.append("(" + qualifier + "," + Bytes.toLong(CellUtil.cloneValue(cell)) + ")")
      } else {
        b.append("(" + qualifier + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")
      }
    }
    b.toString()
  }



}
