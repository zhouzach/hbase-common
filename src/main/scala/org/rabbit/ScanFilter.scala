package org.rabbit

import java.util

import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

/**
 * https://blog.csdn.net/Dante_003/article/details/79232926
 */
object ScanFilter {

  def byRowRange(scan: Scan, startRow: String, stopRow: String) = {
    scan.withStartRow(Bytes.toBytes(startRow))
    scan.withStopRow(Bytes.toBytes(stopRow))
  }

  def byRowPrefix(scan: Scan, prefix: String) = {
    scan.setRowPrefixFilter(Bytes.toBytes(prefix))
  }

  def byRowValue(scan: Scan, value: String) = {
    val rf = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(value)))
    scan.setFilter(rf)
  }

  def byRowFilterForRegex(scan: Scan) = {
//    val rowComparator = new RegexStringComparator("\\d+_12345*")
    //https://stackoverflow.com/questions/35222278/filter-rows-in-hbase-based-on-partial-row-keys
    val rowComparator = new RegexStringComparator(".2020-07-22.")
    val filter = new RowFilter(CompareOperator.EQUAL,rowComparator)
    scan.setFilter(filter)
  }

  def bySingleColumnValue(scan: Scan,family: String, qualifier: String,value: String) = {
    val filter = new SingleColumnValueFilter(
      Bytes.toBytes(family),
      Bytes.toBytes(qualifier),
      CompareOperator.EQUAL,
      Bytes.toBytes(value))
    filter.setFilterIfMissing(true)

    scan.setFilter(filter)
  }

  def bySingleColumnValueForRegex(scan: Scan,family: String, qualifier: String) = {

    val comparator = new RegexStringComparator(".ac.")
    val filter = new SingleColumnValueFilter(
      Bytes.toBytes(family),
      Bytes.toBytes(qualifier),
      CompareOperator.EQUAL,
      comparator)
    filter.setFilterIfMissing(true)

    scan.setFilter(filter)
  }

  def keyOnlyFilter(scan: Scan) = {
    scan.setFilter(new KeyOnlyFilter())
  }

  def byFilterList(scan: Scan) = {
    val filters = new util.ArrayList[Filter]()

    val pf = new PrefixFilter(Bytes.toBytes("row")); // OK  筛选匹配行键的前缀成功的行

    val cpf = new ColumnPrefixFilter(Bytes.toBytes("qual1"))

    val vf = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("ROW2_QUAL1"))
    val skf = new SkipFilter(vf) //如果发现一行中的某一列不符合条件，那么整行就会被过滤掉

    val ccf = new ColumnCountGetFilter(2)//这个过滤器来返回每行最多返回多少列，并在遇到一行的列数超过我们所设置的限制值的时候，结束扫描操作
    val ko = new KeyOnlyFilter()

    val rf = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("row")))
    val wmf = new WhileMatchFilter(rf)

    val rrf = new RandomRowFilter(0.8f); // OK 随机选出一部分的行

    val fkof = new FirstKeyOnlyFilter()


    //    new SingleColumnValueExcludeFilter

    filters.add(cpf)

    val fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters)
    scan.setFilter(fl)
  }


}
