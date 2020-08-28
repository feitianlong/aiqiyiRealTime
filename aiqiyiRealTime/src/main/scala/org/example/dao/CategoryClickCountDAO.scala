package org.example.dao


import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.example.domain.CategoryClickCount
import org.example.util.HBaseUtil

import scala.collection.mutable.ListBuffer

object CategoryClickCountDAO {

  val tableName = "category_clickcount"
  val columnfamily = "columnfamily_info"
  val qualifer= "click_count"

  def save(list: ListBuffer[CategoryClickCount]): Unit = {
    val table = HBaseUtil.getInstance().getTable(tableName)
    for( line <- list){
      table.incrementColumnValue(Bytes.toBytes(line.categoryId),Bytes.toBytes(columnfamily),Bytes.toBytes(qualifer),line.clickCount)
    }
  }

  def count(day_category:String) : Long ={
    val table = HBaseUtil.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_category))
    val value = table.get(get).getValue(Bytes.toBytes(columnfamily),Bytes.toBytes(qualifer))
    if(value == null){
      0L
    }else {
      Bytes.toLong(value)
    }
  }



}
