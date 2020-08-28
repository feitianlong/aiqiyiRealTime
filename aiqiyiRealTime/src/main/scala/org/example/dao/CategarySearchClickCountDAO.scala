package org.example.dao

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.example.domain.CategorySearchClickCount
import org.example.util.HBaseUtil

import scala.collection.mutable.ListBuffer

object CategarySearchClickCountDAO {
  val tableName = "category_search_cout"
  val columnfamily = "columnfamily_info"
  val qualifer= "click_count"

  /**
   * 保存数据
   * @param list
   */
  def save(list:ListBuffer[CategorySearchClickCount]): Unit ={
    val table =  HBaseUtil.getInstance().getTable(tableName)
    for(line <- list){
      table.incrementColumnValue(Bytes.toBytes(line.day_search_category),Bytes.toBytes(columnfamily),Bytes.toBytes(qualifer),line.clickCount);
    }

  }

  def count(day_categary:String) : Long={
    val table  =HBaseUtil.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_categary))
    val  value =  table.get(get).getValue(Bytes.toBytes(columnfamily), Bytes.toBytes(qualifer))
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }

}
