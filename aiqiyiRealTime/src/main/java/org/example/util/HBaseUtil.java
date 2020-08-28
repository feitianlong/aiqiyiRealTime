package org.example.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HBaseUtil(){
        configuration = new Configuration();
        configuration.set("habse.zookeeper.quorum","hdp-01:2181,hdp-02:2181,hdp-03:2181");
        configuration.set("hbase.rootdir","hdfs://aiqiyi/hbase");
        try{
            admin = new HBaseAdmin(configuration);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 单例实现HBaseUtil
     */
    private static HBaseUtil instance = null;
    public static  HBaseUtil getInstance() {
        if (instance == null){
            synchronized(HBaseUtil.class) {
                if (null == instance ){
                    instance = new HBaseUtil();
                }
            }
        }

        return instance;
    }

    /**
     * 根据表名获取Htable 实例
     */
    public HTable getTable(String tablename){
        HTable table = null;
        try{
            table = new HTable(configuration,tablename);
        }catch(IOException e){
            e.printStackTrace();
        }
        return table;
    }
    /**
     *  添加一条记录到 Hbase 表
     *  @paramtableNameHbase 表名 tableName
     *  @paramrowkeyHbase 表的 rowkey
     *  @paramcfHbase 表的 columnfamily
     *  @paramcolumnHbase 表的列 column
     *  @paramvalue 写入 Hbase 表的值
     */
    public void put(String tableName, String rowkey , String columnfamily ,String column ,String click_value){
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(click_value));
        try{
            table.put(put);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        String tableName = "category_click";
        String rowkey = "20201111_88";
        String columnfamily = "columnfamily_info";
        String column = "click_count";
        String click_value = "2";

        HBaseUtil.getInstance().put(tableName,rowkey,columnfamily,column,click_value);
    }

}
