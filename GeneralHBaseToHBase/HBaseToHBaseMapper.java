package GeneralHBaseToHBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class HBaseToHBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
	Logger log = LoggerFactory.getLogger(HBaseToHBaseMapper.class);
	private static int versionNum = 0;
	private static String[] columnFromTable = null;
	private static String[] columnToTable = null;
	private static String column1 = null;
	private static String column2 = null;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		versionNum = Integer.parseInt(conf.get("SETVERSION", "0"));
		column1 = conf.get("COLUMNFROMTABLE",null);
		if(!(column1 == null)){
			columnFromTable = column1.split(",");
		}
		column2 = conf.get("COLUMNTOTABLE",null); 
		if(!(column2 == null)){
			columnToTable = column2.split(",");
		}
	}
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {
		context.write(key, resultToPut(key,value));
	}	
	/***
	 * 把key，value转换为Put
	 * @param key
	 * @param value
	 * @return
	 * @throws IOException
	 */
	private Put resultToPut(ImmutableBytesWritable key, Result value) throws IOException {
		HashMap<String, String> fTableMap = new HashMap<>();
		HashMap<String, String> tTableMap = new HashMap<>();
		Put put = new Put(key.get());
		if(! (columnFromTable == null || columnFromTable.length == 0)){
			fTableMap = getFamilyAndColumn(columnFromTable);
		}
		if(! (columnToTable == null || columnToTable.length == 0)){
			tTableMap = getFamilyAndColumn(columnToTable);
		}
		if(versionNum==0){                        
			if(fTableMap.size() == 0){            
				if(tTableMap.size() == 0){		  
					for (Cell kv : value.rawCells()) {
						put.add(kv);	// 没有设置版本，没有设置列导入，没有设置列导出
					}
					return put;
				} else{
					return getPut(put, value, tTableMap); // 无版本、无列导入、有列导出
				}
			} else {
				if(tTableMap.size() == 0){
					return getPut(put, value, fTableMap);// 无版本、有列导入、无列导出
				} else {
					return getPut(put, value, tTableMap);// 无版本、有列导入、有列导出
				}
			}
		} else{
			if(fTableMap.size() == 0){
				if(tTableMap.size() == 0){
					return getPut1(put, value); // 有版本，无列导入，无列导出
				}else{
					return getPut2(put, value, tTableMap); //有版本，无列导入，有列导出
				}
			}else{
				if(tTableMap.size() == 0){
					return getPut2(put,value,fTableMap);// 有版本，有列导入，无列导出
				}else{
					return getPut2(put,value,tTableMap); // 有版本，有列导入，有列导出
				}
			}
		}
	}
	/***
	 * 无版本设置的情况下，对于有列导入或者列导出
	 * @param put
	 * @param value
	 * @param tableMap
	 * @return
	 * @throws IOException
	 */

	private Put getPut(Put put,Result value,HashMap<String, String> tableMap) throws IOException{
		for(Cell kv : value.rawCells()){
			byte[] family = kv.getFamily();
			if(tableMap.containsKey(new String(family))){
				String columnStr = tableMap.get(new String(family));
				ArrayList<String> columnBy = toByte(columnStr);
				if(columnBy.contains(new String(kv.getQualifier()))){
					put.add(kv); //没有设置版本，没有设置列导入，有设置列导出
				}
			}
		}
		return put;
	}
	/***
	 * (有版本，无列导入，有列导出)或者(有版本，有列导入，无列导出)
	 * @param put
	 * @param value
	 * @param tTableMap
	 * @return
	 */
	private Put getPut2(Put put,Result value,HashMap<String, String> tableMap){
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map=value.getMap();
        for(byte[] family:map.keySet()){
        	if(tableMap.containsKey(new String(family))){
        		String columnStr = tableMap.get(new String(family));
        		log.info("@@@@@@@@@@@"+new String(family)+"   "+columnStr);
				ArrayList<String> columnBy = toByte(columnStr);
        		NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据
                for(byte[] column:familyMap.keySet()){                              //根据列名循坏
                    log.info("!!!!!!!!!!!"+new String(column));
                	if(columnBy.contains(new String(column))){
	                	NavigableMap<Long, byte[]> valuesMap = familyMap.get(column);
	                    for(Entry<Long, byte[]> s:valuesMap.entrySet()){//获取列对应的不同版本数据，默认最新的一个
	                    	System.out.println("***:"+new String(family)+"  "+new String(column)+"  "+s.getKey()+"  "+new String(s.getValue()));
	                    	put.addColumn(family, column, s.getKey(),s.getValue());
	                    }
                    }
                }
        	}
            
        }
		return put;		
	}
	/***
	 * 有版本、无列导入、无列导出
	 * @param put
	 * @param value
	 * @return
	 */
	private Put getPut1(Put put,Result value){
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map=value.getMap();
        for(byte[] family:map.keySet()){    
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);//列簇作为key获取其中的列相关数据
            for(byte[] column:familyMap.keySet()){                              //根据列名循坏
                NavigableMap<Long, byte[]> valuesMap = familyMap.get(column);
                for(Entry<Long, byte[]> s:valuesMap.entrySet()){                //获取列对应的不同版本数据，默认最新的一个
                	put.addColumn(family, column, s.getKey(),s.getValue());
                }
            }
        }
        return put;
	}
	// str => {"cf1:c1","cf1:c2","cf1:c10","cf1:c11","cf1:c14"}
	/***
	 * 得到列簇名与列名的k,v形式的map
	 * @param str => {"cf1:c1","cf1:c2","cf1:c10","cf1:c11","cf1:c14"}
	 * @return map => {"cf1" => "c1,c2,c10,c11,c14"}
	 */
	private static HashMap<String, String> getFamilyAndColumn(String[] str){
		HashMap<String, String> map = new HashMap<>();
		HashSet<String> set = new HashSet<>();
		for(String s : str){
			set.add(s.split(":")[0]);
		}
		Object[] ob = set.toArray();
		for(int i=0; i<ob.length;i++){
			String family = String.valueOf(ob[i]);
			String columns = "";
			for(int j=0;j < str.length;j++){
				if(family.equals(str[j].split(":")[0])){
					columns += str[j].split(":")[1]+",";
				}
			}
			map.put(family, columns.substring(0, columns.length()-1));
		}
		return map;		
	}
	
	private static ArrayList<String> toByte(String s){
		ArrayList<String> b = new ArrayList<>();
		String[] sarr = s.split(",");
		for(int i=0;i<sarr.length;i++){
			b.add(sarr[i]);
		}
		return b;
	}
}
