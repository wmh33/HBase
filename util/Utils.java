package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
	
	public static final String SPLITTER ="SPLITTER";
	public static final String K="K";
	public static final String CENTERPATH="center";
	
	
	// TODO ������Ҫ�޸ģ���Ҫ
	static String namenode="master";
	static String resourcenode="master";
	static String schedulernode="master";
	static String jobhistorynode="master";
	private static Configuration conf =null;
	private static FileSystem fs;
	
	/**
	 * ������õ�Configuration
	 * @return
	 */
	public static Configuration getConf() {
		if(conf==null){
			conf = new Configuration();
			conf.setBoolean("mapreduce.app-submission.cross-platform", true);// ����ʹ�ÿ�ƽ̨�ύ����
			conf.set("fs.defaultFS", "hdfs://"+namenode+":8020");// ָ��namenode
			conf.set("mapreduce.framework.name", "yarn"); // ָ��ʹ��yarn���
			conf.set("yarn.resourcemanager.address", resourcenode+":8032"); // ָ��resourcemanager
			conf.set("yarn.resourcemanager.scheduler.address", schedulernode+":8030");// ָ����Դ������
			conf.set("mapreduce.jobhistory.address", jobhistorynode+":10020");// ָ��historyserver
			conf.set("mapreduce.job.jar", JarUtil.jar(Utils.class));
		}
		return conf;
	}
	
	public static FileSystem getFs(){
		if(fs==null){
			try {
				fs = FileSystem.get(getConf());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fs;
	}
	
	public static boolean delete(String hdfsPath) throws IllegalArgumentException, IOException{
		return getFs().delete(new Path(hdfsPath), true);
	}
	public static boolean shouldRunNextIteration(String preCenter,String nextCenter,double delta,String splitter) throws IOException{
		FSDataInputStream is=Utils.getFs().open(new Path(preCenter));
		FSDataInputStream is2=Utils.getFs().open(new Path(nextCenter));
		BufferedReader br=new BufferedReader(new InputStreamReader(is));
		BufferedReader br2=new BufferedReader(new InputStreamReader(is2));
		String line="";
		String line2="";
		double error=0.0;
		while( (line=br.readLine())!=null){
		 line2=br2.readLine(); 
		 error +=calDistance(line,line2,splitter);
	}
		br.close();
		br2.close();
		is.close();
		is2.close();

		if(Math.sqrt(error)<delta){
			System.out.println("delta:"+Math.sqrt(error));
			return false;
		}
		
		return true;
	}
	
	public static double calDistance(String line, String string,String splitter) {
		double sum=0;
		String[] data=line.split(splitter);
		String[] centerI=string.split(splitter);
		for(int i=0;i<data.length;i++){
			sum+=Math.pow(Double.parseDouble(data[i])-Double.parseDouble(centerI[i]), 2);
		}
		return Math.sqrt(sum);
	}

//	public static void main(String[] args) throws IOException {
//		String pr="hdfs://master:8020/user/Administrator/Kmeans/k2_xu/part-r-00000";
//		String ne="hdfs://master:8020/user/Administrator/Kmeans/k3/part-r-00000";
//		double delta=0.5;
//		String splitter=",";
//		System.out.println(shouldRunNextIteration( pr,
//				ne,delta, splitter));
//		
//	}

}
