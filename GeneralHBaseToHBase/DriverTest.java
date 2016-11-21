package GeneralHBaseToHBase;

import org.apache.hadoop.util.ToolRunner;



public class DriverTest {

	public static void main(String[] args) throws Exception {
		// 无版本设置、无列导入设置，无列导出设置
		String[] myArgs1= new String[]{
				"test1", // 输入表
				"test2", // 输出表
				"0",     // 版本大小数，如果值为0，则为默认从输入表导出最新的数据到输出表
				"-1",    // 列导入设置，如果为-1 ，则没有设置列导入
				"-1"	// 列导出设置，如果为-1，则没有设置列导出
			};
			
		ToolRunner.run(HBaseDriver.getConfiguration(), 
				new HBaseDriver(),
				myArgs1);
		
		// 无版本设置、有列导入设置，无列导出设置
		String[] myArgs2= new String[]{
				"test1",
				"test3",
				"0",
				"cf1:c1,cf1:c2,cf1:c10,cf1:c11,cf1:c14",
				"-1"
			};
			
		ToolRunner.run(HBaseDriver.getConfiguration(), 
				new HBaseDriver(),
				myArgs2);
		
		// 无版本设置，无列导入设置，有列导出设置
		String[] myArgs3= new String[]{
				"test1",
				"test4",
				"0",
				"-1",
				"cf1:c1,cf1:c10,cf1:c14"
			};
			
		ToolRunner.run(HBaseDriver.getConfiguration(), 
				new HBaseDriver(),
				myArgs3);
		// 有版本设置，无列导入设置，无列导出设置
		String[] myArgs4= new String[]{
				"test1",
				"test5",
				"2",
				"-1",
				"-1"
			};
			
		ToolRunner.run(HBaseDriver.getConfiguration(), 
				new HBaseDriver(),
				myArgs4);
		
//		 有版本设置、有列导入设置，无列导出设置
		String[] myArgs5= new String[]{
				"test1",
				"test6",
				"2",
				"cf1:c1,cf1:c2,cf1:c10,cf1:c11,cf1:c14",
				"-1"
			};
			
		ToolRunner.run(HBaseDriver.getConfiguration(), 
				new HBaseDriver(),
				myArgs5);
		
		// 有版本设置、无列导入设置，有列导出设置
		String[] myArgs6= new String[]{
				"test1",
				"test7",
				"2",
				"-1",
				"cf1:c1,cf1:c10,cf1:c14"
			};
			
		ToolRunner.run(HBaseDriver.getConfiguration(), 
				new HBaseDriver(),
				myArgs6);
		// 有版本设置、有列导入设置，有列导出设置
		String[] myArgs7= new String[]{
				"test1",
				"test8",
				"2",
				"cf1:c1,cf1:c2,cf1:c10,cf1:c11,cf1:c14",
				"cf1:c1,cf1:c10,cf1:c14"
			};
			
		ToolRunner.run(HBaseDriver.getConfiguration(), 
				new HBaseDriver(),
				myArgs7);
	}

}
