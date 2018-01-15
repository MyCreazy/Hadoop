package com.sl.WordSort;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 关键词分类
 * 
 * @author Administrator Jan 15, 2018
 */
public class WordSort {
	/**
	 * map函数
	 * 
	 * @author Administrator Jan 15, 2018
	 */
	public static class testMap extends Mapper<Object, Text, Text, IntWritable> {
		/**
		 * 分组根字符串
		 */
		private String sortRootStr = "服饰,汽车,母婴";

		/**
		 * 创建一个值为1的对象
		 */
		private final static IntWritable one = new IntWritable(1);

		/**
		 * 词语
		 */
		private Text wordtext = new Text();

		/**
		 * map函数
		 * 
		 * @throws InterruptedException
		 * @throws IOException
		 */
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = sortRootStr.split("\\,");
			if (str != null) {
				//// 由于存在汉字，这里需要处理一下编码格式
				value = transformTextToAssignFormat(value, "gbk");
				String tempvalue = value.toString();
				System.out.println("拿到内容:" + tempvalue);
				//// StringTokenizer默认以空格隔开
				StringTokenizer temptokenizer = new StringTokenizer(tempvalue, "\n");
				//// 处理每一行
				while (temptokenizer.hasMoreTokens()) {
					StringTokenizer templine = new StringTokenizer(temptokenizer.nextToken());
					for (int i = 0; i < 4; i++) {
						String tempfildStr = templine.nextToken();
						if (i == 3) {
							int countstr = str.length;
							for (int j = 0; j < countstr; j++) {
								String tempeachstr = str[j];
								if (tempfildStr.contains(tempeachstr)) {
									//// 这个就是需要的数据
									Text textkey = new Text(tempeachstr);
									context.write(textkey, one);
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * 继承实现Reducer
	 * 
	 * @author Administrator Jan 15, 2018
	 */
	public static class testReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * 实现reducer
		 * 
		 * @throws InterruptedException
		 * @throws IOException
		 */
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//// 每个key都会调用一下reducer，这里直接累加
			int sum = 0;
			for (IntWritable temp : values) {
				sum += temp.get();
			}

			//// 然后把累加的值写入
			IntWritable result = new IntWritable(sum);
			context.write(key, result);
		}
	}

	/**
	 * 删除目录及其下所有文件
	 * 
	 * @param dir
	 * @return
	 */
	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			// 递归删除目录中的子目录下
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}

		// 目录此时为空，可以删除
		return dir.delete();
	}

	/**
	 * 字符编码
	 * 
	 * @param text
	 * @param encoding
	 * @return
	 */
	public static Text transformTextToAssignFormat(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new Text(value);
	}

	/**
	 * main 函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//// hadoop是严格校验运行环境的，如果目录存在因为不清楚是否要覆盖还是要删除原来的目录，所以如果输出目录存在会报错的
		//// 判断输出目录是否存在，如果存在则删除
		File file = new File("F:\\phicommwork\\Sumupdocument\\bigdata\\Practice\\document\\output");
		if (file.exists()) {
			boolean delresult = deleteDir(file);
			System.out.println("删除目录成功。。。。");
		}

		Configuration conf = new Configuration();
		//// 实例化Job
		Job job = Job.getInstance(conf, "exercise");
		job.setJarByClass(WordSort.class);
		//// 设置reducer，combiner，mapper
		job.setMapperClass(testMap.class);
		job.setReducerClass(testReducer.class);
		//// 因为不是算平均值等，所以这里把combiner设置好
		job.setCombinerClass(testReducer.class);
		/// 设置输入输出的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//// 设置输入输出文件路径,这里设置为获取本地文件
		FileInputFormat.addInputPath(job, new Path("F:\\rr.txt"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\Sumupdocument\\bigdata\\Practice\\document\\output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("输出完成");
	}
}
