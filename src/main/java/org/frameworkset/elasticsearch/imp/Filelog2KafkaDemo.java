package org.frameworkset.elasticsearch.imp;
/**
 * Copyright 2008 biaoping.yin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.kafka.clients.producer.RecordMetadata;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileImportConfig;
import org.frameworkset.tran.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.kafka.output.filelog.FileLog2KafkaImportBuilder;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.ReocordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: 采集日志文件数据并发送kafka作业，如需调试同步功能，直接运行main方法</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Filelog2KafkaDemo {
	private static Logger logger = LoggerFactory.getLogger(Filelog2KafkaDemo.class);
	public static void main(String args[]){

		Filelog2KafkaDemo dbdemo = new Filelog2KafkaDemo();

		dbdemo.scheduleTimestampImportData();
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(){
		FileLog2KafkaImportBuilder importBuilder = new FileLog2KafkaImportBuilder();
		importBuilder.setFetchSize(300);
		//kafka相关配置参数
		/**
		 *
		 <property name="productorPropes">
		 <propes>

		 <property name="value.serializer" value="org.apache.kafka.common.serialization.StringSerializer">
		 <description> <![CDATA[ 指定序列化处理类，默认为kafka.serializer.DefaultEncoder,即byte[] ]]></description>
		 </property>
		 <property name="key.serializer" value="org.apache.kafka.common.serialization.LongSerializer">
		 <description> <![CDATA[ 指定序列化处理类，默认为kafka.serializer.DefaultEncoder,即byte[] ]]></description>
		 </property>

		 <property name="compression.type" value="gzip">
		 <description> <![CDATA[ 是否压缩，默认0表示不压缩，1表示用gzip压缩，2表示用snappy压缩。压缩后消息中会有头来指明消息压缩类型，故在消费者端消息解压是透明的无需指定]]></description>
		 </property>
		 <property name="bootstrap.servers" value="192.168.137.133:9093">
		 <description> <![CDATA[ 指定kafka节点列表，用于获取metadata(元数据)，不必全部指定]]></description>
		 </property>
		 <property name="batch.size" value="10000">
		 <description> <![CDATA[ 批处理消息大小：
		 the producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. This helps performance on both the client and the server. This configuration controls the default batch size in bytes.
		 No attempt will be made to batch records larger than this size.

		 Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent.

		 A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a buffer of the specified batch size in anticipation of additional records.
		 ]]></description>
		 </property>

		 <property name="linger.ms" value="10000">
		 <description> <![CDATA[
		 <p>
		 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
		 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
		 * generally have one of these buffers for each active partition).
		 * <p>
		 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
		 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
		 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
		 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
		 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
		 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
		 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
		 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
		 * efficient requests when not under maximal load at the cost of a small amount of latency.
		 * <p>
		 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
		 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
		 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
		 * a TimeoutException.
		 * <p>]]></description>
		 </property>
		 <property name="buffer.memory" value="10000">
		 <description> <![CDATA[ 批处理消息大小：
		 The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
		 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
		 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
		 * a TimeoutException.]]></description>
		 </property>

		 </propes>
		 </property>
		 */

		// kafka服务器参数配置
		// kafka 2x 客户端参数项及说明类：org.apache.kafka.clients.consumer.ConsumerConfig
		KafkaOutputConfig kafkaOutputConfig = new KafkaOutputConfig();
		kafkaOutputConfig.setTopic("es2kafka");//设置kafka主题名称
		kafkaOutputConfig.addKafkaProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		kafkaOutputConfig.addKafkaProperty("key.serializer","org.apache.kafka.common.serialization.LongSerializer");
		kafkaOutputConfig.addKafkaProperty("compression.type","gzip");
		kafkaOutputConfig.addKafkaProperty("bootstrap.servers","192.168.137.133:9092");
		kafkaOutputConfig.addKafkaProperty("batch.size","10");
//		kafkaOutputConfig.addKafkaProperty("linger.ms","10000");
//		kafkaOutputConfig.addKafkaProperty("buffer.memory","10000");
		kafkaOutputConfig.setKafkaAsynSend(true);
//指定文件中每条记录格式，不指定默认为json格式输出
		kafkaOutputConfig.setReocordGenerator(new ReocordGenerator() {
			@Override
			public void buildRecord(Context taskContext, CommonRecord record, Writer builder) {
				//record.setRecordKey("xxxxxx"); //指定记录key
				//直接将记录按照json格式输出到文本文件中
				SerialUtil.normalObject2json(record.getDatas(),//获取记录中的字段数据并转换为json格式
						builder);
//          System.out.println(data);

			}
		});
		importBuilder.setKafkaOutputConfig(kafkaOutputConfig);
		//定时任务配置结束

		FileImportConfig config = new FileImportConfig();
		//.*.txt.[0-9]+$
		//[17:21:32:388]
//		config.addConfig(new FileConfig("D:\\ecslog",//指定目录
//				"error-2021-03-27-1.log",//指定文件名称，可以是正则表达式
//				"^\\[[0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
//				.setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
////				.setMaxBytes(1048576)//控制每条日志的最大长度，超过长度将被截取掉
//				//.setStartPointer(1000l)//设置采集的起始位置，日志内容偏移量
//				.addField("tag","error") //添加字段tag到记录中
//				.setExcludeLines(new String[]{"\\[DEBUG\\]"}));//不采集debug日志

		config.addConfig(new FileConfig("D:\\workspace\\bbossesdemo\\filelog-elasticsearch\\",//指定目录
						"es.log",//指定文件名称，可以是正则表达式
						"^\\[[0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
						.setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
						.addField("tag","elasticsearch")//添加字段tag到记录中
				//.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
		);
//		config.addConfig("E:\\ELK\\data\\data3",".*.txt","^[0-9]{4}-[0-9]{2}-[0-9]{2}");
		/**
		 * 启用元数据信息到记录中，元数据信息以map结构方式作为@filemeta字段值添加到记录中，文件插件支持的元信息字段如下：
		 * hostIp：主机ip
		 * hostName：主机名称
		 * filePath： 文件路径
		 * timestamp：采集的时间戳
		 * pointer：记录对应的截止文件指针,long类型
		 * fileId：linux文件号，windows系统对应文件路径
		 * 例如：
		 * {
		 *   "_index": "filelog",
		 *   "_type": "_doc",
		 *   "_id": "HKErgXgBivowv_nD0Jhn",
		 *   "_version": 1,
		 *   "_score": null,
		 *   "_source": {
		 *     "title": "解放",
		 *     "subtitle": "小康",
		 *     "ipinfo": "",
		 *     "newcollecttime": "2021-03-30T03:27:04.546Z",
		 *     "author": "张无忌",
		 *     "@filemeta": {
		 *       "path": "D:\\ecslog\\error-2021-03-27-1.log",
		 *       "hostname": "",
		 *       "pointer": 3342583,
		 *       "hostip": "",
		 *       "timestamp": 1617074824542,
		 *       "fileId": "D:/ecslog/error-2021-03-27-1.log"
		 *     },
		 *     "@message": "[18:04:40:161] [INFO] - org.frameworkset.tran.schedule.ScheduleService.externalTimeSchedule(ScheduleService.java:192) - Execute schedule job Take 3 ms"
		 *   }
		 * }
		 *
		 * true 开启 false 关闭
		 */
		config.setEnableMeta(true);
		importBuilder.setFileImportConfig(config);

		importBuilder.setFromFirst(false);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("filelog2kafka");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//指定增量同步的起始时间
//		importBuilder.setLastValue(new Date());
		//增量配置结束

		//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
		importBuilder.addFieldValue("author","张无忌");
//		importBuilder.addFieldMapping("operModule","OPER_MODULE");
//		importBuilder.addFieldMapping("logContent","LOG_CONTENT");
//		importBuilder.addFieldMapping("logOperuser","LOG_OPERUSER");

		//设置ip地址信息库地址
		importBuilder.setGeoipDatabase("E:/workspace/hnai/terminal/geolite2/GeoLite2-City.mmdb");
		importBuilder.setGeoipAsnDatabase("E:/workspace/hnai/terminal/geolite2/GeoLite2-ASN.mmdb");
		importBuilder.setGeoip2regionDatabase("E:/workspace/hnai/terminal/geolite2/ip2region.db");
		/**
		 * 重新设置es数据结构
		 */
		/**
		 * 重新设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
//				System.out.println(data);

//				context.addFieldValue("author","duoduo");//将会覆盖全局设置的author变量
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");

				//如果日志是普通的文本日志，非json格式，则可以自己根据规则对包含日志记录内容的message字段进行解析
				String message = context.getStringValue("@message");
				String[] fvs = message.split(" ");//空格解析字段
				/**
				 * //解析示意代码
				 * String[] fvs = message.split(" ");//空格解析字段
				 * //将解析后的信息添加到记录中
				 * context.addFieldValue("f1",fvs[0]);
				 * context.addFieldValue("f2",fvs[1]);
				 * context.addFieldValue("logVisitorial",fvs[2]);//包含ip信息
				 */
				//直接获取文件元信息
				Map fileMata = (Map)context.getValue("@filemeta");
				/**
				 * 文件插件支持的元信息字段如下：
				 * hostIp：主机ip
				 * hostName：主机名称
				 * filePath： 文件路径
				 * timestamp：采集的时间戳
				 * pointer：记录对应的截止文件指针,long类型
				 * fileId：linux文件号，windows系统对应文件路径
				 */
				String filePath = (String)context.getMetaValue("filePath");



//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
				/**
				 * 获取ip对应的运营商和区域信息
				 */
				/**
				 IpInfo ipInfo = (IpInfo) context.getIpInfo(fvs[2]);
				 if(ipInfo != null)
				 context.addFieldValue("ipinfo", ipInfo);
				 else{
				 context.addFieldValue("ipinfo", "");
				 }*/
				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
//				Date optime = context.getDateValue("LOG_OPERTIME",dateFormat);
//				context.addFieldValue("logOpertime",optime);
				context.addFieldValue("newcollecttime",new Date());

				/**
				 //关联查询数据,单值查询
				 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from head where billid = ? and othercondition= ?",
				 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
				 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("headdata",headdata);
				 //关联查询数据,多值查询
				 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
				 "select * from facedata where billid = ?",
				 context.getIntegerValue("billid"));
				 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("facedatas",facedatas);
				 */
			}
		});
		//映射和转换配置结束
		importBuilder.setExportResultHandler(new ExportResultHandler<Object, RecordMetadata>() {
			@Override
			public void success(TaskCommand<Object,RecordMetadata> taskCommand, RecordMetadata result) {
				TaskMetrics taskMetric = taskCommand.getTaskMetrics();
				logger.debug("处理耗时："+taskCommand.getElapsed() +"毫秒");
				logger.debug(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void error(TaskCommand<Object,RecordMetadata> taskCommand, RecordMetadata result) {
				logger.warn(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void exception(TaskCommand<Object,RecordMetadata> taskCommand, Exception exception) {
				logger.warn(taskCommand.getTaskMetrics().toString(),exception);
			}

			@Override
			public int getMaxRetry() {
				return 0;
			}
		});

		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setPrintTaskLog(true);

		/**
		 * 构建和启动导出elasticsearch数据并发送kafka同步作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();

	}

}
