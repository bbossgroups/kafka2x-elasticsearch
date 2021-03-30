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
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.IndexField;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.kafka.output.es.ES2KafkaExportBuilder;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.ReocordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * <p>Description: 导出elasticsearch数据并发送kafka同步作业，如需调试同步功能，直接运行main方法</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2KafkaDemo {
	private static Logger logger = LoggerFactory.getLogger(ES2KafkaDemo.class);
	public static void main(String args[]){
		ClientInterface clientInterface = ElasticSearchHelper.getRestClientUtil();
		boolean uper7 = clientInterface.isVersionUpper7();
		if(uper7) {
			List<IndexField> indexFields = clientInterface.getIndexMappingFields("dbdemo");
		}
		else{
			List<IndexField> indexFields = clientInterface.getIndexMappingFields("dbdemo","typename");
		}
		ES2KafkaDemo dbdemo = new ES2KafkaDemo();

		dbdemo.scheduleTimestampImportData();
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(){
		ES2KafkaExportBuilder importBuilder = new ES2KafkaExportBuilder();
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
				String data = (String)taskContext.getTaskContext().getTaskData("data");//从任务上下文中获取本次任务执行前设置时间戳
//          System.out.println(data);

			}
		});
		importBuilder.setKafkaOutputConfig(kafkaOutputConfig);
		importBuilder.setIncreamentEndOffset(300);//单位秒，同步从上次同步截止时间当前时间前5分钟的数据，下次继续从上次截止时间开始同步数据
		//vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27
		importBuilder
				.setDsl2ndSqlFile("dsl2ndSqlFile.xml")
				.setDslName("scrollQuery")
				.setScrollLiveTime("10m")
//				.setSliceQuery(true)
//				.setSliceSize(5)
				.setQueryUrl("dbdemo/_search")
				//通过简单的示例，演示根据实间范围计算queryUrl,以当前时间为截止时间，后续版本6.2.8将增加lastEndtime参数作为截止时间（在设置了IncreamentEndOffset情况下有值）
//				.setQueryUrlFunction((TaskContext taskContext, Date lastStartTime, Date lastEndTime)->{
//					String formate = "yyyy.MM.dd";
//					SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
//					String startTime = dateFormat.format(lastEndTime);
//					Date endTime = new Date();
//					String endTimeStr = dateFormat.format(endTime);
//					return "dbdemo-"+startTime+ ",dbdemo-"+endTimeStr+"/_search";
////					return "vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27/_search";
//				})

//				//添加dsl中需要用到的参数及参数值
				.addParam("var1","v1")
				.addParam("var2","v2")
				.addParam("var3","v3");
		importBuilder.setSourceElasticsearch("default");

		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(30000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束

		//设置任务执行拦截器，可以添加多个
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {

				String formate = "yyyyMMddHHmmss";
				//HN_BOSS_TRADE00001_YYYYMMDDHHMM_000001.txt
				SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
				String time = dateFormat.format(new Date());
				//可以在preCall方法中设置任务级别全局变量，然后在其他任务级别和记录级别接口中通过taskContext.getTaskData("time");方法获取time参数
				taskContext.addTaskData("time",time);

			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall 1");
			}

			@Override
			public void throwException(TaskContext taskContext, Exception e) {
				System.out.println("throwException 1");
			}
		});
//		//设置任务执行拦截器结束，可以添加多个
		//增量配置开始
		importBuilder.setLastValueColumn("collecttime");//手动指定日期增量查询字段变量名称
		importBuilder.setFromFirst(false);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("es2kafka");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
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
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
				String data = (String)context.getTaskContext().getTaskData("data");
//				System.out.println(data);

//				context.addFieldValue("author","duoduo");//将会覆盖全局设置的author变量
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");

//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
				/**
				 * 获取ip对应的运营商和区域信息
				 */
				/**
				 * 获取ip对应的运营商和区域信息
				 */
				IpInfo ipInfo = (IpInfo) context.getIpInfo("logVisitorial");
				if(ipInfo != null)
					context.addFieldValue("ipinfo", ipInfo);
				else{
					context.addFieldValue("ipinfo", "");
				}
//				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
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
				System.out.println("处理耗时："+taskCommand.getElapsed() +"毫秒");
				System.out.println(taskCommand.getTaskMetrics());
			}

			@Override
			public void error(TaskCommand<Object,RecordMetadata> taskCommand, RecordMetadata result) {
				System.out.println(taskCommand.getTaskMetrics());
			}

			@Override
			public void exception(TaskCommand<Object,RecordMetadata> taskCommand, Exception exception) {
				System.out.println(taskCommand.getTaskMetrics());
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
