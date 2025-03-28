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
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.input.file.*;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.plugin.kafka.output.Kafka2OutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;

/**
 * <p>Description: 采集日志文件数据并发送kafka作业，如需调试同步功能，直接运行main方法</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Filelog2MultiKafkaDemo {
	private static Logger logger = LoggerFactory.getLogger(Filelog2MultiKafkaDemo.class);
	public static void main(String args[]){

		Filelog2MultiKafkaDemo dbdemo = new Filelog2MultiKafkaDemo();

		dbdemo.scheduleTimestampImportData();
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(){
		ImportBuilder importBuilder = new ImportBuilder();
		importBuilder.setFetchSize(300);
        importBuilder.setBatchSize(500);
		importBuilder.setAsynFlushStatus(true);
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
		Kafka2OutputConfig kafkaOutputConfig = new Kafka2OutputConfig();
		kafkaOutputConfig.setTopic("file2kafka1");//设置kafka主题名称
		kafkaOutputConfig.addKafkaProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		kafkaOutputConfig.addKafkaProperty("key.serializer","org.apache.kafka.common.serialization.LongSerializer");
		kafkaOutputConfig.addKafkaProperty("compression.type","gzip");
		kafkaOutputConfig.addKafkaProperty("bootstrap.servers","172.24.176.18:9092");
		kafkaOutputConfig.addKafkaProperty("batch.size","10");
//		kafkaOutputConfig.addKafkaProperty("linger.ms","10000");
//		kafkaOutputConfig.addKafkaProperty("buffer.memory","268435456");
		kafkaOutputConfig.addKafkaProperty("max.block.ms","600000");
		kafkaOutputConfig.setKafkaAsynSend(true);
		importBuilder.setLogsendTaskMetric(1000l);//设置发送多少条消息后打印发送统计信息
//指定文件中每条记录格式，不指定默认为json格式输出
		kafkaOutputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) {
				//record.setRecordKey("xxxxxx"); //指定记录key
				//直接将记录按照json格式输出到文本文件中
				SerialUtil.object2jsonDisableCloseAndFlush(record.getDatas(),//获取记录中的字段数据并转换为json格式
						builder);
//          System.out.println(data);

			}
		});
		importBuilder.addOutputConfig(kafkaOutputConfig);

        ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
        elasticsearchOutputConfig
                .addTargetElasticsearch("elasticsearch.serverNames","default")
                .addElasticsearchProperty("default.elasticsearch.rest.hostNames","192.168.137.1:9200")
                .addElasticsearchProperty("default.elasticsearch.showTemplate","false")
                .addElasticsearchProperty("default.elasticUser","elastic")
                .addElasticsearchProperty("default.elasticPassword","changeme")
                .addElasticsearchProperty("default.elasticsearch.failAllContinue","true")
                .addElasticsearchProperty("default.http.timeoutSocket","60000")
                .addElasticsearchProperty("default.http.timeoutConnection","40000")
                .addElasticsearchProperty("default.http.connectionRequestTimeout","70000")
                .addElasticsearchProperty("default.http.maxTotal","200")
                .addElasticsearchProperty("default.http.defaultMaxPerRoute","100")
                .setIndex("filelogdemo")
                .setEsIdField("logId")//设置文档主键，不设置，则自动产生文档id
                .setDebugResponse(false)//设置是否将每次处理的reponse打印到日志文件中，默认false
                .setDiscardBulkResponse(false);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false
        importBuilder.addOutputConfig(elasticsearchOutputConfig);
		//定时任务配置结束

		FileInputConfig fileInputConfig = new FileInputConfig();
        fileInputConfig.setJsondata(true);
//		config.setCharsetEncode("GB2312");//全局字符集配置
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

//		config.addConfig(new FileConfig("D:\\workspace\\bbossesdemo\\filelog-elasticsearch\\",//指定目录
//						"es.log",//指定文件名称，可以是正则表达式
//						"^\\[[0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3}\\]")//指定多行记录的开头识别标记，正则表达式
//						.setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
//						.addField("tag","elasticsearch")//添加字段tag到记录中
//				//.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
//		);
		fileInputConfig.addConfig(new FileConfig()//指定多行记录的开头识别标记，正则表达式
                
						.setSourcePath("C:\\workdir\\text").setFileFilter(new FileFilter() {
							@Override
							public boolean accept(FilterFileInfo filterFileInfo, FileConfig fileConfig) {
								return filterFileInfo.getFileName().endsWith(".txt");
							}
						})//指定文件过滤器.setCloseEOF(false)//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
                        .setCloseOlderTime(5000L)
//						.setCharsetEncode("GB2312") //文件集级别配置
//				.setIncludeLines(new String[]{".*ERROR.*"})//采集包含ERROR的日志
				//.setExcludeLines(new String[]{".*endpoint.*"}))//采集不包含endpoint的日志
		);
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
		fileInputConfig.setEnableMeta(true);
		importBuilder.setInputConfig(fileInputConfig);

		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("filelog2kafka");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//指定增量同步的起始时间
//		importBuilder.setLastValue(new Date());
		//增量配置结束
 

		//设置ip地址信息库地址
        importBuilder.setGeoipDatabase("C:/workdir/geolite2/GeoLite2-City.mmdb");
        importBuilder.setGeoipAsnDatabase("C:/workdir/geolite2/GeoLite2-ASN.mmdb");
        importBuilder.setGeoip2regionDatabase("C:/workdir/geolite2/ip2region.db");
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				//文件开始被采集前调用
				FileTaskContext fileTaskContext = (FileTaskContext)taskContext;
				FileInfo fileInfo = fileTaskContext.getFileInfo();
				taskContext.addTaskData("fileInfo",fileInfo);
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				//文件采集完毕后执行，可以归档文件
				FileTaskContext fileTaskContext = (FileTaskContext)taskContext;
				FileInfo fileInfo = fileTaskContext.getFileInfo();
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {

			}
		});
		/**
		 * 重新设置es数据结构
		 */
		/**
		 * 重新设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
 
				FileInfo fileInfo = (FileInfo) context.getTaskContext().getTaskData("fileInfo");

//
			}
		});
		//数据异步同步通道缓存队列设置，默认为10
		importBuilder.setTranDataBufferQueue(5);
		//映射和转换配置结束
		importBuilder.setExportResultHandler(new ExportResultHandler() {
			@Override
			public void success(TaskCommand taskCommand, Object result) {
				TaskMetrics taskMetric = taskCommand.getTaskMetrics();
//				logger.debug("处理耗时："+taskCommand.getElapsed() +"毫秒");
//				logger.debug(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void error(TaskCommand  taskCommand, Object result) {
				logger.warn(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void exception(TaskCommand  taskCommand, Throwable exception) {
				logger.warn(taskCommand.getTaskMetrics().toString(),exception);
			}


		});

		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setPrintTaskLog(true);
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

		/**
		 * 构建和启动导出elasticsearch数据并发送kafka同步作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();
//		dataStream.destroy(true);
	}

}
