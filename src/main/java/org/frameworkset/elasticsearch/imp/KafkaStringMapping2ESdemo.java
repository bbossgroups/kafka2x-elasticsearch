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


import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.kafka.KafkaStringRecord;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.kafka.input.Kafka2InputConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.*;

/**
 * <p>Description: 同步处理程序，如需调试同步功能，直接运行main方法</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaStringMapping2ESdemo {
	private static Logger logger = LoggerFactory.getLogger(KafkaStringMapping2ESdemo.class);
	public static void main(String args[]){
		KafkaStringMapping2ESdemo dbdemo = new KafkaStringMapping2ESdemo();
		boolean dropIndice = true;//CommonLauncher.getBooleanAttribute("dropIndice",false);//同时指定了默认值

		dbdemo.scheduleTimestampImportData(dropIndice);
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(boolean dropIndice){
		ImportBuilder importBuilder = new ImportBuilder();
		//增量定时任务不要删表，但是可以通过删表来做初始化操作
		if(dropIndice) {
			try {
				//清除测试表,导入的时候回重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
				String repsonse = ElasticSearchHelper.getRestClientUtil().dropIndice("kafkademo");
				System.out.println(repsonse);
			} catch (Exception e) {
			}
		}




		//kafka相关配置参数
		/**
		 *
		 <property name="value.deserializer" value="org.apache.kafka.common.serialization.StringDeserializer">
		 <description> <![CDATA[ Deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.]]></description>
		 </property>
		 <property name="key.deserializer" value="org.apache.kafka.common.serialization.LongDeserializer">
		 <description> <![CDATA[ Deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.]]></description>
		 </property>
		 <property name="group.id" value="test">
		 <description> <![CDATA[ A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.]]></description>
		 </property>
		 <property name="session.timeout.ms" value="30000">
		 <description> <![CDATA[ The timeout used to detect client failures when using "
		 + "Kafka's group management facility. The client sends periodic heartbeats to indicate its liveness "
		 + "to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, "
		 + "then the broker will remove this client from the group and initiate a rebalance. Note that the value "
		 + "must be in the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code> "
		 + "and <code>group.max.session.timeout.ms</code>.]]></description>
		 </property>
		 <property name="auto.commit.interval.ms" value="1000">
		 <description> <![CDATA[ The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.]]></description>
		 </property>



		 <property name="auto.offset.reset" value="latest">
		 <description> <![CDATA[ What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: automatically reset the offset to the latest offset</li><li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>]]></description>
		 </property>
		 <property name="bootstrap.servers" value="192.168.137.133:9093">
		 <description> <![CDATA[ A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form "
		 + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to "
		 + "discover the full cluster membership (which may change dynamically), this list need not contain the full set of "
		 + "servers (you may want more than one, though, in case a server is down).]]></description>
		 </property>
		 <property name="enable.auto.commit" value="true">
		 <description> <![CDATA[If true the consumer's offset will be periodically committed in the background.]]></description>
		 </property>
		 */

		// kafka服务器参数配置
		// kafka 2x 客户端参数项及说明类：org.apache.kafka.clients.consumer.ConsumerConfig
		Kafka2InputConfig kafka2InputConfig = new Kafka2InputConfig();

		kafka2InputConfig//.addKafkaConfig("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
				//.addKafkaConfig("key.deserializer","org.apache.kafka.common.serialization.LongDeserializer")
				.addKafkaConfig("group.id","trantest") // 消费组ID
				.addKafkaConfig("session.timeout.ms","30000")
				.addKafkaConfig("auto.commit.interval.ms","5000")
				.addKafkaConfig("auto.offset.reset","latest")
//				.addKafkaConfig("bootstrap.servers","192.168.137.133:9093")
//				.addKafkaConfig("bootstrap.servers","127.0.0.1:9092")
				.addKafkaConfig("bootstrap.servers","192.168.137.133:9092")
				.addKafkaConfig("enable.auto.commit","true")
				.addKafkaConfig("max.poll.records","500") // The maximum number of records returned in a single call to poll().
//				.setKafkaTopic("xinkonglog") // kafka topic
				.setKafkaTopic("es2kafka") // kafka topic
				.setConsumerThreads(5) // 并行消费线程数，建议与topic partitions数一致
				.setKafkaWorkQueue(10)
				.setKafkaWorkThreads(2)

				.setPollTimeOut(1000) // 从kafka consumer poll(timeout)参数

		;
        kafka2InputConfig.setValueCodec(CODEC_TEXT_SPLIT);

        kafka2InputConfig.setFieldSplit("|");//指定消息字段分割符，按照分隔符将消息切割为字符串数组
//指定数组元素与字段名称的映射配置，
        kafka2InputConfig.addCellMapping(0, "logOperTime");
        kafka2InputConfig.addCellMapping(1, "operModule");
        kafka2InputConfig.addCellMapping(2, "logOperuser");

		importBuilder.setInputConfig(kafka2InputConfig);

//		importBuilder.addIgnoreFieldMapping("remark1");
//		importBuilder.setSql("select * from td_sm_log ");
		/**
		 * es相关配置
		 */
		ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
		elasticsearchOutputConfig
				.setIndex("kafkademo") //必填项，索引名称
				.setIndexType("kafkademo") //es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType
				.setRefreshOption("refresh");//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
		importBuilder.setOutputConfig(elasticsearchOutputConfig)
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(100) ; //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理
//				.setFetchSize(100); //按批从kafka拉取数据的大小，设置了max.poll.records就不要设施FetchSize
		//设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
		importBuilder.setFlushInterval(10000l);

//		/**
//		 * 重新设置es数据结构
//		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {

				 // 获取原始的Kafka记录
				 KafkaStringRecord record = (KafkaStringRecord) context.getCurrentRecord().getRecord();
				 if(record.getKey() == null)
				 	System.out.println("key is null!");
                 String message = (String)record.getData();
//                 String[] datas = message.split("|");//按指定消息字段分割符，将消息切割为字符串数组
//                //指定数组元素与字段名称的映射配置，
//                context.addFieldValue("logOperTime",datas[0]);
//                context.addFieldValue("operModule",datas[1]);
//                context.addFieldValue("logOperuser",datas[2]);
			}
		});
		//映射和转换配置结束

		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时，自动关闭线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
			@Override
			public void success(TaskCommand<String> taskCommand, String result) {
				TaskMetrics taskMetric = taskCommand.getTaskMetrics();
				System.out.println("处理耗时："+taskCommand.getElapsed() +"毫秒");
				System.out.println(taskCommand.getTaskMetrics());
			}

			@Override
			public void error(TaskCommand<String> taskCommand, String result) {
				System.out.println(taskCommand.getTaskMetrics());
			}

			@Override
			public void exception(TaskCommand<String> taskCommand, Throwable exception) {
				System.out.println(taskCommand.getTaskMetrics());
			}


		});
		/**
		 importBuilder.setEsIdGenerator(new EsIdGenerator() {
		 //如果指定EsIdGenerator，则根据下面的方法生成文档id，
		 // 否则根据setEsIdField方法设置的字段值作为文档id，
		 // 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id

		 @Override
		 public Object genId(Context context) throws Exception {
		 return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
		 }
		 });
		 */
		/**
		 * 构建DataStream，执行mongodb数据到es的同步操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行同步操作

	}

}
