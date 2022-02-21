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


import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.runtime.CommonLauncher;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.kafka.KafkaImportConfig;
import org.frameworkset.tran.kafka.input.fileftp.Kafka2FileFtpExportBuilder;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.output.fileftp.FileOupputConfig;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;

/**
 * <p>Description: 采集日志文件数据并发送kafka作业，如需调试同步功能，直接运行main方法</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2FileFtpDemo {
	private static Logger logger = LoggerFactory.getLogger(Kafka2FileFtpDemo.class);
	public static void main(String args[]){

		Kafka2FileFtpDemo dbdemo = new Kafka2FileFtpDemo();

		dbdemo.scheduleTimestampImportData();
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(){
		Kafka2FileFtpExportBuilder importBuilder = new Kafka2FileFtpExportBuilder();
		importBuilder.setBatchSize(500).setFetchSize(1000);


		String ftpIp = CommonLauncher.getProperty("ftpIP","10.13.6.127");//同时指定了默认值
		FileOupputConfig fileFtpOupputConfig = new FileOupputConfig();
		FtpOutConfig ftpOutConfig = new FtpOutConfig();
		ftpOutConfig.setBackupSuccessFiles(true);
		ftpOutConfig.setTransferEmptyFiles(true);
		ftpOutConfig.setFtpIP(ftpIp);

		ftpOutConfig.setFtpPort(5322);
//		ftpOutConfig.addHostKeyVerifier("2a:da:5a:6a:cf:7d:65:e5:ac:ff:d3:73:7f:2c:55:c9");
		ftpOutConfig.setFtpUser("ecs");
		ftpOutConfig.setFtpPassword("ecs@123");
		ftpOutConfig.setRemoteFileDir("/home/ecs/failLog");
		ftpOutConfig.setKeepAliveTimeout(100000);
		ftpOutConfig.setFailedFileResendInterval(100000);

		fileFtpOupputConfig.setFtpOutConfig(ftpOutConfig);
		fileFtpOupputConfig.setFileDir("D:\\workdir");
		fileFtpOupputConfig.setMaxFileRecordSize(10);
		fileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {
			@Override
			public String genName(TaskContext taskContext, int fileSeq) {


				return "HN_BOSS_TRADE_"+fileSeq + ".txt";
			}
		});
		fileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(Context taskContext, CommonRecord record, Writer builder) {
				SerialUtil.normalObject2json(record.getDatas(),builder);

			}
		});
		importBuilder.setFileOupputConfig(fileFtpOupputConfig);
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

		//bin/kafka-console-producer.sh --broker-list 10.13.11.12:9092 --topic xinkonglog
		/**
		 * 发送测试数据
		 {"collecttime":1588864468000,"optime":1526747614000,"author":"duoduo","subtitle":"小康","name":"认证管理","oper":"admin","id":269389,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"admin(系统管理员) 退出[公共开发平台]"}
		 {"collecttime":1588864468000,"optime":1523458966000,"author":"duoduo","subtitle":"小康","name":"认证管理","oper":"admin","id":269390,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"admin(系统管理员) 登陆[内容管理系统平台]"}
		 {"collecttime":1588864468000,"optime":1522163574000,"author":"duoduo","subtitle":"小康","name":"认证管理","oper":"admin","id":269391,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"admin(系统管理员) 退出[公共开发平台]"}
		 {"collecttime":1588864468000,"optime":1480349157000,"author":"duoduo","subtitle":"小康","name":"机构管理","oper":"admin","id":269392,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"系统管理员新增子机构3eqr"}
		 {"collecttime":1588864467000,"optime":1520869926000,"author":"duoduo","subtitle":"小康","name":"认证管理","oper":"admin","id":269393,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"admin(系统管理员) 登陆[内容管理系统平台]"}
		 {"collecttime":1588864468000,"optime":1520860610000,"author":"duoduo","subtitle":"小康","name":"认证管理","oper":"admin","id":269394,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"admin(系统管理员) 退出[公共开发平台]"}
		 {"collecttime":1588864468000,"optime":1572536935000,"author":"duoduo","subtitle":"小康","name":"认证-管理","oper":"|admin","id":269395,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"admin(系统管理员) 登陆[公共开发平台]"}
		 {"collecttime":1588864467000,"optime":1520869054000,"author":"duoduo","subtitle":"小康","name":"站点管理","oper":"admin","id":269396,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"创建站点.站点名:test"}
		 {"collecttime":1588864468000,"optime":1478092711000,"author":"duoduo","subtitle":"小康","name":"认证管理","oper":"admin","id":269397,"title":"解放","ipinfo":"{\"country\":\"中国\",\"countryId\":\"CN\",\"area\":\"\",\"areaId\":\"\",\"region\":\"浙江省\",\"regionId\":\"ZJ\",\"city\":\"杭州\",\"cityId\":\"\",\"county\":\"浙江省\",\"countyId\":\"ZJ\",\"isp\":\"Chinanet\",\"ispId\":4134,\"ip\":\"115.204.150.34\",\"geoPoint\":{\"lon\":120.1619,\"lat\":30.294}}","content":"admin(系统管理员) 登陆[公共开发平台]"}

		 */
		// kafka服务器参数配置
		// kafka 2x 客户端参数项及说明类：org.apache.kafka.clients.consumer.ConsumerConfig
		importBuilder//.addKafkaConfig("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
				//.addKafkaConfig("key.deserializer","org.apache.kafka.common.serialization.LongDeserializer")
				.addKafkaConfig("group.id","trandbtest") // 消费组ID
				.addKafkaConfig("session.timeout.ms","30000")
				.addKafkaConfig("auto.commit.interval.ms","5000")
				.addKafkaConfig("auto.offset.reset","latest")
//				.addKafkaConfig("bootstrap.servers","192.168.137.133:9093")
				.addKafkaConfig("bootstrap.servers","10.13.11.12:9092")
				.addKafkaConfig("enable.auto.commit","true")
				.addKafkaConfig("max.poll.records","500") // The maximum number of records returned in a single call to poll().
				.setKafkaTopic("es2kafka") // kafka topic
				.setConsumerThreads(5) // 并行消费线程数，建议与topic partitions数一致
				.setKafkaWorkQueue(10)
				.setKafkaWorkThreads(2)
				.setCheckinterval(2000)   // 批量从kafka拉取数据，闲置时间间隔，如果在指定的时间间隔内，没有数据到达并且数据拉取队列中有数据，则强制将队列中的数据交给同步作业程序进行同步处理

				.setPollTimeOut(1000) // 从kafka consumer poll(timeout)参数
				.setValueCodec(KafkaImportConfig.CODEC_JSON)
				.setKeyCodec(KafkaImportConfig.CODEC_LONG);
		importBuilder.setFlushInterval(10000l);
		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("filelog2ftp");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
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
		importBuilder.setExportResultHandler(new ExportResultHandler<String,String>() {
			@Override
			public void success(TaskCommand<String,String> taskCommand, String result) {
				TaskMetrics taskMetric = taskCommand.getTaskMetrics();
				logger.debug("处理耗时："+taskCommand.getElapsed() +"毫秒");
				logger.debug(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void error(TaskCommand<String,String> taskCommand, String result) {
				logger.warn(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void exception(TaskCommand<String,String> taskCommand, Exception exception) {
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
