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


import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.dummy.output.DummyOutputConfig;
import org.frameworkset.tran.plugin.kafka.input.Kafka2InputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_JSON;
import static org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig.CODEC_LONG;

/**
 * <p>Description: 同步处理程序，如需调试同步功能，直接运行main方法</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2Dummydemo {
	private static Logger logger = LoggerFactory.getLogger(Kafka2Dummydemo.class);
	public static void main(String args[]){
		Kafka2Dummydemo dbdemo = new Kafka2Dummydemo();
		boolean dropIndice = true;//CommonLauncher.getBooleanAttribute("dropIndice",false);//同时指定了默认值

		dbdemo.scheduleTimestampImportData(dropIndice);
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(boolean dropIndice){
		ImportBuilder importBuilder = new ImportBuilder();


		importBuilder.setPrintTaskLog(true); //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false



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

		//bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic xinkonglog
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
		Kafka2InputConfig kafka2InputConfig = new Kafka2InputConfig();
		kafka2InputConfig.addKafkaConfig("group.id","trandbtest") // 消费组ID
				.addKafkaConfig("session.timeout.ms","30000")
				.addKafkaConfig("auto.commit.interval.ms","5000")
				.addKafkaConfig("auto.offset.reset","latest")
//				.addKafkaConfig("bootstrap.servers","192.168.137.133:9093")
				.addKafkaConfig("bootstrap.servers","10.13.6.127:9092")
				.addKafkaConfig("enable.auto.commit","false")
				.addKafkaConfig("max.poll.records","500") // The maximum number of records returned in a single call to poll().
				.setKafkaTopic("db2kafka") // kafka topic
				.setConsumerThreads(5) // 并行消费线程数，建议与topic partitions数一致
				.setKafkaWorkQueue(10)
				.setKafkaWorkThreads(2)

				.setPollTimeOut(1000) // 从kafka consumer poll(timeout)参数
				.setValueCodec(CODEC_JSON)//"org.apache.kafka.common.serialization.ByteArrayDeserializer"
				.setKeyCodec(CODEC_LONG);//"org.apache.kafka.common.serialization.ByteArrayDeserializer"

		importBuilder.setInputConfig(kafka2InputConfig);
		DummyOutputConfig dummyOutputConfig = new DummyOutputConfig();
		dummyOutputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) throws Exception{
				SerialUtil.object2jsonDisableCloseAndFlush(record.getDatas(),builder);

			}
		}).setPrintRecord(true);
		importBuilder.setOutputConfig(dummyOutputConfig);

//				.setFetchSize(100); //按批从kafka拉取数据的大小，设置了max.poll.records就不要设施FetchSize
		//设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
		importBuilder.setFlushInterval(10000l);

//		//设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
//		importBuilder.addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Throwable e) {
//				System.out.println("throwException");
//			}
//		}).addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall 1");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall 1");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Throwable e) {
//				System.out.println("throwException 1");
//			}
//		});
//		//设置任务执行拦截器结束，可以添加多个

		//映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 *
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
//
//		/**
//		 * 重新设置es数据结构
//		 */
		final AtomicInteger s = new AtomicInteger(0);
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
//				if(s.incrementAndGet() % 3 == 2) {
//					context.markRecoredInsert();
//				}
//				else if(s.incrementAndGet() % 3 == 1){
//					context.markRecoredUpdate();
//				}
//				else{
//					context.markRecoredDelete();
//				}
				/**
				 String name =  context.getStringValue("name");
				 Integer num = SQLExecutor.queryObjectWithDBName(Integer.class,"firstds","select count(*) from batchtest1 where name = ?",name);//判断目标数据库表中是否已经存在name对应的记录
				 if(num == null || num == 0){
				 context.markRecoredInsert();//不存在，标记为新增
				 }
				 else{
				 context.markRecoredUpdate();//存在，标记为修改
				 context.addFieldValue("content","new ocntnent");//模拟调整修改content字段内容
				 }
				 //				context.markRecoredDelete(); //亦可以根据条件，将记录标记为删除
				 */

				context.addFieldValue("author","duoduo");
				context.addFieldValue("title","解放");
				context.addFieldValue("subtitle","小康");
				context.addFieldValue("collecttime",new Date());//
//				Object password_lifetime = context.getValue("password_lifetime");
//				if(password_lifetime == null){
//					context.addFieldValue("password_lifetime", 0);
//				}
//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
//				context.addIgnoreFieldMapping("subtitle");
				/**
				 * 获取ip对应的运营商和区域信息
				 */
//				IpInfo ipInfo = context.getIpInfo("Host");
//				if(ipInfo != null)
//					context.addFieldValue("ipinfo", SimpleStringUtil.object2json(ipInfo));
//				else{
//					context.addFieldValue("ipinfo", "");
//				}
//				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
//				Date optime = context.getDateValue("LOG_OPERTIME",dateFormat);
//				context.addFieldValue("logOpertime",optime);
				context.addFieldValue("collecttime",new Date());
				long optime = context.getLongValue("optime");
				context.addFieldValue("optime",new Date(optime));
				/**
				 //关联查询数据,单值查询
				 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,"test",
				 "select * from head where billid = ? and othercondition= ?",
				 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
				 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("headdata",headdata);
				 //关联查询数据,多值查询
				 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,"test",
				 "select * from facedata where billid = ?",
				 context.getIntegerValue("billid"));
				 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
				 context.addFieldValue("facedatas",facedatas);
				 */
			}
		});
		//映射和转换配置结束

		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

		importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
			@Override
			public void success(TaskCommand<String> taskCommand, String result) {
				logger.info(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void error(TaskCommand<String> taskCommand, String result) {
                logger.error(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void exception(TaskCommand<String> taskCommand, Throwable exception) {
                logger.error(taskCommand.getTaskMetrics().toString());
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
