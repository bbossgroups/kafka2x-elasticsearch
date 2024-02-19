//package org.frameworkset.elasticsearch.imp;
//
//import com.alibaba.druid.pool.DruidDataSource;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.iflytek.qbfk.annotation.Extract;
//import com.kxdigit.cmcc.common.service.AbstractETLTask;
//import com.kxdigit.cmcc.plugin.kafka2kafka.bean.LlmLogInfo;
//import com.kxdigit.cmcc.plugin.kafka2kafka.bean.LubanOutputInfo;
//import com.kxdigit.cmcc.plugin.kafka2kafka.config.PluginConfig;
//import com.kxdigit.cmcc.plugin.kafka2kafka.constants.ConfigProperty;
//import com.kxdigit.cmcc.plugin.kafka2kafka.map.DataByModelMap;
//import com.kxdigit.cmcc.plugin.kafka2kafka.sink.LlmLogSink;
//import com.kxdigit.cmcc.plugin.kafka2kafka.utils.LubanUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.collections.MapUtils;
//import org.apache.commons.lang.StringUtils;
//import org.frameworkset.elasticsearch.serial.SerialUtil;
//import org.frameworkset.tran.CommonRecord;
//import org.frameworkset.tran.DataRefactor;
//import org.frameworkset.tran.config.ImportBuilder;
//import org.frameworkset.tran.config.InputConfig;
//import org.frameworkset.tran.config.OutputConfig;
//import org.frameworkset.tran.context.Context;
//import org.frameworkset.tran.plugin.kafka.input.Kafka2InputConfig;
//import org.frameworkset.tran.plugin.kafka.input.KafkaInputConfig;
//import org.frameworkset.tran.plugin.kafka.output.Kafka2OutputConfig;
//import org.frameworkset.tran.util.RecordGenerator;
//import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
//
//import java.io.Writer;
//import java.util.*;
//
//@Slf4j
//@Extract(bus = "extract")
//public class Kafka2Kafka extends AbstractETLTask {
//    private final KafkaProperties kafkaProperties;
//    private final PluginConfig pluginConfig;
//    private final LlmLogSink llmLogSink;
//    private final DruidDataSource ds;
//
//    public Kafka2Kafka(KafkaProperties kafkaProperties, PluginConfig pluginConfig) {
//        this.kafkaProperties = kafkaProperties;
//        this.pluginConfig = pluginConfig;
////        log.info("pluginConfig -----------> {}", pluginConfig);
//        PluginConfig.SinkConfig sink = pluginConfig.getSink();
////        log.info("sink -----------> {}", sink);
//        this.llmLogSink = new LlmLogSink(sink.getDriver(), sink.getUrl(), sink.getUser(), sink.getPassword());
//        this.ds = llmLogSink.open();
//    }
//
//    @Override
//    public InputConfig getInputConfig() {
//        String servers = StringUtils.join(kafkaProperties.getBootstrapServers(), ",");
//        PluginConfig.SourceConfig source = pluginConfig.getSource();
//        String groupId = StringUtils.defaultIfEmpty(source.getGroupId(), "kafka2kafka-group");
//        String topic = source.getConstantTopic();
//        return new Kafka2InputConfig()
//                //.addKafkaConfig("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//                //.addKafkaConfig("key.deserializer","org.apache.kafka.common.serialization.LongDeserializer")
//                .addKafkaConfig("group.id", groupId) // 消费组ID
//                .addKafkaConfig(ConfigProperty.SESSION_TIMEOUT_MS, this.getProperties(pluginConfig.getSource().getProperty(), ConfigProperty.SESSION_TIMEOUT_MS, "30000"))
//                .addKafkaConfig(ConfigProperty.AUTO_COMMIT_INTERVAL_MS, this.getProperties(pluginConfig.getSource().getProperty(), ConfigProperty.AUTO_COMMIT_INTERVAL_MS, "5000"))
//                .addKafkaConfig("bootstrap.servers", servers)
//                .addKafkaConfig("enable.auto.commit", "true")
//                .addKafkaConfig("auto.offset.reset", "latest")//设置从最新消费
//                // The maximum number of records returned in a single call to poll().
//                .addKafkaConfig(ConfigProperty.MAX_POLL_RECORDS, this.getProperties(source.getProperty(), ConfigProperty.MAX_POLL_RECORDS, "100"))
//                .setKafkaTopic(topic) // kafka topic
//                .setConsumerThreads(source.getPartition()) // 并行消费线程数，建议与topic partitions数一致
//                // 从kafka consumer poll(timeout)参数
//                .setPollTimeOut(Integer.parseInt(this.getProperties(source.getProperty(), ConfigProperty.POLL_TIMEOUT, "1000")))
//                .setValueCodec(KafkaInputConfig.CODEC_JSON);
//    }
//
//    @Override
//    public OutputConfig getOutputConfig() {
//        String servers = StringUtils.join(kafkaProperties.getBootstrapServers(), ",");
//        // kafka输出服务器参数配置
//        // kafka 2x 客户端参数项及说明类：org.apache.kafka.clients.consumer.ConsumerConfig
//        Kafka2OutputConfig kafkaOutputConfig = new Kafka2OutputConfig();
//        kafkaOutputConfig.addKafkaProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaOutputConfig.addKafkaProperty("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
//        kafkaOutputConfig.addKafkaProperty("compression.type", "gzip");
//        kafkaOutputConfig.addKafkaProperty("bootstrap.servers", servers);
//        kafkaOutputConfig.addKafkaProperty("batch.size", "10");
//        kafkaOutputConfig.addKafkaProperty("max.request.size", String.valueOf(pluginConfig.getSink().getMaxRequestSize()));
////		kafkaOutputConfig.addKafkaProperty("linger.ms","10000");
////		kafkaOutputConfig.addKafkaProperty("buffer.memory","10000");
//        kafkaOutputConfig.setKafkaAsynSend(true);
//        kafkaOutputConfig.setTopic(pluginConfig.getSink().getOtherTopic());
//        //指定文件中每条记录格式，不指定默认为json格式输出
//        kafkaOutputConfig.setRecordGenerator(new RecordGenerator() {
//            @Override
//            public void buildRecord(Context taskContext, CommonRecord record, Writer builder) {
//                //record.setRecordKey("xxxxxx"); //指定记录key
//                //直接将记录按照json格式输出到文本文件中
//              //如果不想有空值字段到消息中，可以在这里处理一下datas，把空值字段移除掉，然后再序列化就可以了  
//                Map<String, Object> datas = record.getDatas();
//                List<String> emptyKeys = new ArrayList<>();
//                Iterator<Map.Entry<String, Object>> iterator = datas.entrySet().iterator();
//                while (iterator.hasNext()){
//                    Map.Entry<String, Object> entry = iterator.next();
//                    if(entry.getValue() == null){
//                        emptyKeys.add(entry.getKey());
//                    }
//                }
//                for(String key:emptyKeys){
//                    datas.remove(key);
//                }
//                        
//                
//                SerialUtil.normalObject2json(datas, builder);//获取记录中的字段数据并转换为json格式
//            }
//        });
//
//        return kafkaOutputConfig;
//    }
//
//    @Override
//    public void complete(Map<String, Object> dataMap, boolean success) {
//    }
//
//    /**
//     * 设置属性
//     */
//    private String getProperties(Map<String, String> property, String key, String defaultValue) {
//        return (null != property && !property.isEmpty()) ? property.getOrDefault(key, defaultValue) : defaultValue;
//    }
//
//    @Override
//    public DataRefactor getDataRefactor() {
//        return context -> {
//            context.markRecoredInsert();
//            Object dataObj = context.getCurrentRecord().getData();
//            if (dataObj != null) {
//                JSONObject originJsonObj = JSONObject.parseObject(JSONObject.toJSONString(dataObj));
//                Map<String, Object> resultMap = new HashMap<>();
//                if (originJsonObj != null) {
//                    // 数据转换操作
//                    List<LlmLogInfo> llmLogInfoList = new ArrayList<>();
//                    final LlmLogInfo llmLogInfo = new LlmLogInfo();
//                    boolean judgeInvoke = false;
//
//                    try {
//                        judgeInvoke = DataByModelMap.dataByModelMap(originJsonObj, resultMap, pluginConfig, llmLogInfo);
//                    } catch (Exception e) {
//                        log.error("dataByModelMap invoke failed ...", e);
//                    }
//
//                    // 先清空所有字段
//                    Set<String> delFieldSet = originJsonObj.keySet();
//                    for (String delField : delFieldSet) {
//                        context.addIgnoreFieldMapping(delField);
//                    }
//
//                    //再添加需要字段
//                    if (judgeInvoke && resultMap.containsKey("summary")) {//处理【过大模型的数据】需要加睡眠时间
//                        //TODO：这里调用鲁班，并且进行落库的封装
//                        log.info("origin data ----------------> {}", resultMap);
//                        List<String> inputs = new ArrayList<>();
//                        String summary = LubanUtil.addValueToLbList(inputs, resultMap.get("summary"));
//                        String category = LubanUtil.addValueToLbList(inputs, resultMap.get("category"));
//                        String event = LubanUtil.addValueToLbList(inputs, resultMap.get("event"));
//                        String organization = LubanUtil.addValueToLbList(inputs, resultMap.get("organization"));
//                        String word = LubanUtil.addValueToLbList(inputs, resultMap.get("word"));
//
//                        //对 inputs 进行去重
//                        Set<String> set = new HashSet<>(inputs);
//                        List<String> deduplicationInputList = new ArrayList<>(set);
//
//                        String baseUrl = pluginConfig.getSource().getBaseUrl();
//                        List<LubanOutputInfo> lubanOutputInfoList = new ArrayList<>();
//                        LubanUtil.getVector(deduplicationInputList, baseUrl, lubanOutputInfoList);
//
//                        List<JSONObject> summaryList = new ArrayList<>();
//                        List<JSONObject> categoryList = new ArrayList<>();
//                        List<JSONObject> eventList = new ArrayList<>();
//                        List<JSONObject> organizationList = new ArrayList<>();
//                        List<JSONObject> wordList = new ArrayList<>();
//
//                        for (LubanOutputInfo lubanOutputInfo : lubanOutputInfoList) {
//                            String text = lubanOutputInfo.getText();
//                            float[] vector1 = lubanOutputInfo.getVector1();
//                            float[] vector2 = lubanOutputInfo.getVector2();
//
//                            if (summary.equals(text)) {
//                                JSONObject jsonObj = new JSONObject();
//                                jsonObj.put("vector_o", vector1);
//                                jsonObj.put("vector_t", vector2);
//                                summaryList.add(jsonObj);
//                            }
//                            if (category.contains(text)) {
//                                JSONObject jsonObj = new JSONObject();
//                                jsonObj.put("vector_o", vector1);
//                                jsonObj.put("vector_t", vector2);
//                                categoryList.add(jsonObj);
//                            }
//                            if (event.contains(text)) {
//                                JSONObject jsonObj = new JSONObject();
//                                jsonObj.put("vector_o", vector1);
//                                jsonObj.put("vector_t", vector2);
//                                eventList.add(jsonObj);
//                            }
//                            if (organization.contains(text)) {
//                                JSONObject jsonObj = new JSONObject();
//                                jsonObj.put("vector_o", vector1);
//                                jsonObj.put("vector_t", vector2);
//                                organizationList.add(jsonObj);
//                            }
//                            if (word.contains(text)) {
//                                JSONObject jsonObj = new JSONObject();
//                                jsonObj.put("vector_o", vector1);
//                                jsonObj.put("vector_t", vector2);
//                                wordList.add(jsonObj);
//                            }
//                        }
//                        resultMap.put("summary_vector", summaryList);
//                        resultMap.put("category_vector", categoryList);
//                        resultMap.put("event_vector", eventList);
//                        resultMap.put("organization_vector", organizationList);
//                        resultMap.put("word_vector", wordList);
//
//                        if (MapUtils.getLongValue(resultMap, "vtl", 0) > pluginConfig.getSink().getVtl() * 1000) {
//                            context.setKafkaTopic(pluginConfig.getSink().getOtherTopic());
//                            log.info("sink topic --------------> {}", pluginConfig.getSink().getOtherTopic());
//                            context.addMapFieldValues(resultMap, true);
//                        }
//                    }
//                    else{
//                        context.setDrop(true);
//                    }
///*                    else {
//                        //为了方便演示，这里先注释掉
//                        log.info("sink topic --------------> {}", pluginConfig.getSink().getOtherTopic());
//                        context.setKafkaTopic(pluginConfig.getSink().getOtherTopic());
//                        context.addMapFieldValues(resultMap);
//                    }*/
//                    if (judgeInvoke) {
//                        llmLogInfoList.add(llmLogInfo);
//                        //TODO：这里 对 llmLofInfo 进行落库（MySQL）
//                        llmLogSink.insertLlmLog(ds, llmLogInfoList);
//                    }
//                }
//            }
//        };
//    }
//
//    @Override
//    public void start() {
//        new ImportBuilder()
//                .setInputConfig(this.getInputConfig()) // 设置同步数据源配置
//                .setOutputConfig(this.getOutputConfig()) // 设置同步目标源配置
//                .setBatchSize(taskConfig.getBatchSize()) // 设置刷新数据的批量大小
//                .setFlushInterval(taskConfig.getFlushInterval()) // 设置刷新数据的频率
//                .setPrintTaskLog(true) // 设置打印任务执行日志
//                .setContinueOnError(true) // 设置任务报错是否继续
//                .setIgnoreNullValueField(true)
//                .setDataRefactor(this.getDataRefactor()) // 设置数据转换
//                .builder() // 构建
//                .execute(); // 执行
//    }
//}