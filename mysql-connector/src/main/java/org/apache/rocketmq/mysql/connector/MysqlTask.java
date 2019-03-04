package org.apache.rocketmq.mysql.connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.DataEntryBuilder;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.mysql.Config;
import org.apache.rocketmq.mysql.MysqlConstants;
import org.apache.rocketmq.mysql.Replicator;
import org.apache.rocketmq.mysql.binlog.DataRow;
import org.apache.rocketmq.mysql.binlog.Transaction;
import org.apache.rocketmq.mysql.schema.column.ColumnParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MysqlTask.class);

    private Replicator replicator;

    private Config config;

    @Override
    public Collection<SourceDataEntry> poll() {

        List<SourceDataEntry> res = new ArrayList<>();

        try {
            Transaction transaction = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            if(null == transaction || null == transaction.getDataRows() || 0 == transaction.getDataRows().size()){
                return res;
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(MysqlConstants.BINLOG_FILENAME, transaction.getNextBinlogPosition().getBinlogFilename());
            jsonObject.put(MysqlConstants.NEXT_POSITION, transaction.getNextBinlogPosition().getPosition());

            for(DataRow dataRow : transaction.getDataRows()){
                Schema schema = new Schema();
                schema.setDataSource(dataRow.getTable().getDatabase());
                schema.setName(dataRow.getTable().getName());
                schema.setFields(new ArrayList<>());
                for(int i = 0; i < dataRow.getTable().getColList().size(); i++){
                    String columnName = dataRow.getTable().getColList().get(i);
                    String rawDataType = dataRow.getTable().getRawDataTypeList().get(i);
                    Field field = new Field(i, columnName, ColumnParser.mapConnectorFieldType(rawDataType));
                    schema.getFields().add(field);
                }
                DataEntryBuilder dataEntryBuilder = new DataEntryBuilder(schema);
                dataEntryBuilder.timestamp(System.currentTimeMillis())
                    .queue(dataRow.getTable().getName())
                    .entryType(dataRow.getType());
                for(int i = 0; i < dataRow.getTable().getColList().size() ; i++){
                    Object[] value = new Object[2];
                    if(null != dataRow.getRowBeforeUpdate()){
                        value[0] = dataRow.getTable().getParserList().get(i).getValue(dataRow.getRowBeforeUpdate()[i]);
                    }
                    if(null != dataRow.getRow()){
                        value[1] = dataRow.getTable().getParserList().get(i).getValue(dataRow.getRow()[i]);
                    }
                    dataEntryBuilder.putFiled(dataRow.getTable().getColList().get(i), JSON.toJSONString(value));
                }
                SourceDataEntry sourceDataEntry = dataEntryBuilder.buildSourceDataEntry(
                    MysqlConstants.getPartition(config.mysqlAddr, config.mysqlPort).getBytes("UTF-8"),
                    jsonObject.toJSONString().getBytes("UTF-8"));
                res.add(sourceDataEntry);
            }
        } catch (Exception e) {
            log.error("Mysql task poll error, current config:" + JSON.toJSONString(config), e);
        }
        return res;
    }

    @Override
    public void start(KeyValue props) {

        try {
            this.config = new Config();
            this.config.load(props);
            byte[] positionInfo = this.context.positionStorageReader().getPosition(MysqlConstants.getPartition(config.mysqlAddr, config.mysqlPort).getBytes("UTF-8"));

            if (null != positionInfo && positionInfo.length > 0) {

                String positionJson = new String(positionInfo, "UTF-8");
                JSONObject jsonObject = JSONObject.parseObject(positionJson);
                this.config.startType = "SPECIFIED";
                this.config.binlogFilename = jsonObject.getString(MysqlConstants.BINLOG_FILENAME);
                this.config.nextPosition = jsonObject.getLong(MysqlConstants.NEXT_POSITION);

            }
            this.replicator = new Replicator(config);
        } catch (Exception e) {
            log.error("Mysql task start failed.", e);
        }
        this.replicator.start();
    }

    @Override
    public void stop() {
        replicator.stop();
    }

    @Override public void pause() {

    }

    @Override public void resume() {

    }
}
