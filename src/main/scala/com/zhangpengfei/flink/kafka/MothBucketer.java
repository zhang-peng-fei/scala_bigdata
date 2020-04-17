package com.zhangpengfei.flink.kafka;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

/**
 * @author 张朋飞
 */
public class MothBucketer<T> implements Bucketer<T> {

    @Override
    public Path getBucketPath(Clock clock, Path basePath, T element) {
        ApiCallLog apiCallLog = (ApiCallLog) element;
        return new Path(basePath, "month_id=" + apiCallLog.getMonthId() + "/day_id=" + apiCallLog.getDayId());
    }
}
