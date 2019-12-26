package com.zhangpengfei.flink.kafka;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

/**
 * @author 张朋飞
 */
public class MothBucketer<T> implements Bucketer<T> {

    @Override
    public Path getBucketPath(Clock clock, Path basePath, T element) {
        ObjectNode node = (ObjectNode) element;
        String monthId = node.get("monthId").toString();
        String dayId = node.get("dayId").toString();

        return new Path(basePath,"month_id=" + monthId + "/day_id=" + dayId);
    }
}
