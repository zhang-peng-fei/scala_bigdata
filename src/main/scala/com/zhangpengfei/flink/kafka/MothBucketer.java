package com.zhangpengfei.flink.kafka;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author 张朋飞
 */
public class MothBucketer<T> implements Bucketer<T> {

    @Override
    public Path getBucketPath(Clock clock, Path basePath, T element) {
        Tuple2 node = (Tuple2) element;
        String[] res = node._1.toString().split("-");
        System.out.println("res=" + Arrays.toString(res));
        return new Path(basePath, "month_id=" + res[0] + "/day_id=" + res[1]);
        /*String[] res = element.toString().split("|");
        System.out.println(element.toString());
        System.out.println(res.length);
        System.out.println(element.toString().length());

        return new Path(basePath, "month_id=" + res[0] + "/day_id=" + res[1]);*/
    }
}
