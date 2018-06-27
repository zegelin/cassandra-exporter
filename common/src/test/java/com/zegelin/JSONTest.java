package com.zegelin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.SummaryMetricFamily;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.annotate.JsonRootName;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.annotate.JsonTypeResolver;
import org.codehaus.jackson.map.jsontype.TypeIdResolver;
import org.codehaus.jackson.map.jsontype.TypeResolverBuilder;
import org.codehaus.jackson.type.JavaType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JSONTest {

    abstract class MetricFamilyMixin {

    }



    public static void main(String[] args) throws IOException {
//
//        final ImmutableList<MetricFamily<?>> metricFamilies = ImmutableList.of(
//                new SummaryMetricFamily("summary_metric", "blah blah blah", ImmutableMap.of(
//                        ImmutableMap.of("label", "value1"), new SummaryMetricFamily.Summary(123, 111, ImmutableMap.of(.5, 1, .75, 2, .9, 3)),
//                        ImmutableMap.of("label", "value2"), new SummaryMetricFamily.Summary(123, 111, ImmutableMap.of(.5, 1, .75, 2, .9, 3))
//                )),
//                new GaugeMetricFamily("gauge_metric", "blah blah", ImmutableMap.of(
//                        ImmutableMap.of("label", "value1"), 123,
//                        ImmutableMap.of("label", "value2"), 456
//                ))
//        );
//
//
//        final ObjectMapper objectMapper = new ObjectMapper();
//
//        objectMapper.getSerializationConfig().addMixInAnnotations(MetricFamily.class, MetricFamilyMixin.class);
//
//        objectMapper.writerWithDefaultPrettyPrinter().writeValue(System.out, metricFamilies);

    }

}
