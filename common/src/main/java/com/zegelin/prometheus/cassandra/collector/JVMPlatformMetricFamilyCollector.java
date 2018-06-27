package com.zegelin.prometheus.cassandra.collector;

//public class JVMPlatformMetricFamilyCollector implements MetricFamilyCollector {
//    @Override
//    public Stream<MetricFamily> collect() {
//
////        Iterables.concat()
//
//
////        final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
////
////        for (final MemoryPoolMXBean memoryPoolMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
////
////            memoryPoolMXBean.getUsage();
////            memoryPoolMXBean.getName()
////        }
//
//
//        return garbageCollectorMetrics().stream();
//    }
//
////    static Set<MetricFamily> memoryPoolMetrics() {
////
////    }
//
//    static Set<MetricFamily> garbageCollectorMetrics() {
//        final List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
//
//        final HashSet<NumericMetric> lastGCDurationMetrics = new HashSet<>(garbageCollectorMXBeans.size());
//
//        final HashSet<NumericMetric> collectionCountMetrics = new HashSet<>(garbageCollectorMXBeans.size());
//        final HashSet<NumericMetric> collectionTimeMetrics = new HashSet<>(garbageCollectorMXBeans.size());
//
//        for (final GarbageCollectorMXBean garbageCollectorMXBean : garbageCollectorMXBeans) {
//            final String collectorName = garbageCollectorMXBean.getName();
//
//            final ImmutableMap<String, String> labels = ImmutableMap.of("collector", collectorName);
//
//            // sun/oracle extensions
//            if (garbageCollectorMXBean instanceof com.sun.management.GarbageCollectorMXBean) {
//                final com.sun.management.GarbageCollectorMXBean sunGarbageCollectorMXBean = (com.sun.management.GarbageCollectorMXBean) garbageCollectorMXBean;
//
//                final GcInfo lastGcInfo = sunGarbageCollectorMXBean.getLastGcInfo();
//
//                lastGCDurationMetrics.add(new NumericMetric(labels, lastGcInfo.getDuration())); // last GC duration (ms)
////                lastGcInfo.getMemoryUsageBeforeGc();
////                lastGcInfo.getMemoryUsageAfterGc();
//
//            }
//
////            garbageCollectorMXBean1.getLastGcInfo().
//
//            collectionCountMetrics.add(new NumericMetric(labels, garbageCollectorMXBean.getCollectionCount())); // total GC count
//            collectionTimeMetrics.add(new NumericMetric(labels, garbageCollectorMXBean.getCollectionTime())); // elapsed time (ms)
//        }
//
//        return ImmutableSet.of(
//                new GaugeMetricFamily("jvm_gc_", null, lastGCDurationMetrics),
//
//                new GaugeMetricFamily("jvm_gc_count", null, collectionCountMetrics),
//                new GaugeMetricFamily("jvm_gc_seconds_total", null, collectionTimeMetrics)
//        );
//    }
//}
