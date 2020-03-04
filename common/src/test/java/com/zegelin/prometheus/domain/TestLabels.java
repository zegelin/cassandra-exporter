package com.zegelin.prometheus.domain;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import io.netty.util.ResourceLeakDetector;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestLabels {
    private static ResourceLeakDetector.Level originalDetectionLevel;

    @Mock
    private Appender<ILoggingEvent> loggingEventAppender;

    @BeforeClass
    public static void beforeClass() {
        originalDetectionLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(ResourceLeakDetector.class).addAppender(loggingEventAppender);
    }

    @AfterMethod
    public void afterMethod() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(ResourceLeakDetector.class).detachAppender(loggingEventAppender);
    }

    @AfterClass
    public static void afterClass() {
        ResourceLeakDetector.setLevel(originalDetectionLevel);
    }

    @Ignore ("Fails on a false-positive leak detection in Netty. See #46")
    @Test
    public void testPlainTextNettyLeakWarning() {
        for (int i = 0; i < 10; i++) {
            Labels labels = Labels.of("key", "value");
            labels.asPlainTextFormatUTF8EncodedByteBuf();
            Runtime.getRuntime().gc();
        }

        verifyNoMoreInteractions(loggingEventAppender);
    }

    @Test
    public void testJSONNettyLeakWarning() {
        for (int i = 0; i < 10; i++) {
            Labels labels = Labels.of("key", "value");
            labels.asJSONFormatUTF8EncodedByteBuf();
            Runtime.getRuntime().gc();
        }

        verifyNoMoreInteractions(loggingEventAppender);
    }

    @Test
    public void testPlainTextFinalizer() throws Throwable {
        Labels labels = Labels.of("key", "value");
        labels.asPlainTextFormatUTF8EncodedByteBuf();
        assertThatCode(() -> labels.finalize()).doesNotThrowAnyException();
    }

    @Test
    public void testJSONFinalizer() throws Throwable {
        Labels labels = Labels.of("key", "value");
        labels.asJSONFormatUTF8EncodedByteBuf();
        assertThatCode(() -> labels.finalize()).doesNotThrowAnyException();
    }
}
