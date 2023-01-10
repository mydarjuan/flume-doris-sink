package com.darjuan.flume.doris.service;

import org.apache.flume.Event;

import java.io.UnsupportedEncodingException;

public interface EventProcess {
    void eventAppend(Event event) throws UnsupportedEncodingException;
}
