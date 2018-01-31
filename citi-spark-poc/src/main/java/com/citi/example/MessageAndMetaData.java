package com.citi.example;

import java.io.Serializable;

import kafka.cluster.Partition;

public class MessageAndMetaData<E> implements Serializable {

  private E payload;
  private byte[] key;
  private long offset;
  private Partition partition;
  private String topic;
  private String consumer;

  public byte[] getKey() {
    return key;
  }

  public void setKey(byte[] key) {
    this.key = key;
  }

  public Partition getPartition() {
    return partition;
  }

  public void setPartition(Partition partition) {
    this.partition = partition;
  }

  public String getConsumer() {
    return consumer;
  }

  public void setConsumer(String consumer) {
    this.consumer = consumer;
  }

  public E getPayload() {
    return payload;
  }

  public void setPayload(E msg) {
    this.payload = msg;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }
}