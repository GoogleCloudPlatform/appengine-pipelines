package com.google.appengine.tools.pipeline.impl;

/**
 * Queue settings implementation.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public final class QueueSettings implements Cloneable {

  private String onBackend;
  private String onModule;
  private String moduleVersion;
  private String onQueue;
  private Long delay;

  /**
   * Merge will override any {@code null} setting with a matching setting from {@code other}.
   */
  public QueueSettings merge(QueueSettings other) {
    if (onBackend == null && onModule == null) {
      onBackend = other.getOnBackend();
    }
    if (onModule == null && onBackend == null) {
      onModule = other.getOnModule();
      moduleVersion = other.getModuleVersion();
    }
    if (onQueue == null) {
      onQueue = other.getOnQueue();
    }
    return this;
  }

  public QueueSettings setOnBackend(String onBackend) {
    if (onBackend != null && onModule != null) {
      throw new IllegalStateException("OnModule and OnBackend cannot be combined");
    }
    this.onBackend = onBackend;
    return this;
  }

  public String getOnBackend() {
    return onBackend;
  }

  public QueueSettings setOnModule(String onModule) {
    if (onModule != null && onBackend != null) {
      throw new IllegalStateException("OnModule and OnBackend cannot be combined");
    }
    this.onModule = onModule;
    return this;
  }

  public String getOnModule() {
    return onModule;
  }

  public QueueSettings setModuleVersion(String moduleVersion) {
    this.moduleVersion = moduleVersion;
    return this;
  }

  public String getModuleVersion() {
    return moduleVersion;
  }

  public QueueSettings setOnQueue(String onQueue) {
    this.onQueue = onQueue;
    return this;
  }

  public String getOnQueue() {
    return onQueue;
  }

  public void setDelayInSeconds(Long delay) {
    this.delay = delay;
  }

  public Long getDelayInSeconds() {
    return delay;
  }

  @Override
  public QueueSettings clone() {
    try {
      return (QueueSettings) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Should never happen", e);
    }
  }

  @Override
  public String toString() {
    return "QueueSettings[onBackEnd=" + onBackend + ", onModule=" + onModule + ", moduleVersion="
        + moduleVersion + ", onQueue=" + onQueue + ", delayInSeconds=" + delay + "]";
  }
}
