/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.metastore.analyze;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.metastore.config.MetastoreConfigConstants;
import org.apache.drill.metastore.config.MetastoreConfigFileInfo;
import org.apache.drill.metastore.exceptions.MetastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Class is responsible for returning instance of {@link AnalyzeInfoProvider} class for specific {@link GroupScan}
 * which will be initialized based on {@link MetastoreConfigConstants#ANALYZE_INFO_PROVIDER_CLASSES} config property values.
 * AnalyzeInfoProviders list initialization is delayed until {@link #get(GroupScan)} method is called.
 * AnalyzeInfoProvider implementations must have constructor without arguments.
 */
public class AnalyzeInfoProviderRegistry {
  private static final Logger logger = LoggerFactory.getLogger(AnalyzeInfoProviderRegistry.class);

  private final DrillConfig config;
  private volatile List<AnalyzeInfoProvider> analyzeInfoProviders;

  public AnalyzeInfoProviderRegistry(DrillConfig config) {
    this.config = config;
  }

  /**
   * Returns {@link AnalyzeInfoProvider} instance suitable for specified {@link GroupScan}.
   *
   * @param groupScan group scan
   * @return {@link AnalyzeInfoProvider} instance
   */
  public AnalyzeInfoProvider get(GroupScan groupScan) {
    if (analyzeInfoProviders == null) {
      synchronized (this) {
        if (analyzeInfoProviders == null) {
          analyzeInfoProviders = Collections.unmodifiableList(init());
        }
      }
    }

    List<AnalyzeInfoProvider> suitableProviders = analyzeInfoProviders.stream()
        .filter(analyzeInfoProvider -> analyzeInfoProvider.supportsGroupScan(groupScan))
        .collect(Collectors.toList());

    if (suitableProviders.isEmpty()) {
      throw new MetastoreException(String.format("Unable to find AnalyzeInfoProvider implementation for GroupScan class [%s]",
          groupScan.getClass().getSimpleName()));
    } else if (suitableProviders.size() > 1) {
      logger.warn("Found several AnalyzeInfoProvider implementations for the same GroupScan class [{}}]. " +
          "Please specify only unique AnalyzeInfoProvider implementations for every group scan using [{}] property " +
              "to prevent using arbitrary implementation.",
          groupScan.getClass().getSimpleName(), MetastoreConfigConstants.ANALYZE_INFO_PROVIDER_CLASSES);
    }

    return suitableProviders.get(0);
  }

  private List<AnalyzeInfoProvider> init() {
    List<AnalyzeInfoProvider> providers;
    DrillConfig metastoreConfig = DrillConfig.create(
        null, null, true, new MetastoreConfigFileInfo(), config.root());
    List<String> classNames = metastoreConfig.getStringList(MetastoreConfigConstants.ANALYZE_INFO_PROVIDER_CLASSES);

    providers = classNames.stream()
        .map(this::getAnalyzeInfoProviderInstance)
        .filter(Objects::nonNull)
        .map(instance -> (AnalyzeInfoProvider) instance)
        .collect(Collectors.toList());
    return providers;
  }

  private Object getAnalyzeInfoProviderInstance(String className) {
    MethodHandles.Lookup publicLookup = MethodHandles.publicLookup();
    MethodHandle constructor;
    try {
      MethodType methodType = MethodType.methodType(void.class);
      constructor = publicLookup.findConstructor(Class.forName(className), methodType);
    } catch (ClassNotFoundException e) {
      logger.error("Unable to find AnalyzeInfoProvider implementation class [{}}]", className, e);
      return null;
    } catch (NoSuchMethodException | IllegalAccessException e) {
      logger.error("AnalyzeInfoProvider implementation class [%s] must have constructor without arguments.");
      return null;
    }

    Object instance;
    try {
      instance = constructor.invoke();
    } catch (Throwable e) {
      logger.error("Unable to init AnalyzeInfoProvider class [{}}]", className, e);
      return null;
    }

    if (!(instance instanceof AnalyzeInfoProvider)) {
      logger.error("Created instance of [{}] does not implement [{}] interface",
              instance.getClass().getSimpleName(), AnalyzeInfoProvider.class.getSimpleName());
      return null;
    }
    return instance;
  }
}
