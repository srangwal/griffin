/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 *  Base class to read properties
 *  TODO: Make this class thread-safe
 * Created by srangwala on 7/20/15.
 */
public class GriffinConfig {

    public static final Logger logger = LoggerFactory.getLogger(GriffinConfig.class);
    public static final String configFile = "config.properties";
    private static Properties properties = null;

    private GriffinConfig() {
    }

    private static void init() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(configFile));
        } catch (IOException e) {
            Preconditions.checkState(false, e.getMessage());
        }
    }

    public static String getProperty(String propertyName, String defaultValue) {
        Preconditions.checkNotNull(propertyName);
        if (properties == null) {
            init();
        }
        return properties.getProperty(propertyName, defaultValue);
    }

    public static String getProperty(String propertyName) {
        Preconditions.checkNotNull(propertyName);
        if (properties == null) {
            init();
        }
        return properties.getProperty(propertyName);
    }

}
