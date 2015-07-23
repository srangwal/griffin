/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Created by srangwala on 7/20/15.
 */
public class GriffinRangedIntConfig {

    public static final Logger logger = LoggerFactory.getLogger(GriffinRangedIntConfig.class);
    private String propertyName;
    private String description;
    private Range<Integer> integerRange;
    private int defaultValue;

    public GriffinRangedIntConfig(String propertyName, String description, int defaultValue,
                                  int minValue, int maxValue) {
        this.propertyName = propertyName;
        this.description = description;
        this.integerRange = Range.closed(minValue, maxValue);

        this.defaultValue = defaultValue;
    }


    public String getDescription() {
        return description;
    }

    public int getValue() {
        String value = GriffinConfig.getProperty(this.propertyName, Integer.toString(this.defaultValue));
        try {
            int intValue = Integer.parseInt(value);
            if (integerRange.contains(intValue)) {
                return intValue;
            }
        } catch (Exception e) {
            /* we will return default value */
        }
        return this.defaultValue;

    }
}
