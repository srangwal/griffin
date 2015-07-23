/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin.utils;

/**
 *
 * Created by srangwala on 7/20/15.
 */
public class GriffinStringConfig {

    private String propertyName;
    private String description;
    private String defaultValue;

    public GriffinStringConfig(String propertyName, String description, String defaultValue) {
        this.propertyName = propertyName;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public String getValue() {
        return GriffinConfig.getProperty(this.propertyName, this.defaultValue);

    }
}
