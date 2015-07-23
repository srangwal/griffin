/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin;

/**
 * A class representing supported compression types in Griffin
 * @author srangwala
*/
enum GriffinCompression {

    NONE   (""),        // No compression
    SNAPPY ("sz"),		// snappy
    BZIP2  ("bz2"),	    // bzip2
    GZIP   ("gz");		// gzip

    private String fileExtension;

    GriffinCompression(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public String getFileExtension() {
        return this.fileExtension;
    }

    public static GriffinCompression getByName(String name) {
        return GriffinCompression.valueOf(name.trim().toUpperCase());
    }

    public static String getFileExtensionByName(String name) {
        GriffinCompression comp = getByName(name);
        return comp.getFileExtension();
    }
}
