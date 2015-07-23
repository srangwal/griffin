/**
 * Copyright (c) 2015, Turn Inc. All Rights Reserved.
 * Use of this source code is governed by a BSD-style license that can be found
 * in the LICENSE file.
 **/
package com.turn.griffin;

import com.google.common.base.Optional;
import com.turn.griffin.GriffinControl.FileInfo;

import java.util.Map;
import java.util.Scanner;

/**
 *
 * Created by srangwala on 7/20/15.
 */
public class GriffinDaemon {

    public static void main(String[] args) {

        Optional<GriffinModule> module = GriffinModule.getInstance();
        Optional<GriffinLibCacheUtil> libCacheManager = module.get().getLibCacheManager();

        Scanner sc = new Scanner(System.in);
        Map<String, FileInfo> cache;
        while (true) {
            System.out.print("Enter command: ");
            String command = sc.next();

            switch (command) {

                case "help":
                    System.out.println("help: list of available commands");
                    System.out.println("localrepo: List of all the files in local repository");
                    System.out.println("globalrepo: List of all the files in global repository");
                    System.out.println("missingfiles: List of files missing in the local repository ");
                    System.out.println("syncblob <blobname> <dest> <filepath>: Push given file to specified instances in the cluster");
                    System.out.println("\t e.g., syncblob config .* config.properties");
                    break;
                case "localrepo":
                    cache = libCacheManager.get().getLatestLocalFileInfo().get();
                    System.out.println("Local Repo");
                    printCache(cache);
                    break;

                case "globalrepo":
                    cache = libCacheManager.get().getLatestGlobalFileInfo().get();
                    System.out.println("Global Repo");
                    printCache(cache);
                    break;

                case "missingfiles":
                    cache = libCacheManager.get().findFilesToDownload();
                    System.out.println("Missing Files");
                    printCache(cache);
                    break;

                case "syncblob":
                    String blobname = sc.next();
                    String dest = sc.next();
                    String filename = sc.next();
                    module.get().syncBlob(blobname, dest, filename);
                    System.out.println(String.format("Pushed file %s as blob %s to destination %s", filename, blobname, dest));
                    break;

                default:
                    System.out.println("Invalid command:");

            }

        }
    }

    private static void printCache(Map<String, FileInfo> cache) {
        for (FileInfo f : cache.values()) {
            System.out.println(String.format("\t%s",
                    f.toString().replaceAll(System.getProperty("line.separator"), " ")));
        }
    }
}
