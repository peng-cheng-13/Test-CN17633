/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Iterator;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays information for the path specified in args. Depends on different options, this command
 * can also display the information for all directly children under the path, or recursively.
 */
@ThreadSafe
public final class FindCommand extends WithWildCardPathCommand {
  public static final String IN_ALLUXIO_STATE_DIR = "DIR";
  public static final String IN_ALLUXIO_STATE_FILE_FORMAT = "%d%%";
  public static final String LS_FORMAT_PERMISSION = "%-11s";
  public static final String LS_FORMAT_FILE_SIZE = "%15s";
  public static final String LS_FORMAT_CREATE_TIME = "%24s";
  public static final String LS_FORMAT_ALLUXIO_STATE = "%5s";
  public static final String LS_FORMAT_PERSISTENCE_STATE = "%16s";
  public static final String LS_FORMAT_USER_NAME = "%-15s";
  public static final String LS_FORMAT_GROUP_NAME = "%-15s";
  public static final String LS_FORMAT_FILE_PATH = "%-5s";
  public static final String LS_FORMAT_NO_ACL = LS_FORMAT_FILE_SIZE + LS_FORMAT_PERSISTENCE_STATE
      + LS_FORMAT_CREATE_TIME + LS_FORMAT_ALLUXIO_STATE + " " + LS_FORMAT_FILE_PATH + "%n";
  public static final String LS_FORMAT = LS_FORMAT_PERMISSION + LS_FORMAT_USER_NAME
      + LS_FORMAT_GROUP_NAME + LS_FORMAT_FILE_SIZE + LS_FORMAT_PERSISTENCE_STATE
      + LS_FORMAT_CREATE_TIME + LS_FORMAT_ALLUXIO_STATE + " " + LS_FORMAT_FILE_PATH + "%n";

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .required(false)
          .hasArg(false)
          .desc("force to load metadata for immediate children in a directory")
          .build();

  private static final Option LIST_DIR_AS_FILE_OPTION =
      Option.builder("d")
          .required(false)
          .hasArg(false)
          .desc("list directories as plain files")
          .build();

  private static final Option LIST_HUMAN_READABLE_OPTION =
      Option.builder("h")
          .required(false)
          .hasArg(false)
          .desc("print human-readable format sizes")
          .build();

  private static final Option LIST_PINNED_FILES_OPTION =
      Option.builder("p")
          .required(false)
          .hasArg(false)
          .desc("list all pinned files")
          .build();

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("list subdirectories recursively")
          .build();

  /**
   * Constructs a new instance to display information for all directories and files directly under
   * the path specified in args.
   * @param fs the filesystem of Alluxio
   */
  public FindCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "find";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public Options getOptions() {
    Option keyOption =
        Option.builder("key").required(true).numberOfArgs(1).desc("key option").build();
    Option eqOption =
        Option.builder("eq").required(false).numberOfArgs(1).desc("select eq option").build();
    Option stOption =
        Option.builder("st").required(false).numberOfArgs(1).desc("select st option").build();
    Option ltOption =
        Option.builder("lt").required(false).numberOfArgs(1).desc("select lt option").build();
    Options tmpOp = new Options().addOption(keyOption).addOption(eqOption)
        .addOption(stOption).addOption(ltOption).addOption(RECURSIVE_OPTION)
            .addOption(FORCE_OPTION).addOption(LIST_DIR_AS_FILE_OPTION)
                .addOption(LIST_PINNED_FILES_OPTION).addOption(LIST_HUMAN_READABLE_OPTION);
    return tmpOp;
  }

  /**
   * Displays information for all directories and files directly under the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param recursive Whether list the path recursively
   * @param dirAsFile list the directory status as a plain file
   * @param hSize print human-readable format sizes
   * @param keylist the query key
   * @param valuelist the query value
   * @param typelist the query type
   */
  private void ls(AlluxioURI path, boolean recursive, boolean forceLoadMetadata, boolean dirAsFile,
                  boolean hSize, boolean pinnedOnly, List<String> keylist, List<String> valuelist,
                      List<String> typelist)
      throws AlluxioException, IOException {
    //Retrieve key list
    Set<String> pathkey = null;
    Set<String> pathlist = null;
    for (int i = 0; i < keylist.size(); i++) {
      System.out.println("Select key : " + keylist.get(i));
      System.out.println("Select value : " + valuelist.get(i));
      System.out.println("Select type : " + typelist.get(i));
    }
    long startTime = System.currentTimeMillis();
    try {
      pathkey = mFileSystem.selectValues(keylist, valuelist, typelist);
    } catch (Exception e) {
      System.out.println("Get path key error!");
      System.exit(-1);
    }
    if (pathkey == null) {
      System.out.println("No satisfied KV pairs in Hash Store");
      System.exit(-1);
    } else {
      Iterator<String> iterator = pathkey.iterator();
      while (iterator.hasNext()) {
        System.out.println("Selected key : " + iterator.next());
      }
    }
    //Retrieve path list
    try {
      pathlist = mFileSystem.selectPaths(pathkey);
    } catch (Exception e) {
      System.out.println("Get path list error!");
      System.exit(-1);
    }
    long endTime = System.currentTimeMillis();
    if (pathlist == null) {
      System.out.println("No satisfied KV pairs in Hash Store");
      System.exit(-1);
    } else {
      Iterator<String> iterator = pathlist.iterator();
      while (iterator.hasNext()) {
        System.out.println("Path : " + iterator.next());
      }
    }
    System.out.println("Select file in " + (endTime - startTime) + " ms.");
  }

  @Override
  public void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    List<String> keylist = new ArrayList<String>();
    List<String> valuelist = new ArrayList<String>();
    List<String> typelist = new ArrayList<String>();
    String qValue1;
    String qValue2;
    String selectType;
    if (cl.hasOption("key")) {
      keylist.add(cl.getOptionValue("key"));
    }
    if (cl.hasOption("eq")) {
      if (cl.hasOption("lt") || cl.hasOption("st")) {
        throw new IOException(
            "Wrong query condition! Usage: find <path> [-key key -eq v1 | -lt v2 -st v3]");
      }
      qValue1 = cl.getOptionValue("eq");
      valuelist.add(qValue1);
      //System.out.println("eq value is : " + qValue1);
      selectType = "eq";
      typelist.add(selectType);
    }
    if (cl.hasOption("lt")) {
      qValue1 =  cl.getOptionValue("lt");
      //System.out.println("lt value is : " + qValue1);
      valuelist.add(qValue1);
      selectType = "lt";
      typelist.add(selectType);
    }
    if (cl.hasOption("st")) {
      qValue2 =  cl.getOptionValue("st");
      valuelist.add(qValue2);
      //System.out.println("st value is : " + qValue2);
      selectType = "st";
      typelist.add(selectType);
    }
    if (cl.hasOption("lt") && cl.hasOption("st")) {
      keylist.add(cl.getOptionValue("key"));
    }
    if ((keylist.size() != valuelist.size()) || (keylist.size() != typelist.size())) {
      System.out.println("Error in query conditon, please check!");
      return;
    }
    ls(path, true, cl.hasOption("f"), cl.hasOption("d"), cl.hasOption("h"),
        cl.hasOption("p"), keylist, valuelist, typelist);
  }

  @Override
  public String getUsage() {
    return "find <path> [-key key -eq v1 | -lt v2 -st v3]";
  }

  @Override
  public String getDescription() {
    return "Query user-defined metadata";
  }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length < 1) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM_INSUFFICIENT
          .getMessage(getCommandName(), 1, args.length));
    }
  }
}
