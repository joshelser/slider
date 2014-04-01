/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hoya.yarn.cluster

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hadoop.yarn.service.launcher.ServiceLauncherBaseTest
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.HoyaXmlConfKeys
import org.apache.hoya.api.ClusterNode
import org.apache.hoya.exceptions.ErrorStrings
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.tools.*
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.yarn.appmaster.HoyaAppMaster
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hoya.yarn.params.ActionFreezeArgs
import org.junit.After
import org.junit.Assume
import org.junit.Rule
import org.junit.rules.Timeout

import static org.apache.hoya.testtools.HoyaTestUtils.*
import static org.apache.hoya.testtools.KeysForTests.*
import static org.apache.hoya.HoyaXMLConfKeysForTesting.*

/**
 * Base class for mini cluster tests -creates a field for the
 * mini yarn cluster
 */
@CompileStatic
@Slf4j
public abstract class YarnMiniClusterTestBase extends ServiceLauncherBaseTest {
  /**
   * Mini YARN cluster only
   */
  public static final int CLUSTER_GO_LIVE_TIME = 3 * 60 * 1000
  public static final int CLUSTER_STOP_TIME = 1 * 60 * 1000

  public static final int SIGTERM = -15
  public static final int SIGKILL = -9
  public static final int SIGSTOP = -17
  public static final String SERVICE_LAUNCHER = "ServiceLauncher"
  public static
  final String NO_ARCHIVE_DEFINED = "Archive configuration option not set: "
  /**
   * RAM for the YARN containers: {@value}
   */
  public static final String YRAM = "256"


  public static final YarnConfiguration HOYA_CONFIG = HoyaUtils.createConfiguration();
  static {
    HOYA_CONFIG.setInt(HoyaXmlConfKeys.KEY_AM_RESTART_LIMIT, 1)
    HOYA_CONFIG.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 100)
    HOYA_CONFIG.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false)
    HOYA_CONFIG.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false)
    HOYA_CONFIG.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 1)
    
  }


  public static final int THAW_WAIT_TIME
  public static final int FREEZE_WAIT_TIME
  public static final int HBASE_LAUNCH_WAIT_TIME
  public static final int ACCUMULO_LAUNCH_WAIT_TIME
  public static final int HOYA_TEST_TIMEOUT
  public static final boolean ACCUMULO_TESTS_ENABLED
  public static final boolean HBASE_TESTS_ENABLED
  static {
    THAW_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_TEST_THAW_WAIT_TIME,
        DEFAULT_THAW_WAIT_TIME)
    FREEZE_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_TEST_FREEZE_WAIT_TIME,
        DEFAULT_TEST_FREEZE_WAIT_TIME)
    HBASE_LAUNCH_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_TEST_HBASE_LAUNCH_TIME,
        DEFAULT_HBASE_LAUNCH_TIME)
    HOYA_TEST_TIMEOUT = HOYA_CONFIG.getInt(
        KEY_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT)
    ACCUMULO_LAUNCH_WAIT_TIME = HOYA_CONFIG.getInt(
        KEY_ACCUMULO_LAUNCH_TIME,
        DEFAULT_ACCUMULO_LAUNCH_TIME)
    ACCUMULO_TESTS_ENABLED =
        HOYA_CONFIG.getBoolean(KEY_TEST_ACCUMULO_ENABLED, true)
    HBASE_TESTS_ENABLED =
        HOYA_CONFIG.getBoolean(KEY_TEST_HBASE_ENABLED, true)

  }

  protected MiniDFSCluster hdfsCluster
  protected MiniYARNCluster miniCluster
  protected boolean switchToImageDeploy = false
  protected boolean imageIsRemote = false
  protected URI remoteImageURI

  protected List<HoyaClient> clustersToTeardown = [];


  @Rule
  public final Timeout testTimeout = new Timeout(HOYA_TEST_TIMEOUT);


  @After
  public void teardown() {
    describe("teardown")
    stopRunningClusters();
    stopMiniCluster();
  }

  protected void addToTeardown(HoyaClient client) {
    clustersToTeardown << client;
  }
  protected void addToTeardown(ServiceLauncher<HoyaClient> launcher) {
    HoyaClient hoyaClient = launcher.service
    if (hoyaClient) addToTeardown(hoyaClient)
  }


  protected YarnConfiguration getConfiguration() {
    return HOYA_CONFIG;
  }

  /**
   * Stop any running cluster that has been added
   */
  public void stopRunningClusters() {
    clustersToTeardown.each { HoyaClient hoyaClient ->
      try {
        maybeStopCluster(hoyaClient, "", "teardown");
      } catch (Exception e) {
        log.warn("While stopping cluster " + e, e);
      }
    }
  }

  public void stopMiniCluster() {
    Log l = LogFactory.getLog(this.getClass())
    ServiceOperations.stopQuietly(l, miniCluster)
    hdfsCluster?.shutdown();
  }

  /**
   * Create and start a minicluster
   * @param name cluster/test name
   * @param conf configuration to use
   * @param noOfNodeManagers #of NMs
   * @param numLocalDirs #of local dirs
   * @param numLogDirs #of log dirs
   * @param startZK create a ZK micro cluster
   * @param startHDFS create an HDFS mini cluster
   */
  protected void createMiniCluster(String name,
                                   YarnConfiguration conf,
                                   int noOfNodeManagers,
                                   int numLocalDirs,
                                   int numLogDirs,
                                   boolean startHDFS) {
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        FifoScheduler.class, ResourceScheduler.class);
    HoyaUtils.patchConfiguration(conf)
    miniCluster = new MiniYARNCluster(name, noOfNodeManagers, numLocalDirs, numLogDirs)
    miniCluster.init(conf)
    miniCluster.start();
    if (startHDFS) {
      createMiniHDFSCluster(name, conf)
    }
  }

  /**
   * Create a mini HDFS cluster and save it to the hdfsClusterField
   * @param name
   * @param conf
   */
  public void createMiniHDFSCluster(String name, YarnConfiguration conf) {
    hdfsCluster = buildMiniHDFSCluster(name, conf)
  }

  /**
   * Inner work building the mini dfs cluster
   * @param name
   * @param conf
   * @return
   */
  public static MiniDFSCluster buildMiniHDFSCluster(
      String name,
      YarnConfiguration conf) {
    File baseDir = new File("./target/hdfs/$name").absoluteFile;
    //use file: to rm it recursively
    FileUtil.fullyDelete(baseDir)
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.absolutePath)
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)

    def cluster = builder.build()
    return cluster
  }

  /**
   * Launch the hoya client with the specific args against the MiniMR cluster
   * launcher ie expected to have successfully completed
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  protected ServiceLauncher<HoyaClient> launchHoyaClientAgainstMiniMR(Configuration conf,
                                                                      List args) {
    ServiceLauncher<HoyaClient> launcher = launchHoyaClientNoExitCodeCheck(conf, args)
    int exited = launcher.serviceExitCode
    if (exited != 0) {
      throw new HoyaException(exited,"Launch failed with exit code $exited")
    }
    return launcher;
  }

  /**
   * Launch the hoya client with the specific args against the MiniMR cluster
   * without any checks for exit codes
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  public ServiceLauncher<HoyaClient> launchHoyaClientNoExitCodeCheck(
      Configuration conf,
      List args) {
    assert miniCluster != null
    return launchHoyaClientAgainstRM(RMAddr, args, conf)
  }


  /**
   * Kill all Hoya Services. That i
   * @param signal
   */
  public void killHoyaAM(int signal) {
    killJavaProcesses(HoyaAppMaster.SERVICE_CLASSNAME, signal)
  }

  /**
   * Kill any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public void killJavaProcesses(String grepString, int signal) {

    GString bashCommand = "jps -l| grep ${grepString} | awk '{print \$1}' | xargs kill $signal"
    log.info("Bash command = $bashCommand" )
    Process bash = ["bash", "-c", bashCommand].execute()
    bash.waitFor()

    log.info(bash.in.text)
    log.error(bash.err.text)
  }

  public void killJavaProcesses(List<String> greps, int signal) {
    for (String grep : greps) {
      killJavaProcesses(grep,signal)
    }
  }

  /**
   * List any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public String lsJavaProcesses() {
    Process bash = ["jps","-v"].execute()
    bash.waitFor()
    String out = bash.in.text
    log.info(out)
    String err = bash.err.text
    log.error(err)
    return out + "\n" + err
  }


  public void killServiceLaunchers(int value) {
    killHoyaAM(value)
  }

  public YarnConfiguration getTestConfiguration() {
    YarnConfiguration conf = getConfiguration()

    conf.addResource(HOYA_TEST)
    return conf
  }

  protected String getRMAddr() {
    assert miniCluster != null
    String addr = miniCluster.config.get(YarnConfiguration.RM_ADDRESS)
    assert addr != null;
    assert addr != "";
    return addr
  }

  /**
   * return the default filesystem, which is HDFS if the miniDFS cluster is
   * up, file:// if not
   * @return a filesystem string to pass down
   */
  protected String getFsDefaultName() {
    return buildFsDefaultName(hdfsCluster)
  }

  public static String buildFsDefaultName(MiniDFSCluster miniDFSCluster) {
    if (miniDFSCluster) {
      return "hdfs://localhost:${miniDFSCluster.nameNodePort}/"
    } else {
      return "file:///"
    }
  }

  protected String getWaitTimeArg() {
    return WAIT_TIME_ARG;
  }

  protected int getWaitTimeMillis(Configuration conf) {

    return WAIT_TIME * 1000;
  }

  /**
   * Create a hoya cluster
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<HoyaClient> createHoyaCluster(
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean deleteExistingData,
      boolean blockUntilRunning,
      Map<String, String> clusterOps) {
    createOrBuildHoyaCluster(
        HoyaActions.ACTION_CREATE,
        clustername,
        roles,
        extraArgs,
        deleteExistingData,
        blockUntilRunning,
        clusterOps)
  }

  /**
   * Create or build a hoya cluster (the action is set by the first verb)
   * @param action operation to invoke: HoyaActions.ACTION_CREATE or HoyaActions.ACTION_BUILD
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<HoyaClient> createOrBuildHoyaCluster(String action, String clustername, Map<String, Integer> roles, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning, Map<String, String> clusterOps) {
    assert clustername != null
    assert miniCluster != null
    if (deleteExistingData) {
      HadoopFS dfs = HadoopFS.get(new URI(fsDefaultName), miniCluster.config)
      Path clusterDir = new HoyaFileSystem(dfs, miniCluster.config).buildHoyaClusterDirPath(clustername)
      log.info("deleting customer data at $clusterDir")
      //this is a safety check to stop us doing something stupid like deleting /
      assert clusterDir.toString().contains("/.hoya/") || clusterDir.toString().contains("/.slider/")
      dfs.delete(clusterDir, true)
    }


    List<String> roleList = [];
    roles.each { String role, Integer val ->
      log.info("Role $role := $val")
      roleList << Arguments.ARG_COMPONENT << role << Integer.toString(val)
    }

    List<String> argsList = [
        action, clustername,
        Arguments.ARG_MANAGER, RMAddr,
        Arguments.ARG_FILESYSTEM, fsDefaultName,
        Arguments.ARG_DEBUG,
        Arguments.ARG_CONFDIR, confDir
    ]
    if (blockUntilRunning) {
      argsList << Arguments.ARG_WAIT << WAIT_TIME_ARG
    }

    argsList += getExtraHoyaClientArgs()
    argsList += roleList;
    argsList += imageCommands

    //now inject any cluster options
    clusterOps.each { String opt, String val ->
      argsList << Arguments.ARG_OPTION << opt << val;
    }

    if (extraArgs != null) {
      argsList += extraArgs;
    }
    ServiceLauncher<HoyaClient> launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        argsList
    )
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    if (blockUntilRunning) {
      hoyaClient.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME))
    }
    return launcher;
  }

  /**
   * Add arguments to launch Hoya with.
   *
   * Extra arguments are added after standard arguments and before roles.
   *
   * @return additional arguments to launch Hoya with
   */
  protected List<String> getExtraHoyaClientArgs() {
    []
  }

  public String getConfDir() {
    return resourceConfDirURI
  }

  /**
   * Get the key for the application
   * @return
   */
  public String getApplicationHomeKey() {
    failNotImplemented()
    null
  }

  /**
   * Get the archive path -which defaults to the local one
   * @return
   */
  public String getArchivePath() {
    return localArchive
  }
  /**
   * Get the local archive -the one defined in the test configuration
   * @return a possibly null/empty string
   */
  public final String getLocalArchive() {
    return testConfiguration.getTrimmed(archiveKey)
  }
  /**
   * Get the key for archives in tests
   * @return
   */
  public String getArchiveKey() {
    failNotImplemented()
    null
  }

  public void assumeArchiveDefined() {
    String archive = archivePath
    boolean defined = archive != null && archive != ""
    if (!defined) {
      log.warn(NO_ARCHIVE_DEFINED + archiveKey);
    }
    Assume.assumeTrue(NO_ARCHIVE_DEFINED + archiveKey, defined)
  }
  /**
   * Assume that application home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * application home to be set.
   */
  public void assumeApplicationHome() {
    Assume.assumeTrue("Application home dir option not set " + applicationHomeKey,
        applicationHome != null && applicationHome != "")
  }


  public String getApplicationHome() {
    return testConfiguration.getTrimmed(applicationHomeKey)
  }

  public List<String> getImageCommands() {
    if (switchToImageDeploy) {
      // its an image that had better be defined
      assert archivePath
      if (!imageIsRemote) {
        // its not remote, so assert it exists
        File f = new File(archivePath)
        assert f.exists()
        return [Arguments.ARG_IMAGE, f.toURI().toString()]
      } else {
        assert remoteImageURI

        // if it is remote, then its whatever the archivePath property refers to
        return [Arguments.ARG_IMAGE, remoteImageURI.toString()];
      }
    } else {
      assert applicationHome
      assert new File(applicationHome).exists();
      return [Arguments.ARG_APP_HOME, applicationHome]
    }
  }

  /**
   * Start a cluster that has already been defined
   * @param clustername cluster name
   * @param extraArgs list of extra args to add to the creation command
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<HoyaClient> thawHoyaCluster(String clustername, List<String> extraArgs, boolean blockUntilRunning) {
    assert clustername != null
    assert miniCluster != null

    List<String> argsList = [
        HoyaActions.ACTION_THAW, clustername,
        Arguments.ARG_MANAGER, RMAddr,
        Arguments.ARG_WAIT, WAIT_TIME_ARG,
        Arguments.ARG_FILESYSTEM, fsDefaultName,
    ]
    if (extraArgs != null) {
      argsList += extraArgs;
    }
    ServiceLauncher<HoyaClient> launcher = launchHoyaClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        argsList
    )
    assert launcher.serviceExitCode == 0
    HoyaClient hoyaClient = (HoyaClient) launcher.service
    if (blockUntilRunning) {
      hoyaClient.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME))
    }
    return launcher;
  }


  /**
   * Get the resource configuration dir in the source tree
   * @return
   */
  public File getResourceConfDir() {
    File f = new File(testConfigurationPath).absoluteFile;
    if (!f.exists()) {
      throw new FileNotFoundException("Resource configuration directory $f not found")
    }
    return f;
  }

  public String getTestConfigurationPath() {
    failNotImplemented()
    null;
  }

  /**
   get a URI string to the resource conf dir that is suitable for passing down
   to the AM -and works even when the default FS is hdfs
   */
  public String getResourceConfDirURI() {;
    return resourceConfDir.absoluteFile.toURI().toString()
  }


  public void logReport(ApplicationReport report) {
    log.info(HoyaUtils.reportToString(report));
  }


  public void logApplications(List<ApplicationReport> apps) {
    apps.each { ApplicationReport r -> logReport(r) }
  }

  /**
   * Wait for the cluster live; fail if it isn't within the (standard) timeout
   * @param hoyaClient client
   * @return the app report of the live cluster
   */
  public ApplicationReport waitForClusterLive(HoyaClient hoyaClient) {
    return waitForClusterLive(hoyaClient, CLUSTER_GO_LIVE_TIME)
  }

  /**
   * force kill the application after waiting for
   * it to shut down cleanly
   * @param hoyaClient client to talk to
   */
  public ApplicationReport waitForAppToFinish(HoyaClient hoyaClient) {

    int waitTime = getWaitTimeMillis(hoyaClient.config)
    return waitForAppToFinish(hoyaClient, waitTime)
  }

  public static ApplicationReport waitForAppToFinish(
      HoyaClient hoyaClient,
      int waitTime) {
    ApplicationReport report = hoyaClient.monitorAppToState(
        YarnApplicationState.FINISHED,
        new Duration(waitTime));
    if (report == null) {
      log.info("Forcibly killing application")
      dumpClusterStatus(hoyaClient, "final application status")
      //list all the nodes' details
      List<ClusterNode> nodes = listNodesInRole(hoyaClient, "")
      if (nodes.empty) {
        log.info("No live nodes")
      }
      nodes.each { ClusterNode node -> log.info(node.toString())}
      hoyaClient.forceKillApplication("timed out waiting for application to complete");
    }
    return report;
  }

  /**
   * stop the cluster via the stop action -and wait for {@link #CLUSTER_STOP_TIME}
   * for the cluster to stop. If it doesn't
   * @param hoyaClient client
   * @param clustername cluster
   * @return the exit code
   */
  public int clusterActionFreeze(HoyaClient hoyaClient, String clustername, String message = "action freeze") {
    log.info("Freezing cluster $clustername: $message")
    ActionFreezeArgs freezeArgs  = new ActionFreezeArgs();
    freezeArgs.waittime = CLUSTER_STOP_TIME
    freezeArgs.message = message
    int exitCode = hoyaClient.actionFreeze(clustername,
        freezeArgs);
    if (exitCode != 0) {
      log.warn("Cluster freeze failed with error code $exitCode")
    }
    return exitCode
  }

  /**
   * Teardown-time cluster termination; will stop the cluster iff the client
   * is not null
   * @param hoyaClient client
   * @param clustername name of cluster to teardown
   * @return
   */
  public int maybeStopCluster(
      HoyaClient hoyaClient,
      String clustername,
      String message) {
    if (hoyaClient != null) {
      if (!clustername) {
        clustername = hoyaClient.deployedClusterName;
      }
      //only stop a cluster that exists
      if (clustername) {
        return clusterActionFreeze(hoyaClient, clustername, message);
      }
    }
    return 0;
  }


  String roleMapToString(Map<String,Integer> roles) {
    StringBuilder builder = new StringBuilder()
    roles.each { String name, int value ->
      builder.append("$name->$value ")
    }
    return builder.toString()
  }

  /**
   * Turn on test runs against a copy of the archive that is
   * uploaded to HDFS -this method copies up the
   * archive then switches the tests into archive mode
   */
  public void enableTestRunAgainstUploadedArchive() {
    Path remotePath = copyLocalArchiveToHDFS(localArchive)
    // image mode
    switchToRemoteImageDeploy(remotePath);
  }

  /**
   * Switch to deploying a remote image
   * @param remotePath the remote path to use
   */
  public void switchToRemoteImageDeploy(Path remotePath) {
    switchToImageDeploy = true
    imageIsRemote = true
    remoteImageURI = remotePath.toUri()
  }

  /**
   * Copy a local archive to HDFS
   * @param localArchive local archive
   * @return the path of the uploaded image
   */
  public Path copyLocalArchiveToHDFS(String localArchive) {
    assert localArchive
    File localArchiveFile = new File(localArchive)
    assert localArchiveFile.exists()
    assert hdfsCluster
    Path remoteUnresolvedArchive = new Path(localArchiveFile.name)
    assert FileUtil.copy(
        localArchiveFile,
        hdfsCluster.fileSystem,
        remoteUnresolvedArchive,
        false,
        testConfiguration)
    Path remotePath = hdfsCluster.fileSystem.resolvePath(
        remoteUnresolvedArchive)
    return remotePath
  }

  /**
   * Assert that an operation failed because a cluster is in use
   * @param e exception
   */
  public static void assertFailureClusterInUse(HoyaException e) {
    assertExceptionDetails(e,
        HoyaExitCodes.EXIT_APPLICATION_IN_USE,
        ErrorStrings.E_CLUSTER_RUNNING)
  }

  /**
   * Create a HoyaFileSystem instance bonded to the running FS.
   * The YARN cluster must be up and running already
   * @return
   */
  public HoyaFileSystem createHoyaFileSystem() {
    HadoopFS dfs = HadoopFS.get(new URI(fsDefaultName), configuration)
    HoyaFileSystem hfs = new HoyaFileSystem(dfs, configuration)
    return hfs
  }

}
