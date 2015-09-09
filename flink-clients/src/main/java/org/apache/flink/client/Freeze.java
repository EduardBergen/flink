package org.apache.flink.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.*;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.*;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;

import org.apache.flink.runtime.taskmanager.*;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.Tuple2;
import scala.Tuple3;
import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Implementation of a simple command line fronted for executing remote programs locally. PROTOTYPE
 */
public class Freeze {
    private static final Logger LOG = LoggerFactory.getLogger(Freeze.class);
    private static final URI freezeUri = URI.create("/home/ed/Desktop/flinkFreezeConf/freezes/5");
    private static Configuration localConfiguration;

    /**
     * Basic prototyping implementation of a captured replay
     */
    public static void main(String[] args) {

        try {

            //
            //
            // Deserializing captured flink specific environment for the replay

            JobID jobId = (JobID) deserializeFromFile("jobId");
            JobVertexID jobVertexID = (JobVertexID) deserializeFromFile("vertexId");
            //int indexInSubtaskGroup = (Integer) deserializeFromFile("IndexInSubtaskGroup");
            int parallelism = (Integer) deserializeFromFile("parallelism");
            int subtaskIndex = (Integer) deserializeFromFile("subtaskIndex");
            Configuration jobConfiguration = (Configuration) deserializeFromFile("jobConfiguration");
            Configuration taskConfiguration = (Configuration) deserializeFromFile("taskConfiguration");

            InputSplit currentInputSplit = (InputSplit)deserializeFromFile("currentInputSplit");

            //List<BlobKey> requiredBlobKeys = (List<BlobKey>) deserializeFromFile("requiredBlobKeys");  Is not needed, blobkeys will be created on the fly
            List<String> requiredJarFileNames = (List<String>) deserializeFromFile("requiredJarFiles");

            List<File> requiredJarFiles = new ArrayList<File>();
            for (String filename : requiredJarFileNames) {
                requiredJarFiles.add(new File(freezeUri + File.separator + filename));
            }



            String nameOfInvokableClass = (String) deserializeFromFile("nameOfInvokableClass");
            //Timeout actorAskTimeout = (Timeout) deserializeFromFile("actorAskTimeout");
            SingleInputGate[] inputGates = (SingleInputGate[]) deserializeFromFile("inputGates");
            //Map<IntermediateDataSetID, SingleInputGate> inputGatesById = (Map<IntermediateDataSetID, SingleInputGate>) deserializeFromFile("inputGatesById");


            //
            //
            // Creating Blobs cache with required Jars and getting classloader

            localConfiguration = new Configuration();
            BlobServer blobServer = new BlobServer(localConfiguration);
            InetSocketAddress blobServerInetSocketAddress = new InetSocketAddress("localhost", blobServer.getPort());

            List<BlobKey> currentBlobKeys = uploadRequiredJarFiles(blobServerInetSocketAddress, requiredJarFiles);

            BlobCache localBlobCache = new BlobCache(blobServerInetSocketAddress, new Configuration());
            LibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(localBlobCache, 1000l);

            ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
            libraryCacheManager.registerTask(jobId, executionAttemptId, currentBlobKeys);
            ClassLoader userCodeClassLoader = libraryCacheManager.getClassLoader(jobId);


            //
            //
            // Setting up jobManager and taskInputSplitProvider

            String jobManagerAddress = jobConfiguration.getString(
                    ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);

            int jobManagerRPCPort = jobConfiguration.getInteger(
                    ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
                    ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

            InetSocketAddress endpointJobManger = new InetSocketAddress(InetAddress.getByName(jobManagerAddress), jobManagerRPCPort);

            Some<Tuple2<String, Object>> remoting = new Some<Tuple2<String, Object>>(new Tuple2<String, Object>("", 0));
            ActorSystem jobActorSystem = AkkaUtils.createActorSystem(jobConfiguration, remoting);
            ActorRef jobManager = JobManager.getJobManagerRemoteReference(endpointJobManger, jobActorSystem, jobConfiguration);


            //
            //
            // Setting up taskConfiguration for defaultMemoryManager;

            //
            //
            // Repositioning of chain information to first position inside the task configuration.
            // The captured exception environment has only one chain only.
            // This must be factorized in a general abstraction.

            String CHAINING_NUM_STUBS = "chaining.num";
            String OUTPUTS_NUM = "out.num";
            taskConfiguration.setInteger(CHAINING_NUM_STUBS, 1);
            taskConfiguration.setInteger(OUTPUTS_NUM, 1);
            taskConfiguration.setInteger("chaining.taskconfig.0.out.num", 1);
            taskConfiguration.setString("chaining.taskconfig.0.out.serializer", "org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory");
            taskConfiguration.setBytes("chaining.taskconfig.0.out.serializer.param.CLASS_DATA", taskConfiguration.getBytes("chaining.taskconfig.1.out.serializer.param.CLASS_DATA", null));
            taskConfiguration.setInteger("chaining.taskconfig.0.out.shipstrategy.0", taskConfiguration.getInteger("chaining.taskconfig.1.out.shipstrategy.0", -1));
            taskConfiguration.setString("chaining.taskconfig.0.out.comp.0", taskConfiguration.getString("chaining.taskconfig.1.out.comp.0", ""));
            taskConfiguration.setBytes("chaining.taskconfig.0.out.serializer.param.SER_DATA", taskConfiguration.getBytes("chaining.taskconfig.1.out.serializer.param.SER_DATA", null));
            taskConfiguration.setBytes("chaining.taskconfig.0.out.comp.param.0.SER_DATA", taskConfiguration.getBytes("chaining.taskconfig.1.out.comp.param.0.SER_DATA", null));

            String taskManagerHostname = taskConfiguration.getString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null);
            Tuple3<TaskManagerConfiguration, NetworkEnvironmentConfiguration, InstanceConnectionInfo> taskManagerConfiguration = TaskManager.parseTaskManagerConfiguration(taskConfiguration, taskManagerHostname, false);
            long configuredMemory = taskConfiguration.getLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1L);


            //
            //
            // Bringing up defaultMemoryManager

            long memorySize = -1L;
            if (configuredMemory > 0) {
                LOG.info("Using $configuredMemory MB for Flink managed memory.");
                memorySize = configuredMemory << 20;
            } else {
                float fraction = taskConfiguration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);
                if (!( fraction > 0.0f && fraction < 1.0f )) {
                    throw new IllegalConfigurationException("MemoryManager fraction of the free memory must be between 0.0 and 1.0");
                }
                memorySize = (long) (EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag() * fraction);
                LOG.info("Using $fraction of the currently free heap space for Flink managed " +
                        "memory (${relativeMemSize >> 20} MB).");
            }

            DefaultMemoryManager memoryManager = new DefaultMemoryManager(memorySize,
                    taskManagerConfiguration._1().numberOfSlots(),
                    taskManagerConfiguration._2().networkBufferSize(),
                    true);


            //
            //
            // Setting up BroadcastVariableManager

            IOManager ioManager = new IOManagerAsync();
            BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();
            Map<String, Future<org.apache.flink.core.fs.Path>> distributedCacheEntries = new HashMap<String, Future<org.apache.flink.core.fs.Path>>();


            //
            //
            // Bringing up specific ResultPartitionWriter for a task to write the output

            Integer resultPartitionSize = 1;
            ResultPartition[] producedPartitions = new ResultPartition[resultPartitionSize];
            ResultPartitionWriter[] writers = new ResultPartitionWriter[resultPartitionSize];

            String owningTaskName = "CHAIN DataSource (at getTextDataSet(WordCount.java:141) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at main(WordCount.java:50)) -> Combine(SUM(1), at main(WordCount.java:50) (1/1) (1cef6f97332a2cc31a350a0952008781)";
            ResultPartitionID resultPartitionId = new ResultPartitionID();
            int indexInSubTaskGroup = 0;
            ResultPartitionManager resultPartitionManager = new ResultPartitionManager();

            ResultPartition resultPartition = new ResultPartition(owningTaskName, jobId, resultPartitionId, ResultPartitionType.PIPELINED,
                    indexInSubTaskGroup, resultPartitionManager, new LoggerResultPartitionConsumableNotifier(LOG), ioManager, IOManager.IOMode.ASYNC);

            writers[0] = new ResultPartitionWriter(resultPartition);

            //
            //
            // Maybe the ResultWrite must be resetted

/*
            producedPartitions[0] = new ResultPartition(
                    indexInSubtaskGroup, //taskNameWithSubtasksAndId
                    jobId,
                    1, //partitionId
                    desc.getPartitionType(),
                    desc.getNumberOfSubpartitions(),
                    networkEnvironment.getPartitionManager(),
                    networkEnvironment.getPartitionConsumableNotifier(),
                    ioManager,
                    networkEnvironment.getDefaultIOMode());
*/

            //
            //
            // Setting up runtime and invoking the invokable

            Environment environment = new RuntimeEnvironment(jobId, jobVertexID, executionAttemptId, "taskName", "taskNameWithSubtask",
                    subtaskIndex, parallelism, jobConfiguration, taskConfiguration, userCodeClassLoader,
                    memoryManager, ioManager, broadcastVariableManager,
                    new FreezeInputSplitProvider(currentInputSplit), distributedCacheEntries, writers,
                    inputGates, jobManager);

            Class<? extends AbstractInvokable> invokableClass = Class
                    .forName(nameOfInvokableClass, true, userCodeClassLoader)
                    .asSubclass(AbstractInvokable.class);
            AbstractInvokable invokable = invokableClass.newInstance();
            invokable.setEnvironment(environment);
            invokable.registerInputOutput();
            invokable.invoke();

            //System.exit(retCode);
        }
        catch (Throwable t) {
            LOG.error("Fatal error while running command line interface.", t);
            t.printStackTrace();
            System.exit(31);
        }
    }

    public static List<BlobKey> uploadRequiredJarFiles(InetSocketAddress serverAddress, List<File> userJars) {
        List<BlobKey> userJarBlobKeys = new ArrayList<BlobKey>();

        BlobClient bc = null;
        try {
            bc = new BlobClient(serverAddress);
            for (final File jar: userJars) {
                try {
                    final BlobKey key = bc.put(Files.readAllBytes(Paths.get(jar.getAbsolutePath())));
                    userJarBlobKeys.add(key);
                } finally {
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bc != null) {
                try {
                    bc.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        return userJarBlobKeys;
    }
    private static Object deserializeFromFile(String name){


        String filePath = freezeUri.toString() + File.separatorChar + name;


        Object object = null;
        try {
            FileInputStream fileInputStream = new FileInputStream(filePath);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            object = objectInputStream.readObject();
            objectInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }

    private static class LoggerResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

        private final Logger externalClassLogger;

        public LoggerResultPartitionConsumableNotifier(Logger logger) {
            externalClassLogger = logger;
        }

        @Override
        public void notifyPartitionConsumable(JobID jobId, final ResultPartitionID partitionId) {
            externalClassLogger.info("Trying to find execution graph for job ID $jobId to schedule or update consumers by partition ID $partitionId.");
        }
    }

    private static class FreezeInputSplitProvider implements InputSplitProvider {

        private final InputSplit providedInputSplit;

        public FreezeInputSplitProvider(InputSplit providedInputSplit) {
            this.providedInputSplit = providedInputSplit;
        }


        @Override
        public InputSplit getNextInputSplit() {
            return providedInputSplit;
        }
    }
}
