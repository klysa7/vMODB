package dk.ku.di.dms.vms.calcite;

import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.transaction.TransactionManager;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.handler.VmsEventHandler;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Queue;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class CalciteVmsNode {

    private static final System.Logger LOGGER = System.getLogger(CalciteVmsNode.class.getName());

    public static final class Bridge {
        private static VmsEmbedInternalChannels channels;
        public static Queue<InboundEvent> getInputQueue() {
            return channels.transactionInputQueue();
        }
    }

    public static void start() {
        try {
            LOGGER.log(INFO, "Booting Calcite VMS Kernel (Smart Reflection Mode)...");

            String vmsId = "99";
            int port = 9095;

            VmsNode me = new VmsNode("localhost", port, vmsId, 0, 0, 0,
                    new HashMap<>(), new HashMap<>(), new HashMap<>());

            VmsRuntimeMetadata metadata = new VmsRuntimeMetadata(
                    new HashMap<>(), new HashMap<>(), new HashMap<>(),
                    new HashMap<>(), new HashMap<>(), new HashMap<>(),
                    new HashMap<>(), new HashMap<>(), new HashMap<>()
            );

            VmsEmbedInternalChannels channels = new VmsEmbedInternalChannels();
            ITransactionManager txManager = new TransactionManager(new HashMap<>());
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

            VmsApplicationOptions options = createOptionsDynamically(port);

            VmsEventHandler eventHandler = VmsEventHandler.build(
                    me, txManager, channels, metadata, options,
                    IHttpHandler.DEFAULT, serdes
            );

            Bridge.channels = channels;

            Thread vmsThread = new Thread(eventHandler);
            vmsThread.setName("Calcite-VmsEventHandler");
            vmsThread.start();

            LOGGER.log(INFO, "Calcite VMS Kernel is RUNNING on Port " + port);

        } catch (Exception e) {
            LOGGER.log(ERROR, "CRITICAL: Failed to start Calcite VMS Node", e);
            throw new RuntimeException(e);
        }
    }

    private static VmsApplicationOptions createOptionsDynamically(int port) throws Exception {
        Constructor<?>[] constructors = VmsApplicationOptions.class.getDeclaredConstructors();

        // Find the constructor with the most parameters (likely the full one)
        Constructor<?> targetCtor = Arrays.stream(constructors)
                .max(Comparator.comparingInt(Constructor::getParameterCount))
                .orElseThrow(() -> new RuntimeException("No constructors found for VmsApplicationOptions"));

        targetCtor.setAccessible(true);
        Class<?>[] paramTypes = targetCtor.getParameterTypes();
        Object[] args = new Object[paramTypes.length];

        LOGGER.log(INFO, "Using constructor with " + paramTypes.length + " parameters.");

        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> type = paramTypes[i];
            if (type == int.class || type == Integer.class) {
                args[i] = (i <= 2) ? port : 16384;
            } else if (type == String.class) {
                args[i] = "localhost";
            } else if (type == boolean.class || type == Boolean.class) {
                args[i] = true; // logging=true, checkpointing=true (safe default)
            } else if (type == String[].class) {
                args[i] = new String[]{};
            } else {
                args[i] = null;
            }
        }

        return (VmsApplicationOptions) targetCtor.newInstance(args);
    }
}