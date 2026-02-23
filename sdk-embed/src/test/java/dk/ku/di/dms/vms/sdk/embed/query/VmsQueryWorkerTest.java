//package dk.ku.di.dms.vms.sdk.embed.query;
//
//import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
//import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;
//import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent;
//import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.MockedStatic;
//import org.mockito.Mockito;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import java.nio.ByteBuffer;
//import java.nio.channels.AsynchronousSocketChannel;
//import java.nio.channels.CompletionHandler;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.mockito.ArgumentMatchers.*;
//import static org.mockito.Mockito.*;
//
//@ExtendWith(MockitoExtension.class)
//class VmsQueryWorkerTest {
//
//    @Mock AsynchronousSocketChannel channel;
//    @Mock UniqueHashBufferIndex index;
//    @Mock Iterator<Long> recordIterator;
//    MockedStatic<MemoryManager> memoryManagerMock;
//    VmsQueryWorker worker;
//    QueryRequestEvent.QueryPayload payload;
//    List<byte[]> capturedPackets;
//    final int BUFFER_SIZE = 100;
//    final int RECORD_SIZE = 40;
//    final long QUERY_ID = 999L;
//
//    @BeforeEach
//    void setUp() {
//        capturedPackets = new ArrayList<>();
//
//        memoryManagerMock = Mockito.mockStatic(MemoryManager.class);
//        memoryManagerMock.when(() -> MemoryManager.getTemporaryDirectBuffer(anyInt()))
//                .thenAnswer(inv -> ByteBuffer.allocate(inv.getArgument(0)));
//
//        payload = new QueryRequestEvent.QueryPayload(QUERY_ID, 1L, "TestTable", null);
//
//        lenient().when(index.getRecordSize()).thenReturn(RECORD_SIZE);
//        lenient().doAnswer(invocation -> {
//            ByteBuffer buf = invocation.getArgument(1);
//            buf.position(buf.position() + RECORD_SIZE); // Simulate data write
//            return null;
//        }).when(index).copyRecordToBuffer(anyLong(), any(ByteBuffer.class));
//
//        doAnswer(invocation -> {
//            ByteBuffer buf = invocation.getArgument(0);
//            CompletionHandler<Integer, ByteBuffer> handler = invocation.getArgument(4);
//
//            ByteBuffer copy = buf.duplicate();
//
//            byte[] packetData = new byte[copy.remaining()];
//            copy.get(packetData); // Read the data
//            capturedPackets.add(packetData);
//
//            int bytesWritten = buf.remaining();
//            buf.position(buf.limit());
//            handler.completed(bytesWritten, buf);
//            return null;
//        }).when(channel).write(any(), anyLong(), any(), any(), any());
//    }
//
//    @AfterEach
//    void tearDown() {
//        memoryManagerMock.close();
//    }
//
//    @Test
//    @DisplayName("Should send one batch and EOS when data fits in buffer")
//    void testSingleRecordScan() {
//        when(recordIterator.hasNext()).thenReturn(true, false);
//        when(recordIterator.next()).thenReturn(100L);
//
//        worker = new VmsQueryWorker(channel, index, recordIterator, payload, BUFFER_SIZE, 1000);
//        worker.run();
//
//        verify(index, times(1)).copyRecordToBuffer(eq(100L), any(ByteBuffer.class));
//        verify(channel, times(2)).write(any(), anyLong(), any(), any(), any());
//
//        assertEquals(2, capturedPackets.size());
//
//        byte[] dataPacket = capturedPackets.get(0);
//        assertEquals(QueryResultEvent.QUERY_RESULT_TYPE, dataPacket[0]); // Check Type Byte
//
//        byte[] eosPacket = capturedPackets.get(1);
//        assertEquals(QueryResultEvent.END_OF_STREAM_TYPE, eosPacket[0]); // Check Type Byte
//    }
//
//    @Test
//    @DisplayName("Should split into multiple batches when buffer fills up")
//    void testBufferOverflow() {
//
//        when(recordIterator.hasNext()).thenReturn(true, true, true, false);
//        when(recordIterator.next()).thenReturn(100L, 200L, 300L);
//
//        worker = new VmsQueryWorker(channel, index, recordIterator, payload, BUFFER_SIZE, 1000);
//        worker.run();
//
//        verify(index, times(3)).copyRecordToBuffer(anyLong(), any(ByteBuffer.class));
//
//        verify(channel, times(3)).write(any(), anyLong(), any(), any(), any());
//        assertEquals(3, capturedPackets.size());
//    }
//
//    @Test
//    @DisplayName("Should send only EOS if iterator is empty")
//    void testEmptyScan() {
//        when(recordIterator.hasNext()).thenReturn(false);
//
//        worker = new VmsQueryWorker(channel, index, recordIterator, payload, BUFFER_SIZE, 1000);
//        worker.run();
//
//        verify(index, never()).copyRecordToBuffer(anyLong(), any());
//
//        verify(channel, times(1)).write(any(), anyLong(), any(), any(), any());
//
//        assertEquals(1, capturedPackets.size());
//        assertEquals(QueryResultEvent.END_OF_STREAM_TYPE, capturedPackets.get(0)[0]);
//    }
//}