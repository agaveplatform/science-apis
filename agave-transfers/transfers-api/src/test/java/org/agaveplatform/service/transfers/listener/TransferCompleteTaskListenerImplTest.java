package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseServiceVertxEBProxy;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers completed task listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
class TransferCompleteTaskListenerImplTest {
    public static final String TENANT_ID = "agave.dev";
    public static final String TRANSFER_SRC = "http://foo.bar/cat";
    public static final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";

    @Mock
    Logger logger;
    @Mock
    List<String> parrentList;
    @Mock
    JDBCClient jdbc;
    @Mock
    JsonObject config;
    @Mock
    TransferTaskDatabaseService dbService;
    @Mock
    Vertx vertx;
    @Mock
    Context context;
    @InjectMocks
    TransferCompleteTaskListenerImpl transferCompleteTaskListenerImpl;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

//    @Test
//    void testStart() {
//        when(dbService.getById(anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(null, "address", null));
//        when(dbService.allChildrenCancelledOrCompleted(anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(null, "address", null));
//        when(dbService.updateStatus(anyString(), anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(null, "address", null));
//        when(dbService.createProxy(any(), anyString())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(null, "address", null));
//
//        transferCompleteTaskListenerImpl.start();
//    }

    @Test
   //@Disabled
    void testProcessEvent(Vertx vertx, VertxTestContext ctx) {
        when(dbService.getById(anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(vertx, "address", null));
        when(dbService.allChildrenCancelledOrCompleted(anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(vertx, "address", null));
        when(dbService.updateStatus(anyString(), anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(vertx, "address", null));

        JsonObject transferTask = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TENANT_ID)
                .toJson()
                .put("status", TransferStatusType.TRANSFERRING)
                .put("created", Instant.now().toEpochMilli())
                .put("start_time", Instant.now().toEpochMilli())
                .put("tenant_id", "agave.dev");

        Future<JsonObject> result = transferCompleteTaskListenerImpl.processEvent(transferTask);
        Assertions.assertEquals(TransferStatusType.COMPLETED.name(), result.result().getString("status"),
                "TransferTask status should be completed after processing transfertask.completed event for the task");
        ctx.completeNow();
    }

    @Test
    //@Disabled
    void testProcessParentEvent(Vertx vertx, VertxTestContext ctx) {
        when(dbService.getById(anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(vertx, "address", null));
        when(dbService.allChildrenCancelledOrCompleted(anyString(), anyString(), any())).thenReturn(new TransferTaskDatabaseServiceVertxEBProxy(vertx, "address", null));

        Future<JsonObject> result = transferCompleteTaskListenerImpl.processParentEvent(TENANT_ID, new AgaveUUID(UUIDType.TRANSFER).toString());

        Assertions.assertEquals(null, result);
        ctx.completeNow();
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme