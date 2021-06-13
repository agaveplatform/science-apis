package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED_ACK;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_ALL;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfer All tests")
class TransferAllProtocolVerticalIT extends BaseTestCase {

	@AfterEach
	protected void afterEach() {
		SystemDao systemDao = new SystemDao();
		systemDao.getAll().forEach(remoteSystem -> {try {systemDao.remove(remoteSystem);} catch (Exception ignored){}});
	}

	TransferAllProtocolVertical getMockAllProtocolVerticalInstance(Vertx vertx) {
		TransferAllProtocolVertical listener = Mockito.mock(TransferAllProtocolVertical.class);
		when(listener.getEventChannel()).thenReturn(TRANSFER_ALL);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.getExecutor()).thenCallRealMethod();
		when(listener.getCancelledTasks()).thenCallRealMethod();
		doCallRealMethod().when(listener).setCancelledTasks(any());
		when(listener.getPausedTasks()).thenCallRealMethod();
		doCallRealMethod().when(listener).setPausedTasks(any());

		// have to set them here because they won't be initialized when the mock is created.
		listener.setCancelledTasks(new ConcurrentHashSet<String>());
		listener.setPausedTasks(new ConcurrentHashSet<String>());

		return listener;
	}

	URLCopy getMockUrlCopyInstance(TransferTask transferTask) throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException {
		URLCopy urlCopy = Mockito.mock(URLCopy.class);
		doCallRealMethod().when(urlCopy).copy(any());

		return urlCopy;
	}

	RemoteDataClient getRemoteDataClientInstance() {
		return Mockito.mock(RemoteDataClient.class);
	}

	/**
	 * Reads the json defintion of a {@link StorageSystem} from disk
	 * and returns as JSONObject.
	 *
	 * @param protocolType the protocol of the remote system. One sytem definition exists for each {@link StorageProtocolType}
	 * @return the json representation of a storage system
	 */
	protected JSONObject getSystemJson(StorageProtocolType protocolType) {
		try {
			String systemConfigFilename = String.format("%s/%s.example.com.json",
					STORAGE_SYSTEM_TEMPLATE_DIR, protocolType.name().toLowerCase());

			Path p = Paths.get(systemConfigFilename);
			return new JSONObject(new String(Files.readAllBytes(p)));
		} catch (JSONException|IOException e) {
			fail("Unable to read system json definition");
			return null;
		}
	}

	/**
	 * Parses the json definition of a system and applies a unique id, name, and host path.
	 *
	 * @param protocolType the protocol of the remote system. One sytem definition exists for each {@link StorageProtocolType}
	 * @return system
	 */
	protected RemoteSystem getTestSystem(StorageProtocolType protocolType) {
		try {
			JSONObject json = getSystemJson(protocolType);
			json.remove("id");
			json.put("id", UUID.randomUUID().toString());
			RemoteSystem system = StorageSystem.fromJSON(json);
			system.setOwner(SYSTEM_USER);
			Optional<String> hd = Optional.ofNullable(system.getStorageConfig().getHomeDir());
			String homeDir = String.format("%s/%s/%s",
					hd.orElse(""),
					getClass().getSimpleName(),
					UUID.randomUUID());
			system.getStorageConfig().setHomeDir(homeDir);
			return system;
		} catch (Exception e) {
			fail("Failed to parse test system");
			return null;
		}
	}

	@Test
	@DisplayName("Test actual data transfer")
	@Timeout(value=30, timeUnit=TimeUnit.SECONDS)
	public void testProcessCopyRequest (Vertx vertx, VertxTestContext ctx) throws SystemUnknownException, AgaveNamespaceException, RemoteCredentialException,
			PermissionException, IOException, RemoteDataException, TransferException, RemoteDataSyntaxException {

		RemoteSystem destSystem = getTestSystem(StorageProtocolType.SFTP);
		// save the system so it can be referenced in the transfer protocol vertical
		SystemDao systemDao = new SystemDao();
		systemDao.persist(destSystem);

		RemoteDataClient destRemoteDataClient = destSystem.getRemoteDataClient();
		// generate a uuid to use as the directory name to which the data will be copied.
		String destAbsolutePath = destRemoteDataClient.resolvePath(UUID.randomUUID().toString());
		URI destUri = URI.create(String.format("agave://%s/%s", destSystem.getSystemId(), destAbsolutePath));

		URI srcUri = URI.create(TRANSFER_SRC);
		RemoteDataClient srcRemoteDataClient =
				new RemoteDataClientFactory().getInstance(TEST_USERNAME, null, srcUri);

		// pull in the mock for TransferAllProtocolVertical
		TransferAllProtocolVertical txfrAllVert = getMockAllProtocolVerticalInstance(vertx);
		// ensure we're doing live copies
		when(txfrAllVert.getRemoteDataClient(anyString(), anyString(), any())).thenCallRealMethod();
		when(txfrAllVert.getUrlCopy(any(), any())).thenCallRealMethod();

		RetryRequestManager retryRequestManager = mock(RetryRequestManager.class);
		doNothing().when(retryRequestManager).request(any(), any(), any());
		when(txfrAllVert.getRetryRequestManager()).thenReturn(retryRequestManager);

		doNothing().when(txfrAllVert)._doPublishEvent(any(), any(JsonObject.class));

		// make the actual call to our method under test
		doCallRealMethod().when(txfrAllVert).processCopyRequest(any(), any(), any(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		TransferTask tt =  new TransferTask(TENANT_ID,
				srcUri.toString(),
				destUri.toString(),
				TEST_USER,
				null,
				null);
		tt.setId(1L);

		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject =  tt.toJson()
//				.put("status", TransferStatusType.FAILED.name())
//				.put("endTime", Instant.now());

//		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			//handler.handle(expectedUpdateStatusHandler);
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), eq(tt.getStatus().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrAllVert.getDbService()).thenReturn(dbService);

		// now actually call the mehtod under test
		txfrAllVert.processCopyRequest(srcRemoteDataClient, destRemoteDataClient, tt, resp -> {
			ctx.verify(() -> {

				assertTrue(resp.succeeded(), "operation should succeed");

				assertTrue(resp.result(), "Result of valid copy should be true");

				// this shouldn't be called because we're passing in the src rdc
				verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, srcUri);

				// this shouldn't be called because we're passing in the dest rdc
				verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, destUri);

				verify(txfrAllVert).getUrlCopy(srcRemoteDataClient, destRemoteDataClient);

				verify(txfrAllVert)._doPublishEvent(eq(MessageType.TRANSFER_COMPLETED), anyObject());

				// this should be called as the method get

				systemDao.remove(destSystem);

				ctx.completeNow();
			});
		});
    }

    @Test
    @DisplayName("Test cancel request during processing...")
    public void testProcessCancelSync(Vertx vertx, VertxTestContext ctx) throws RemoteDataException, RemoteCredentialException, IOException, SystemUnknownException, AgaveNamespaceException, PermissionException, TransferException, RemoteDataSyntaxException {
        RemoteSystem destSystem = getTestSystem(StorageProtocolType.SFTP);
        // save the system so it can be referenced in the transfer protocol vertical
        SystemDao systemDao = new SystemDao();
        systemDao.persist(destSystem);

        RemoteDataClient destRemoteDataClient = destSystem.getRemoteDataClient();
        // generate a uuid to use as the directory name to which the data will be copied.
        String destAbsolutePath = destRemoteDataClient.resolvePath(UUID.randomUUID().toString());
        URI destUri = URI.create(String.format("agave://%s/%s", destSystem.getSystemId(), destAbsolutePath));

        URI srcUri = URI.create(TRANSFER_SRC);
        RemoteDataClient srcRemoteDataClient =
                new RemoteDataClientFactory().getInstance(TEST_USERNAME, null, srcUri);

        // pull in the mock for TransferAllProtocolVertical
        TransferAllProtocolVertical txfrAllVert = getMockAllProtocolVerticalInstance(vertx);
        when(txfrAllVert.getRemoteDataClient(anyString(), anyString(), any())).thenReturn(srcRemoteDataClient, destRemoteDataClient);
        when(txfrAllVert.getUrlCopy(any(), any())).thenCallRealMethod();
		doNothing().when(txfrAllVert)._doPublishEvent(any(), any(JsonObject.class));

        // make the actual call to our method under test
        doCallRealMethod().when(txfrAllVert).processEvent(any(), any());

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        TransferTask tt = new TransferTask(TENANT_ID,
				srcUri.toString(),
                destUri.toString(),
                TEST_USER,
                null,
                null);
        tt.setId(1L);

        // mock a cancelled json transfer task result from getById for root task
		JsonObject expectedUpdatedJsonObject =  tt.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());

		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUpdatedJsonObject);

        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
            Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
            handler.handle(expectedUpdateStatusHandler);
            return null;
        }).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject());

        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(txfrAllVert.getDbService()).thenReturn(dbService);

		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(Future.succeededFuture(true));
			return null;
		}).when(txfrAllVert).processCopyRequest(any(), any(), any(), any());

        // now actually call the method under test
        txfrAllVert.processEvent(tt.toJson(), process->{
            assertTrue(process.succeeded(), "Process event should return success on cancel process.");
        });

        ctx.verify(() -> {
            // this shouldn't be called because we're passing in the src rdc
            verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, srcUri);
            // this shouldn't be called because we're passing in the dest rdc
            verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, destUri);

			verify(txfrAllVert, never())._doPublishEvent(eq(MessageType.TRANSFERTASK_ERROR), any(JsonObject.class));

            verify(txfrAllVert, times(1))._doPublishEvent(TRANSFERTASK_CANCELED_ACK, tt.toJson());

            ctx.completeNow();
        });
    }

}