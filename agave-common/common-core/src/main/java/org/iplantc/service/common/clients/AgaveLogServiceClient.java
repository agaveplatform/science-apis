/**
 * 
 */
package org.iplantc.service.common.clients;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author dooley
 * @deprecated
 */
@Deprecated
public class AgaveLogServiceClient {
	
	private static final Logger logger = LogManager.getLogger(AgaveLogServiceClient.class);
	
	public static enum ServiceKeys { 
		APPS02, AUTH02, TRANSFORMS02, FILES02, JOBS02, PROFILES02, NOTIFICATIONS02, SYSTEMS02, METADATA02, POSTITS02, USAGE02, TRANSFERS02, MYPROXY02, MONITORS02, CLIENTS02, TAGS02, REALTIME02, UUID02, REACTORS02, ABACO02, REPOSITORIES02
	};
	
	public static enum ActivityKeys 
	{
		AppsDelete, AppsForm, AppsGetByID, AppsList, AppsListPublic, AppsListShared, AppsAdd, AppsSearchPublicByName, AppsSearchPublicByTag, AppsSearchPublicByTerm, AppsSearchSharedByName, AppsSearchSharedByTag, AppsSearchSharedByTerm, AppsUsage, AppsListPermissions, AppsPublish, AppsClone, AppsClonePublic, AppsClonePrivate, AppsSearchPublicBySystem, AppsUpdatePermissions, AppsRemovePermissions, AppsIdLookup, AppsErase, AppsUpdate, AppsEnable, AppsDisable, AppsHistoryList, 
		AuthCreate, AuthList, AuthRenew, AuthRevoke, AuthVerify,
		DataImpliedExport, DataImpliedTransform, DataList, DataSearchByFile, DataSearchByName, DataSearchByTag, DataSpecifiedExport, DataSpecifiedTransform, DataViewCloud, 
		IOPublicDownload, IOMove, IOMakeDir, IOCopy, IODelete, IODownload, IOExport, IOImport, IOList, IORename, IOShare, IOUpload, IOUsage, FilesGetHistory, IOPemsUpdate, IOPemsList, IOPemsDelete, IOIndex,  
		JobsDelete, JobsGetByID, JobsGetOutput, JobsKill, JobsList, JobsListInputs, JobsResubmit, JobsListOutputs, JobsShare, JobsSubmit, JobsGetInput, JobsUsage, JobStatus, JobSearch, JobAttributeList, JobsGetHistory, JobSubmissionForm, JobsCustomRuntimeEvent, JobReset, JobRestore, 
		ProfileSearchEmail, ProfileSearchName, ProfileSearchUsername, ProfileUsage, ProfileUsername, 
		InternalUsersList, InternalUsersRegistration, InternalUserDelete, InternalUserUpdate, InternalUserGet, InternalUserSearchName, InternalUserSearchEmail, InternalUserSearchUsername, InternalUserSearchStatus, InternalUserClear,
		SystemGetCredentials, SystemAddCredential, SystemRemoveCredential, SystemsListType, SystemsListAll, SystemsSearch, SystemsGetByID, SystemsAdd, SystemsUpdate, SystemsDelete, SystemsPublish, SystemsUnpublish, SystemsClone, SystemsSetDefault, SystemsUnsetDefault, SystemsSetGlobalDefault, SystemsUnsetGlobalDefault, SystemListRoles, SystemEditRoles, SystemRemoveRoles, SystemsDefaultListPublic, SystemsErase, SystemBatchQueueList, SystemBatchQueueUpdate, SystemBatchQueueDelete, SystemBatchQueueAdd, SystemsDisable, SystemsEnable, SystemHistoryGet, SystemHistoryList, SystemHistoryGetById,
		UsageList, UsageSearch, UsageAddTrigger, UsageDeleteTrigger, UsageUpdateTrigger,
		MetaList, MetaListRelated, MetaGetById, MetaSearch, MetaCreate, MetaDelete, MetaEdit, MetaPemsCreate, MetaPemsAdd, MetaPemsUpdate, MetaPemsDelete, MetaPemsList,
		SchemaList, SchemaListRelated, SchemaGetById, SchemaSearch, SchemaCreate, SchemaDelete, SchemaEdit, SchemaPemsCreate, SchemaPemsAdd, SchemaPemsUpdate, SchemaPemsDelete, SchemaPemsList,
		NotifList, NotifAdd, NotifUpdate, NotifDelete, NotifTrigger, NotifListRelated, NotifGetById, NotifSearch, NotifAttemptSearch, NotifAttemptList, NotifAttemptClear, NotifAttemptDelete, NotifAttemptDetails,
		PostItsList, PostItsDelete, PostItsRedeem, PostItsAdd,
		TagsAdd, TagsUpdate, TagsDelete, TagsGetByID, TagsList, TagPermissionAdd, TagPermissionUpdate, TagPermissionDelete, TagPermissionGetByUsername, TagPermissionsList, TagResourceAdd, TagResourceUpdate, TagResourceDelete, TagResourceGetById, TagResourcesList, TagsUsage, TagsSearch, TagsHistoryList,
		TransfersList, TransfersAdd, TransfersUpdate, TransfersDelete, TransfersStop, TransfersSearch, TransfersGetById, TransfersHistoryGet, TransfersHistoryList,
		TriggerListAll, TriggerCreate, TriggerList, TriggerDelete,
		MyProxyList, MyProxyStore, MyProxyGetByName, MyProxyDelete,
		MonitorGetById, MonitorsList, MonitorAdd, MonitorDelete, MonitorUpdate, MonitorChecksList, MonitorCheckGetById, MonitorTrigger, MonitorHistoryGet, MonitorHistoryList, MonitorHistoryGetById,
		ClientsList, ClientsDelete, ClientsUpdate, ClientsAdd, ClientsApiList, ClientsApiSubscribe, ClientsApiDelete,
		UuidLookup, UuidGen,
		
	};

	/**
	 * Call to agave's internal log service to track requests. This is deprecated in favor of an infrastructure level
	 * solution
	 * @param serviceKey the api being called
	 * @param activityKey the key representing the request type being made
	 * @param username the user making the call
	 * @param activityContext the context of the activity
	 * @param ipAddress the caller ip address
	 */
	public static void log(String serviceKey, String activityKey, String username, String activityContext, String ipAddress) 
	{
//		DataOutputStream wr = null;
//		HttpURLConnection connection = null;
//		try
//		{
//			logger.info("Calling  " + serviceKey + ", " + username + ", " + activityKey + ", " + activityContext + ", " + ipAddress + ", " + TenancyHelper.getCurrentTenantId());
//
//			// Construct data
//		    String urlParameters = "servicekey=" + URLEncoder.encode(serviceKey, "UTF-8");
//		    urlParameters += "&activitykey=" + URLEncoder.encode(activityKey, "UTF-8");
//		    urlParameters += "&username=" + URLEncoder.encode(username, "UTF-8");
//		    urlParameters += "&activitycontext=" + URLEncoder.encode(activityContext, "UTF-8");
//		    urlParameters += "&userip=" + URLEncoder.encode(ipAddress, "UTF-8");
//		    urlParameters += "&clientId=" + URLEncoder.encode(TenancyHelper.getCurrentApplicationId(), "UTF-8");
//		    urlParameters += "&tenantId=" + URLEncoder.encode(TenancyHelper.getCurrentTenantId(), "UTF-8");
//
//		    // Send data
//
//		    URL url = new URL(Settings.IPLANT_LOG_SERVICE + serviceKey);
//
//		    connection = (HttpURLConnection)url.openConnection();
//		    connection.setRequestMethod("POST");
//		    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
//		    connection.setRequestProperty("Content-Length", "" + Integer.toString(urlParameters.getBytes().length));
//		    connection.setRequestProperty("Content-Language", "en-US");
//			connection.setUseCaches (false);
//			connection.setDoOutput(true);
//			connection.setDoInput(true);
//
//			try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
//				wr.writeBytes(urlParameters);
//				wr.flush();
//			}
//
//		    //Get Response
////		    try (InputStream is = connection.getInputStream()) {
////				BufferedReader rd = new BufferedReader(new InputStreamReader(is));
////				String line;
////				StringBuffer response = new StringBuffer();
////				while ((line = rd.readLine()) != null) {
////					response.append(line);
////					response.append('\r');
////				}
////				rd.close();
////
////			}
//		}
//		catch (Exception e) {
//			logger.debug("Failed to call remote logging service: " + e.getMessage());
//		}
//		finally {
////			try { if (wr != null) wr.close(); } catch (Throwable ignored) {}
//			try { if (connection != null) connection.disconnect(); } catch (Throwable ignored) {}
//		}
	}
}


