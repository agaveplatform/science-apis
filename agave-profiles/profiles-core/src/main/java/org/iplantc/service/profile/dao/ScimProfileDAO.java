package org.iplantc.service.profile.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.unboundid.scim2.client.requests.ModifyRequestBuilder;
import com.unboundid.scim2.client.requests.SearchRequestBuilder;
import com.unboundid.scim2.common.exceptions.BadRequestException;
import com.unboundid.scim2.common.messages.PatchOpType;
import com.unboundid.scim2.common.messages.PatchOperation;
import com.unboundid.scim2.common.types.*;
import com.unboundid.scim2.common.utils.JsonDiff;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.profile.exceptions.ProfileArgumentException;
import org.iplantc.service.profile.exceptions.ProfileException;
import org.iplantc.service.profile.util.ServiceUtils;
import org.iplantc.service.profile.Settings;
import org.iplantc.service.profile.exceptions.RemoteDataException;
import org.iplantc.service.profile.model.Profile;
import org.iplantc.service.profile.model.ScimProfile;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.unboundid.scim2.client.ScimService;
import com.unboundid.scim2.common.exceptions.ScimException;
import com.unboundid.scim2.common.GenericScimResource;
import com.unboundid.scim2.common.messages.ListResponse;
import com.unboundid.scim2.common.filters.Filter;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides a Data Access Object implementation backed by a <a href="http://www.simplecloud.info/">System for Cross-domain Identity Management (SCIM)</a> service.
 */
public class ScimProfileDAO extends AbstractProfileDAO {

	public ScimProfileDAO() {}
	
	private static final String URL_ENCODING_FORMAT = "utf-8";

	/**
	 * Handles HTTP BASIC auth to the SCIM service. We would likely switch to oauth in prod
	 */
	class HTTPBasicAuthenticator implements ClientRequestFilter {

		private final String user;
		private final String password;

		public HTTPBasicAuthenticator(String user, String password) {
			this.user = user;
			this.password = password;
		}

		public void filter(ClientRequestContext requestContext) throws IOException {
			MultivaluedMap<String, Object> headers = requestContext.getHeaders();
			final String basicAuthentication = getBasicAuthentication();
			headers.add("Authorization", basicAuthentication);
		}

		private String getBasicAuthentication() {
			String token = this.user + ":" + this.password;
			try {
				return "BASIC " + DatatypeConverter.printBase64Binary(token.getBytes("UTF-8"));
			} catch (UnsupportedEncodingException ex) {
				throw new IllegalStateException("Cannot encode with UTF-8", ex);
			}
		}
	}

	@Override
	public Profile getByUsername(String username) throws RemoteDataException
	{
		try {
			List<Profile> profiles = fetchResults(Filter.eq("userName", username), 1, 0);
			if (profiles.isEmpty()) {
				return null;
			} else {
				return profiles.get(0);
			}
		} catch (BadRequestException e) {
			throw new RemoteDataException("Invalid email address", e);
		}
	}
	
	@Override
	public List<Profile> searchByEmail(String email) throws RemoteDataException
	{
		return searchByEmail(email, Settings.DEFAULT_PAGE_SIZE, 0);
	}

	public List<Profile> searchByEmail(String email, int limit, int offset) throws RemoteDataException
	{
		try {
			return fetchResults(Filter.eq("email", email), limit, offset);
		} catch (BadRequestException e) {
			throw new RemoteDataException("Invalid email address", e);
		}
	}

	@Override
	public List<Profile> searchByFullName(String name) throws RemoteDataException
	{
		return searchByFullName(name, Settings.DEFAULT_PAGE_SIZE, 0);
	}

	public List<Profile> searchByFullName(String name, int limit, int offset) throws RemoteDataException
	{
		try {
			return fetchResults(Filter.eq("name.familyName", name), limit, offset);
		} catch (BadRequestException e) {
			throw new RemoteDataException("Invalid name search value", e);
		}
	}

	@Override
	public List<Profile> searchByUsername(String username) throws RemoteDataException
	{
		try {
			return fetchResults(Filter.sw("username", username), Settings.DEFAULT_PAGE_SIZE, 0);
		} catch (BadRequestException e) {
			throw new RemoteDataException("Invalid username search value", e);
		}
	}

	/**
	 * Makes API call to SCIM API for profile info.
	 * @param filter the filter to apply to the query
	 * @param offset the number of results to skip before returing the following {@code limit} results
	 * @param limit the max number of results to return
	 * @return a list of Profile objects mapped from the query response of the SCIM2 service
	 * @throws RemoteDataException if the service is unavailable.
	 */
	private List<Profile> fetchResults(Filter filter, int limit, int offset) throws RemoteDataException
	{
		limit = Math.min(Math.max(0, limit), Settings.MAX_PAGE_SIZE);
		offset = Math.max(1, offset+1);

		List<Profile> profiles = new ArrayList<Profile>();

		try {
			// Create a ScimService using the credentials in the service config
			ScimService scimService = getScimService();

			SearchRequestBuilder builder = scimService.searchRequest("Users").page(offset, limit);
			if (filter != null) {
				builder.filter(filter.toString());
			}

			ListResponse<UserResource> searchResponse = builder.invoke(UserResource.class);

			return searchResponse
					.getResources()
					.stream()
					.map(ScimProfile::new)
					.collect(Collectors.toList());
		} 
		catch (Exception e) {
			throw new RemoteDataException("Failed to retrieve profile data", e);
		}
	}

	/**
	 * Makes API call to SCIM API to update user profile
	 * @param username the username of the user to update
	 * @param updatedProfile the profile object to use to update the SCIM service.
	 * @return a Profile object mapped from the query response of the SCIM2 service
	 * @throws RemoteDataException if the service is unavailable or update fails
	 * @throws ProfileException if the updated Profile is not valid.
	 */
	private Profile update(String username, ScimProfile updatedProfile) throws RemoteDataException, ProfileException
	{
		try {
			// Create a ScimService using the credentials in the service config
			ScimService scimService = getScimService();

			ListResponse<UserResource> searchResponse = scimService.searchRequest("Users")
					.filter(Filter.eq("userName", username).toString())
					.page(1, 1)
					.invoke(UserResource.class);

			if (searchResponse.getTotalResults() != 0) {
				throw new RemoteDataException("Failed to update user profile. Multiple users found with username " + username);
			}

			// fetch the current profile so we have a deterministic way to update values when value mapping on 1-N
			// fields is not deterministic.
			UserResource existingResource = mergeExistingUserResource(searchResponse.getResources().get(0), updatedProfile);

			UserResource updatedResource = scimService.replace(existingResource);
			return new ScimProfile(updatedResource);
		}
		catch (RemoteDataException|ProfileException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RemoteDataException("Failed to update profile data", e);
		}
	}

	/**
	 * Handles the updating of an existing SCIM {@link UserResource} with the values from an Agave {@link ScimProfile}.
	 * Ordering and formatting are preserved, along with all existing metadata. Note that this operation does not
	 * implement PATCH behavior. Any nulled out or missing values from the updatedProfile will be removed from the
	 * {@code existingResource}.
	 *
	 * @param existingResource the remote SCIM resource to udpate
	 * @param updatedProfile the user-supplied values to apply to the {@code existingResource}
	 * @return the {@code existingResource} updated with values from {@code updatedProfile}
	 * @throws ProfileException if any of the updatedProfile values are invalid.
	 * @throws IOException if unable to serialized the {@link UserResource} or {@link ScimProfile}.
	 */
	protected UserResource mergeExistingUserResource(UserResource existingResource, ScimProfile updatedProfile) throws ProfileException, IOException {
		// convert to a ScimProfile for apples-to-apples comparison with the updated profile requested by the
		// api user.
		ScimProfile existingProfile = new ScimProfile(existingResource);

		// now we can walk through the fields that differ between the existing and updated ScimProfile objects
		// and adjust the existing UserResource by matching values. We then just post back the UserResource
		// to the SCIM service, thereby preserving all non-mapped fields and ordering.
		ObjectMapper mapper = new ObjectMapper();
		JsonDiff jsonDiff = new JsonDiff();
		List<PatchOperation> patches = jsonDiff.diff(
				(ObjectNode) mapper.readTree(existingProfile.toJSON()),
				(ObjectNode) mapper.readTree(updatedProfile.toJSON()),
				true);

		for (PatchOperation patchOperation: patches) {
			// username is not changeable
			switch (patchOperation.getPath().toString()) {
				case "lastName":
					if (existingResource.getName() == null) {
						existingResource.setName(new Name());
					}
					existingResource.getName().setFamilyName(updatedProfile.getLastName());
					break;
				case "firstName":
					if (existingResource.getName() == null) {
						existingResource.setName(new Name());
					}
					existingResource.getName().setGivenName(updatedProfile.getFirstName());
					break;
				case "email":
					if (patchOperation.getOpType() == PatchOpType.REMOVE) {
						throw new ProfileException("Email address is required.");
					}
				case "position":
					existingResource.setTitle(updatedProfile.getPosition());
					break;
				case "active":
					existingResource.setActive(updatedProfile.isActive());
					break;
				case "mobilePhone":
					if (patchOperation.getOpType() == PatchOpType.ADD) {
						existingResource.getPhoneNumbers().add(new PhoneNumber()
								.setType("mobile")
								.setValue(ServiceUtils.formatPhoneNumber(updatedProfile.getMobilePhone())));
					} else {
						// look for an existing phone number of type mobile. notice that if there are multiple mobile numbers,
						// this approach will fail
						for (int i = 0; i < existingResource.getPhoneNumbers().size(); i++) {
//								PhoneNumber phoneNumberItem = existingResource.getPhoneNumbers().get(i);
							if (StringUtils.equalsIgnoreCase(existingResource.getPhoneNumbers().get(i).getType(), "mobile")) {
								// if we're deleting the node, get that out of the way here
								if (patchOperation.getOpType() == PatchOpType.REMOVE) {
									existingResource.getPhoneNumbers().remove(i);
								} else {
									// otherwise, update the existing value. formatting is
									existingResource.getPhoneNumbers().get(i).setValue(
											ServiceUtils.formatPhoneNumber(updatedProfile.getMobilePhone(), "%s-%s-%s"));
								}
								break;
							}
						}
					}
					break;
				case "fax":
					if (patchOperation.getOpType() == PatchOpType.ADD) {
						existingResource.getPhoneNumbers().add(new PhoneNumber()
								.setType("fax")
								.setValue(ServiceUtils.formatPhoneNumber(updatedProfile.getFax())));
					} else {
						// look for an existing phone number of type mobile. notice that if there are multiple mobile numbers,
						// this approach will fail
						for (int i = 0; i < existingResource.getPhoneNumbers().size(); i++) {
							if (StringUtils.equalsIgnoreCase(existingResource.getPhoneNumbers().get(i).getType(), "fax")) {
								// if we're deleting the node, get that out of the way here
								if (patchOperation.getOpType() == PatchOpType.REMOVE) {
									existingResource.getPhoneNumbers().remove(i);
								} else {
									// otherwise, update the existing value. formatting is
									existingResource.getPhoneNumbers().get(i).setValue(
											ServiceUtils.formatPhoneNumber(updatedProfile.getFax(), "%s-%s-%s"));
								}
								break;
							}
						}
					}
					break;
				case "phone":
					if (patchOperation.getOpType() == PatchOpType.ADD) {
						existingResource.getPhoneNumbers().add(new PhoneNumber()
								.setType("home")
								.setValue(ServiceUtils.formatPhoneNumber(updatedProfile.getPhone())));
					} else {
						// format the phone number so we can properly compare them
						String existingPhone = ServiceUtils.formatPhoneNumber(existingProfile.getPhone(), "%s-%s-%s");
						if (patchOperation.getOpType() == PatchOpType.REMOVE) {
							for (int i = 0; i < existingResource.getPhoneNumbers().size(); i++) {
								// don't get confused with mobile and fax numbers
								if (List.of("mobile", "fax").contains(existingResource.getPhoneNumbers().get(i).getType().toLowerCase()))
									continue;

								String phoneA = ServiceUtils.formatPhoneNumber(existingResource.getPhoneNumbers().get(i).getValue(), "%s-%s-%s");
								if (StringUtils.equals(existingPhone, phoneA)) {
									existingResource.getPhoneNumbers().remove(i);
									break;
								}
							}
						} else {
							// look for an existing phone number without type of fax or mobile. notice that
							// if there are multiple matches, only the first will ever be udpated.
							for (int i = 0; i < existingResource.getPhoneNumbers().size(); i++) {
								// don't get confused with mobile and fax numbers
								if (List.of("mobile", "fax").contains(existingResource.getPhoneNumbers().get(i).getType().toLowerCase()))
									continue;

								String phoneA = ServiceUtils.formatPhoneNumber(existingResource.getPhoneNumbers().get(i).getValue(), "%s-%s-%s");
								if (StringUtils.equals(existingPhone, phoneA)) {
									// otherwise, update the existing value. formatting is
									existingResource.getPhoneNumbers().get(i).setValue(existingPhone);
									break;
								}
							}
						}
					}
					break;
				case "institution":
					EnterpriseUserExtension enterpriseExtension = existingResource.getExtension(EnterpriseUserExtension.class);
					if (enterpriseExtension != null) {
						enterpriseExtension.setOrganization(updatedProfile.getInstitution());
						existingResource.setExtension(enterpriseExtension);
					}
					break;
			}
		}

		return existingResource;
	}

	/**
	 * Creates a new user profile in the remote identity store by calling the SCIM service.
	 * @param profile a {@link ScimProfile} representing the user to add
	 * @return the saved user
	 * @throws RemoteDataException if the call fails.
	 */
	public Profile create(ScimProfile profile) throws RemoteDataException {
		try {
			// Create a ScimService using the credentials in the service config
			ScimService scimService = getScimService();

			// Convert the new user profile to a UserResource and post to the SCIM service.
			UserResource scimUser = scimService.create("Users", profile.toUserResource());

			// Transfrom the response back to an ScimUser for sending back in the response.
			return new ScimProfile(scimUser);
		}
		catch (ScimException e) {
			throw new RemoteDataException("Error occurred calling the remote service: " + e.getMessage(), e);
		} catch (Exception e) {
			throw new RemoteDataException("Failed to save the user profile. " + e.getMessage(), e);
		}
	}

	/**
	 * Creates a new jax-rs client pointing at the SCIM service configured in the service settings.
	 * @return an instance of a ScimService pointing to the desired target.
	 */
	protected ScimService getScimService() {
		Client client = ClientBuilder.newClient().register(new HTTPBasicAuthenticator(Settings.QUERY_URL_USERNAME, Settings.QUERY_URL_PASSWORD));
		WebTarget target = client.target(Settings.QUERY_URL);
		return new ScimService(target);
	}
}
