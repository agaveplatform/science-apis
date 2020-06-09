package org.iplantc.service.profile.dao;

import org.iplantc.service.profile.model.enumeration.ProfileType;

/**
 * Simple factory to obtain a {@link ProfileDAO} instance to query for user info.
 */
public class ProfileDAOFactory {

	/**
	 * Returns factory for the given {@code profileType}
	 * @param profileType the identity source for which to obtain a {@link ProfileDAO}.
	 * @return data access object for the given data source type.
	 */
	public ProfileDAO getProfileDAO(ProfileType profileType) {
		ProfileDAO dao = null;
		switch (profileType) {
			case TRELLIS:
				dao = new TrellisProfileDAO();
				break;
			case DB:
				dao = new DatabaseProfileDAO();
				break;
			case SCIM:
				dao = new ScimProfileDAO();
				break;
			default:
				dao = new LDAPProfileDAO();
		}

		return dao;
	}
}
