package org.iplantc.service.profile.model;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.unboundid.scim2.common.types.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;

@XStreamAlias("profile")
public class ScimProfile extends Profile {
	private boolean active = false;
	private String password;
	private String mobilePhone;
	public ScimProfile(UserResource userResource) {
		this.username = userResource.getUserName();
		this.firstName = userResource.getName().getGivenName();
		this.lastName = userResource.getName().getFamilyName();
		this.email = userResource.getEmails().get(0).getValue();
		this.position = userResource.getTitle();
		this.active = userResource.getActive();

		for(PhoneNumber phoneNumber: userResource.getPhoneNumbers()) {
			if (phoneNumber.getType().equalsIgnoreCase("fax")) {
				this.fax = phoneNumber.getValue();
			} else if (phoneNumber.getType().equalsIgnoreCase("mobile")) {
				this.mobilePhone = phoneNumber.getValue();
			} else if (phoneNumber.getPrimary()) {
				this.phone = phoneNumber.getValue();
			} else if (StringUtils.isEmpty(this.phone)) {
				this.phone = phoneNumber.getValue();
			}
		}

		EnterpriseUserExtension enterpriseExtension = userResource.getExtension(EnterpriseUserExtension.class);
		if (enterpriseExtension != null) {
			this.institution = enterpriseExtension.getOrganization();
		}
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getMobilePhone() {
		return mobilePhone;
	}

	public void setMobilePhone(String mobilePhone) {
		this.mobilePhone = mobilePhone;
	}

	public UserResource toUserResource() {
		UserResource user = new UserResource();
		user.setUserName(getUsername());
		user.setPassword(getPassword());
		Name name = new Name()
				.setGivenName(getFirstName())
				.setFamilyName(getLastName());
		user.setName(name);
		user.setTitle(getPosition());
		user.setActive(isActive());
		Email email = new Email()
				.setType("home")
				.setPrimary(true)
				.setValue("babs@example.com");
		user.setEmails(Collections.singletonList(email));
		return user;
	}
}
