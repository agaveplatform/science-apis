package org.iplantc.service.monitor.search;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.iplantc.service.common.search.AgaveResourceSearchFilter;
import org.iplantc.service.common.util.StringToTime;
import org.iplantc.service.monitor.model.enumeration.*;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is the basis for search support across the API.
 * Each service should implement this class to filter query parameters
 * from the url into valid hibernate query.
 * 
 * @author dooley
 *
 */
public class MonitorSearchFilter extends AgaveResourceSearchFilter
{
	private static HashMap<String, String> searchTermMappings = new HashMap<String,String>();

	@SuppressWarnings("rawtypes")
	private static HashMap<String, Class> searchTypeMappings = new HashMap<String,Class>();

	public MonitorSearchFilter() {}

	public Set<String> getSearchParameters()
	{
		return getSearchTermMappings().keySet();
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.search.AgaveResourceSearchFilter#getSearchTermMappings()
	 */
	@Override
	public Map<String, String> getSearchTermMappings()
	{
		if (searchTermMappings.isEmpty()) {
            searchTermMappings.put("active", "%sactive");
            searchTermMappings.put("created", "%screated");
            searchTermMappings.put("frequency", "%sfrequency");
            searchTermMappings.put("id", "%suuid");
            searchTermMappings.put("internalusername", "%sinternalUsername");
            searchTermMappings.put("lastsuccess", "%slastSuccess");
            searchTermMappings.put("lastupdated", "%slastUpdated");
            searchTermMappings.put("nextupdate", "%snextUpdateTime");
            searchTermMappings.put("owner", "%sowner");
            searchTermMappings.put("target", "%ssystem.systemId");
            searchTermMappings.put("updatesystemstatus", "%supdateSystemStatus");
		}
		
		return searchTermMappings;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
    public Map<String, Class> getSearchTypeMappings()
	{
	    if (searchTypeMappings.isEmpty()) {
            searchTypeMappings.put("active", Boolean.class);
            searchTypeMappings.put("created", Date.class);
            searchTypeMappings.put("frequency", Integer.class);
            searchTypeMappings.put("id", String.class);
            searchTypeMappings.put("internalusername", String.class);
            searchTypeMappings.put("lastsuccess", Date.class);
            searchTypeMappings.put("lastupdated", Date.class);
            searchTypeMappings.put("nextupdate", Date.class);
            searchTypeMappings.put("owner", String.class);
            searchTypeMappings.put("target", String.class);
            searchTypeMappings.put("updatesystemstatus", Boolean.class);
		}
		
		return searchTypeMappings;

	}
	
	/**
	 * Validates an individual search value against the type defined by the field mapping.
	 *  
	 * @param searchTermType
	 * @param searchField
	 * @param searchValue
	 * @return 
	 * @throws IllegalArgumentException
	 */
	public Object strongTypeSearchValue(Class searchTermType, String searchField, String searchValue) 
	throws IllegalArgumentException
	{
		if (searchTermType == Date.class) {
            Object time = StringToTime.date(searchValue);
            if (Boolean.FALSE.equals(time)) {
                if (NumberUtils.isDigits(searchValue)) {
                    try {
                        DateTime dateTime = new DateTime(Long.valueOf(searchValue));
                        return dateTime.toDate();
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Illegal date format for " + searchField);
                    }
                } else {
                    try {
                        DateTime dateTime = new DateTime(searchValue);
                        return dateTime.toDate();
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Illegal date format for " + searchField);
                    }
                }
            } else {
                return time;
            }
        } else if (searchTermType == Long.class) {
            if (NumberUtils.isNumber(searchValue)) {
                return NumberUtils.toLong(searchValue);
            } else {
                throw new IllegalArgumentException("Illegal integer value for " + searchField);
            }
        } else if (searchTermType == Integer.class) {
            if (NumberUtils.isNumber(searchValue)) {
                return NumberUtils.toInt(searchValue);
            } else {
                throw new IllegalArgumentException("Illegal integer value for " + searchField);
            }
        } else if (searchTermType == Double.class) {
            if (NumberUtils.isNumber(searchValue)) {
                return NumberUtils.toDouble(searchValue);
            } else {
                throw new IllegalArgumentException("Illegal decimal value for " + searchField);
            }
        } else if (searchTermType == Boolean.class) {
            if (NumberUtils.isNumber(searchValue)) {
            	return NumberUtils.toInt(searchValue, 0) == 1;
            } 
            else {
            	return BooleanUtils.toBoolean(searchValue);
            }
        } else {
            return searchValue;
        }
	}

    @Override
    protected String getSearchTermPrefix() {
        return "m.";
    }
}

