/**
 * 
 */
package org.iplantc.service.io.util;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.log4j.Logger;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uri.UrlPathEscaper;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.systems.util.ApiUriUtil;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author dooley
 *
 */
public class ServiceUtils {
	
	private static final Logger log = Logger.getLogger(ServiceUtils.class);

	@SuppressWarnings("unused")
	public static String exec(String command) throws IOException {
		String output = "";
		
		InputStream in = Runtime.getRuntime().exec(command).getInputStream();
		
		byte[] b = new byte[512];
		int len = 0;
		while ((len = in.read(b)) >= 0) {
			output += new String(b).trim();
		}
		
		return output;
	}

	
	public static boolean isValid(String value) {
		return (value != null) && !value.equals("");
	}
	
	public static boolean isValid(File value) {
		return (value != null) && value.exists();
	}
	
	public static boolean isValid(Collection<?> value) {
		return (value !=null) && (!value.isEmpty());
	}
	
	public static boolean isValid(Map<?,?> value) {
		return (value != null) && (value.size() > 0);
	}
	
	public static boolean isValid(Calendar value) {
		return value != null;
	}
	
	public static boolean isValid(Date value) {
		return value != null;
	}
	
	public static boolean isValid(Long value) {
		return value != null && value.intValue() >= 0;
	}
	
	public static boolean isValid(Integer value) {
		return value != null && value.intValue() >= 0;
	}
	
	/**
	 * Determines of the given string is an alphanumeric string. All job names, 
	 * research project names, etc must be alphanumeric strings. This requirement
	 * is imposed for two reasons.  First, directory and file names are derived
	 * from the research project and job names, thus they must conform to *nix
	 * file naming rules. Second, we need to use non-alphanumeric characters to
	 * delimit query terms for searching.
	 * 
	 * @param s
	 * @return
	 */
	public static boolean isAlphaNumeric(String s) {
		if (!isValid(s)) return false;
	    boolean valid = s != null &&
                s.indexOf(":") <= -1 &&
                s.indexOf(";") <= -1 &&
                s.indexOf(",") <= -1 &&
                s.indexOf(" ") <= -1 &&
                //s.indexOf("#") > -1 ||
                s.indexOf("\\") <= -1;
	    /*||
	            s.indexOf("/") > -1 ||
	            s.indexOf("%") > -1 ||
	            s.indexOf("$") > -1 ||
	            s.indexOf("@") > -1 ||
	            s.indexOf("!") > -1 ||
	            s.indexOf("^") > -1 ||
	            s.indexOf("&") > -1 ||
	            s.indexOf("*") > -1 ||
	            s.indexOf("(") > -1 ||
	            s.indexOf(")") > -1 ||
	            s.indexOf("{") > -1 ||
	            s.indexOf("}") > -1 ||
	            s.indexOf("|") > -1 ||
	            s.indexOf("'") > -1 ||
	            s.indexOf("\"") > -1 ||
	            s.indexOf("~") > -1 ||
	            s.indexOf("`") > -1 ||
	            s.indexOf("#") > -1 ||
	            s.indexOf(".") > -1 ||
	            s.indexOf("?") > -1 ||
	            s.indexOf("<") > -1 ||
	            s.indexOf(">") > -1 ||
	            //s.indexOf("=") > -1 ||
	            s.indexOf("+") > -1*/

        return valid;
	}
	
	public static boolean isValidEmailAddress(String value) {
		if (StringUtils.isNotEmpty(value)) {
			return EmailValidator.getInstance().isValid(value);
		} else {
			return false;
		}
		
		
	}
	
	public static boolean isValidPhoneNumber(String value) {
		if (!isValid(value)) return false;
		String expression = "^\\(?(\\d{3})\\)?[- ]?(\\d{3})[- ]?(\\d{4})$";
		Pattern pattern = Pattern.compile(expression,
				Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(value);
		return matcher.matches();
	}
	
	public static boolean isValidURL(String value) {
		if (StringUtils.isNotEmpty(value)) {
			return UrlValidator.getInstance().isValid(value);
		} else {
			return false;
		}
	}
	
	/**
     * Fetch the entire contents of a text file, and return it in a String.
     * This style of implementation does not throw Exceptions to the caller.
     *
     * @param aFile is a file which already exists and can be read.
     */
    public static String getContents(File aFile) {
       //...checks on aFile are elided
       StringBuilder contents = new StringBuilder();
       
       try {
         //use buffering, reading one line at a time
         //FileReader always assumes default encoding is OK!
    	 BufferedReader input =  new BufferedReader(new FileReader(aFile));
         try {
           String line = null; //not declared within while loop
           
           /*
           * readLine is a bit quirky :
           * it returns the content of a line MINUS the newline.
           * it returns null only for the END of the stream.
           * it returns an empty String if two newlines appear in a row.
           */
           while (( line = input.readLine()) != null){
             contents.append(line);
             contents.append(System.getProperty("line.separator"));
           }
         }
         finally {
           input.close();
         }
       }
       catch (IOException ex){
         ex.printStackTrace();
       }
       
       return contents.toString();
     }


	public static String stripSurroundingBrackets(String str) 
	{
		if (str.startsWith("[")) str = str.substring(1).trim();
		if (str.equals("]"))
			str = null;
		else {
			if (str.endsWith("]")) str = str.substring(0, str.lastIndexOf("]") - 1).trim();
			str = str.replaceAll("\"", "");
		}
		
		return str;
	}
	
	public static boolean isEmailAddress(String endpoint)
	{
		String emailPattern = "^[\\w\\-]([\\.\\w])+[\\w]+@([\\w\\-]+\\.)+[a-zA-Z]{2,4}$";
		return Pattern.matches(emailPattern, endpoint);
	}


	public static boolean isValidCallback(String sCallback)
	{
		if (StringUtils.isEmpty(sCallback)) {
			return false;
		} else if (ServiceUtils.isEmailAddress(sCallback)) {
			return true;
		} else {
			try {
				URL url = new URL(sCallback);
                return url.getProtocol().equals("http") || url.getProtocol().equals("https");
            } catch (Exception e) {
				return false;
			}
		}
	}
	
	public static boolean isValidString(JSONObject json, String attribute) throws JSONException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		TreeNode node = mapper.readTree(json.toString()).get(attribute);
		return node != null && (node instanceof TextNode) && ((TextNode)node).asText() != null;
	}
	
	public static boolean isNonEmptyString(JSONObject json, String attribute) throws JSONException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		TreeNode node = mapper.readTree(json.toString()).get(attribute);
		return node != null && (node instanceof TextNode) && !StringUtils.isEmpty(((TextNode)node).asText());
	}
	
	/**
	 * Formats a 10 digit phone number into (###) ###-#### format
	 * 
	 * @param phone
	 * @return formatted phone number string
	 */
	public static String formatPhoneNumber(String phone) 
	{	
		if (StringUtils.isEmpty(phone)) { 
			return null;
		}
		else 
		{
			phone = phone.replaceAll("[^\\d.]", "");
			return String.format("(%s) %s-%s", 
					phone.substring(0, 3), 
					phone.substring(3, 6), 
					phone.substring(6, 10));
		}
	}


	public static boolean isAdmin(String username)
	{	
		if (TenancyHelper.getCurrentEndUser().equals(username) && TenancyHelper.isTenantAdmin()) return true;
		
		InputStream stream = null;
		try
		{
			stream = ServiceUtils.class.getClassLoader().getResourceAsStream("trusted_admins.txt");
			String trustedUserList = IOUtils.toString(stream, "UTF-8");
			if (isValid(trustedUserList)) {
				for(String user: trustedUserList.split(",")) {
					if (username.equalsIgnoreCase(user.trim())) {
						return true;
					}
				}
				return false;
			} else {
				return false;
			}
		}
		catch (IOException e)
		{
			log.warn("Failed to load trusted user file", e);
			return false;
		}
		finally {
			if (stream != null) try {stream.close();} catch (Exception e){}
		}
	}
	
	public static String explode(String glue, List<?> list)
	{
		
		String explodedList = "";
		
		if (!ServiceUtils.isValid(list)) return explodedList;
		
		for(Object field: list)
			explodedList += glue + field.toString();
		
		return explodedList.substring(glue.length());
	}
	
	public static String[] implode(String separator, String tags)
	{
		if (!ServiceUtils.isValid(tags)) 
		{
			return new String[]{""};
		}
		else if (!tags.contains(separator))
		{
			return new String[]{tags};
		}
		else
		{
			return StringUtils.split(tags, separator);
		}
	}
	
	/**
	 * Returns the current local IP address or an empty string in error case /
	 * when no network connection is up.
	 * <p>
	 * The current machine could have more than one local IP address so might
	 * prefer to use {@link #getAllLocalIPs() } or
	 * {@link #getAllLocalIPs(java.lang.String) }.
	 * <p>
	 * If you want just one IP, this is the right method and it tries to find
	 * out the most accurate (primary) IP address. It prefers addresses that
	 * have a meaningful dns name set for example.
	 * 
	 * @return Returns the current local IP address or an empty string in error
	 *         case.
	 * @since 0.1.0
	 */
	public static String getLocalIP()
	{
		String ipOnly = "";
		try
		{
			Enumeration<NetworkInterface> nifs = NetworkInterface
					.getNetworkInterfaces();
			if (nifs == null)
				return "";
			while (nifs.hasMoreElements())
			{
				NetworkInterface nif = nifs.nextElement();
				// We ignore subinterfaces - as not yet needed.

				if (!nif.isLoopback() && nif.isUp() && !nif.isVirtual())
				{
					Enumeration<InetAddress> adrs = nif.getInetAddresses();
					while (adrs.hasMoreElements())
					{
						InetAddress adr = adrs.nextElement();
						if (adr != null
								&& !adr.isLoopbackAddress()
								&& ( nif.isPointToPoint() || !adr
										.isLinkLocalAddress() ))
						{
							String adrIP = adr.getHostAddress();
							String adrName;
							if (nif.isPointToPoint()) // Performance issues getting hostname for mobile internet sticks
								adrName = adrIP;
							else
								adrName = adr.getCanonicalHostName();

							if (!adrName.equals(adrIP))
								return adrIP;
							else
								ipOnly = adrIP;
						}
					}
				}
			}
			return ipOnly;
		}
		catch (SocketException ex)
		{
			return "";
		}
	}

	/**
	 * Returns local ip in the simplest manner.
	 * 
	 * @return
	 * @deprecated
	 */
	public static String getIP()
	{
		InetAddress ip;
		try
		{
			ip = InetAddress.getLocalHost();
			return ip.getHostAddress();
		}
		catch (UnknownHostException e)
		{
			return getLocalIP();
		}
	}
	
    /**
     * Returns an authenticated {@link RemoteDataClient} for the given {@link LogicalFile}. 
     * Exceptions will be thrown rather than null returned if the {@link RemoteSystem} on 
     * which the data resides was not available.
     * @param logicalFile
     * @return
     * @throws AuthenticationException
     * @throws SystemUnavailableException
     * @throws SystemUnknownException
     * @throws RemoteDataException
     */
    public static RemoteDataClient getDestinationRemoteDataClient(LogicalFile logicalFile) 
    throws AuthenticationException, SystemUnavailableException, SystemUnknownException, 
            RemoteDataException
    {
        RemoteSystem system = null;
        RemoteDataClient remoteDataClient = null;
        
        system = logicalFile.getSystem();
        
        if (system == null) 
        {
            throw new SystemUnknownException("No destination system was found for user " 
                    + logicalFile + " satisfying the URI.");
        } 
        else if (!system.isAvailable())
        {
            throw new SystemUnavailableException("The destination system is currently unavailable.");
        } 
        else if ( system.getStatus() != SystemStatusType.UP)
        {
            throw new SystemUnavailableException(system.getStatus().getExpression() );
        }
        else {
            try {
                remoteDataClient = system.getRemoteDataClient(logicalFile.getInternalUsername());
                remoteDataClient.authenticate();
            } 
            catch (IOException e) {
                throw new RemoteDataException("Failed to connect to the remote destination system", e);
            }
            catch (RemoteCredentialException e) {
                throw new AuthenticationException("Failed to authenticate to remote destination system ", e);
            } 
        }
        
        return remoteDataClient;
    }
    
    /** This method is called from both synchronous REST methods and asynchronous quartz jobs so that the 
     * same path transformation can be applied uniformly.  This method is part of a refactorization that
     * moves this code from StagingJob so that is can be accessed in the synchronous part of a task 
     * triggered by a REST call.  Ideally, the asynchronous post-processing of a REST call should never
     * alter a logical file since the synchronous response already contains links that the user is expecting.   
     * 
     * @param destClient the client proxy to a destination system where one or more files will be copied
     * @param file an existing logical file whose paths may need to be adjusted
     * @param userName the logged on user that triggered the work
     * @return
     * @throws URISyntaxException
     * @throws SystemUnknownException
     * @throws PermissionException
     * @throws AgaveNamespaceException
     * @throws FileNotFoundException
     * @throws IOException
     * @throws RemoteDataException
     */
	public static String getAdjustedDestinationPath(RemoteDataClient destClient, LogicalFile file, String userName)
	 throws URISyntaxException, SystemUnknownException, PermissionException, AgaveNamespaceException, 
	        FileNotFoundException, IOException, RemoteDataException
	{
	    // Initialize output to the file's current path.
	    String adjustedPath = file.getPath();
	    
	    // Create a uri from the logical file's source field.
	    URI sourceUri = new URI(file.getSourceUri());
	    
	    // Calculate the absolute source path.
	    String srcAbsolutePath = null;
        if (ApiUriUtil.isInternalURI(sourceUri))
            srcAbsolutePath = UrlPathEscaper.decode(ApiUriUtil.getAbsolutePath(userName, sourceUri));
          else 
            srcAbsolutePath = StringUtils.removeEnd(sourceUri.getPath(), "/");
        
        // Adjust the destination path by appending the source directory name

        if (destClient.doesExist(file.getAgaveRelativePathFromAbsolutePath())) 
        {
            if (destClient.isDirectory(file.getAgaveRelativePathFromAbsolutePath())) 
            {
                if (StringUtils.isEmpty(file.getAgaveRelativePathFromAbsolutePath())) {
                    adjustedPath = srcAbsolutePath;
                } else if (StringUtils.endsWith(file.getAgaveRelativePathFromAbsolutePath(), "/")) {
                    adjustedPath = destClient.resolvePath(file.getAgaveRelativePathFromAbsolutePath() + 
                                                           FilenameUtils.getName(srcAbsolutePath));
                } else {
                    adjustedPath = destClient.resolvePath(file.getAgaveRelativePathFromAbsolutePath() + 
                                                           File.separator + FilenameUtils.getName(srcAbsolutePath));
                }
            }
        }
        else if (!destClient.doesExist(file.getAgaveRelativePathFromAbsolutePath() + (StringUtils.isEmpty(file.getAgaveRelativePathFromAbsolutePath()) ? ".." : "/..")))
        {
            throw new FileNotFoundException("Destination directory not found.");
        }
        
	    return adjustedPath;
	}

	/*
	 * This function generates a MD5 hash from a string, takes the first 8 bytes out of it and generates a signed integer value out of the byte array 
	 * assuming the array to be in BigEndian formatted.
	 */
	
	public static long getMD5LongHash(String str) throws NoSuchAlgorithmException {		
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(str.getBytes());
			return ByteBuffer.wrap(Arrays.copyOf(md.digest(), 8)).asLongBuffer().get();
	}

}
