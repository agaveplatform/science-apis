package org.iplantc.service.common.restlet;


import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by wcs on 7/17/14.
 */
public class UniqueIdFilter implements Filter {
    
    // 3 bytes of randomness fit into 2^24 - 1. 
    private static final int CEILING = 0x1000000;
    
    public void destroy() {
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) 
    throws IOException, ServletException 
    {
        if (req instanceof HttpServletRequest) {
            final HttpServletRequest httpRequest = (HttpServletRequest)req;
            final String requestId = httpRequest.getHeader("UNIQUE_ID");
            if (StringUtils.isNotEmpty(requestId)) {
                MDC.put("UNIQUE_ID", requestId);
            } else {
                MDC.put("UNIQUE_ID", getRandomString());
            }
        } else {
            MDC.put("UNIQUE_ID", "none");
        }
        chain.doFilter(req, resp);

    }

    /** Generate a pseudo-random base64url string that
     * can be used to identify the request serviced by
     * this thread.  
     * 
     * @return the randomized string
     */
    private static String getRandomString() 
    {
        // Get a pseudo-random int value that has its low-order 
        // 24 bits randomized, which is enough to generate a 
        // 4 character base64 string.
        int n = ThreadLocalRandom.current().nextInt(CEILING);
        byte[] b = new byte[3];
        b[2] = (byte) (n);
        n >>>= 8;
        b[1] = (byte) (n);
        n >>>= 8;
        b[0] = (byte) (n);
        
        // Encode the 3 bytes into 4 characters 
        // and avoid any padding.
        return Base64.getUrlEncoder().encodeToString(b);
    }
}
