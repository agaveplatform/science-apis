package org.iplantc.service.notification.providers.email.clients;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.sendgrid.Method;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.helpers.mail.Mail;
import com.sendgrid.helpers.mail.objects.*;
import org.codehaus.plexus.util.StringUtils;
import org.iplantc.service.notification.Settings;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.providers.email.EmailClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sendgrid.SendGrid;

/**
 * Client class to send emails using the SendGrid HTTP API. This is helpful
 * in container environments where there is no local mail server and port
 * blocking may prevent mail from otherwise being sent.
 * 
 * @author dooley
 *
 */
public class SendGridEmailClient implements EmailClient {

    protected Map<String, String> customHeaders = new HashMap<String, String>();

    private boolean sandboxMode = false;

    @Override
    public void send(String recipientName, String recipientAddress, String subject, String body, String htmlBody)
    throws NotificationException 
    {
        
        if (StringUtils.isEmpty(recipientAddress)) {
            throw new NotificationException("Email recipient address cannot be null.");
        }
        
        if (StringUtils.isEmpty(body)) {
            throw new NotificationException("Email body cannot be null.");
        }
        
        if (StringUtils.isEmpty(htmlBody)) {
            htmlBody = "<p><pre>" + body + "</pre></p>";
        }
        
        if (StringUtils.isEmpty(subject)) {
            throw new NotificationException("Email subject cannot be null.");
        }


        Email fromEmail = new Email();
        fromEmail.setName(Settings.SMTP_FROM_NAME);
        fromEmail.setEmail(Settings.SMTP_FROM_ADDRESS);

        Email toEmail = new Email();
        toEmail.setEmail(recipientAddress);
        if (StringUtils.isEmpty(recipientName)) {
            toEmail.setName(Settings.SMTP_FROM_NAME);
        }

        Personalization personalization = new Personalization();
        personalization.addTo(toEmail);
        if (!getCustomHeaders().isEmpty()) {
            for (Entry<String,String> entry: getCustomHeaders().entrySet()) {
                personalization.addHeader(entry.getKey(), entry.getValue());
            }
        }

        Content plainContent = new Content("text/plain", body);
        Content htmlContent = new Content("text/html", htmlBody);

        MailSettings mailSettings = new MailSettings();

        // enable sandbox mode if set during testing
        if (isSandboxMode()) {
            Setting sandBoxMode = new Setting();
            sandBoxMode.setEnable(true);
            mailSettings.setSandboxMode(sandBoxMode);
        }


        Mail mail = new Mail();
        mail.setFrom(fromEmail);
        mail.addPersonalization(personalization);
        mail.setSubject(subject);
        mail.addContent(plainContent);
        mail.addContent(htmlContent);
        mail.addPersonalization(personalization);
        mail.setMailSettings(mailSettings);

        SendGrid sendgrid = new SendGrid(Settings.SMTP_AUTH_TOKEN);

//        SendGrid.Email email = new SendGrid.Email();
//        if (StringUtils.isEmpty(recipientName)) {
//            email.addTo(recipientAddress);
//        } else {
//            email.addTo(recipientAddress, recipientName);
//        }
//        email.setFrom(Settings.SMTP_FROM_ADDRESS);
//        email.setFromName(Settings.SMTP_FROM_NAME);
//        email.setSubject(subject);
//        email.setHtml(htmlBody);
//        email.setText(body);

        // add custom headers if present
//        ObjectMapper mapper = new ObjectMapper();
//        ObjectNode jsonHeader = mapper.createObjectNode();


//        email.addHeader("X-SMTPAPI", jsonHeader.toString());
            
//        email.setTemplateId(Settings.SENDGRID_TEMPLATE_ID);

        try {
            Request request = new Request();
            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            request.setBody(mail.build());

            Response response = sendgrid.api(request);

            if (response == null) {
                throw new NotificationException("Failed to send notification message. Unable to connect to remote service.");
            }
            else if (response.getStatusCode() != 200 && response.getStatusCode() != 201) {
                throw new NotificationException(String.format("Failed to send notification message: (%d) - %s", response.getStatusCode(), response.getBody()));
            }
        }
        catch (IOException e) {
            throw new NotificationException("Failed to send notification due to upstream erorr from mail server.", e);
        }
    }

    /**
     * @return the customHeaders
     */
    public synchronized Map<String, String> getCustomHeaders() {
        return customHeaders;
    }

    /**
     * @param customHeaders the customHeaders to set
     */
    public synchronized void setCustomHeaders(Map<String, String> customHeaders) {
        this.customHeaders = customHeaders;
    }

    public boolean isSandboxMode() {
        return sandboxMode;
    }

    public void setSandboxMode(boolean sandboxMode) {
        this.sandboxMode = sandboxMode;
    }
}
