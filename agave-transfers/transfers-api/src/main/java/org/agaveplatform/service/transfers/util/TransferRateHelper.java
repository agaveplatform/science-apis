package org.agaveplatform.service.transfers.util;

import org.agaveplatform.service.transfers.model.TransferUpdate;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uri.AgaveUriUtil;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransferRateHelper {

    /**
     * Calculates the transfer rate by using the bytes transferred divided by the
     * elapsed time in seconds.
     *
     * @param transferTask the transfer task to update
     * @return transfer rate in bytes per second
     */
    public static double calculateTransferRate(TransferTask transferTask)
    {
        double transferRate = 0D;
        if (transferTask != null) {
            Instant start = transferTask.getStartTime() == null ? transferTask.getCreated() : transferTask.getStartTime();

            Instant end = (transferTask.getEndTime() == null || transferTask.getStatus() == TransferStatusType.TRANSFERRING) ? Instant.now() : transferTask.getEndTime();
            long milliseconds = end.toEpochMilli() - start.toEpochMilli();
            if (milliseconds > 0) {
                transferRate = transferTask.getBytesTransferred() / (milliseconds / 1000.0);
            }
        }

        return transferRate;
    }

    /**
     * Convenience method to calculate and save the transfer rate.
     * calls out to {@link #calculateTransferRate(TransferTask)}
     *
     * @param transferTask the transfer task to update
     * @return the updated transfer task
     */
    public static TransferTask updateTransferRate(TransferTask transferTask) {
        transferTask.setTransferRate(calculateTransferRate(transferTask));

        return transferTask;
    }

    /**
     * Formats bytes into human readable string value.
     *
     * @param memoryLimit in bytes
     * @return the max memory in human readable format (GB, MB, etc)
     */
    public static String formatMaxMemory(Long memoryLimit) {
        return FileUtils.byteCountToDisplaySize(memoryLimit);
    }

    /**
     * Parses numbers from abbreviated human readable form into human readable
     * integers in gb.
     * <ul>
     * <li>1XB => 1000000000</li>
     * <li>1EB => 1000000000</li>
     * <li>1PB => 1000000</li>
     * <li>1TB => 1000</li>
     * <li>1GB => 1</li>
     * </ul>
     * @param humanFileSize the file size in human readable format
     * @return the numeric value of the file size
     * @throws NumberFormatException when an invalid human file size is provided
     */
    public static long parseHumanBytes(String humanFileSize) throws NumberFormatException
    {
        if (humanFileSize == null) {
            throw new NumberFormatException("Cannot parse a null value.");
        }

        humanFileSize = humanFileSize.toUpperCase()
                .replaceAll(",", "")
                .replaceAll(" ", "");

        long returnValue = -1;
        Pattern patt = Pattern.compile("([\\d.-]+)([EPTGM]B)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = patt.matcher(humanFileSize);
        Map<String, Integer> powerMap = new HashMap<String, Integer>();
        powerMap.put("XB", 3);
        powerMap.put("PB", 2);
        powerMap.put("TB", 1);
        powerMap.put("GB", 0);
        if (matcher.find()) {
            String number = matcher.group(1);
            int pow = powerMap.get(matcher.group(2).toUpperCase());
            BigDecimal bytes = new BigDecimal(number);
            bytes = bytes.multiply(BigDecimal.valueOf(1024).pow(pow));
            returnValue = bytes.longValue();
        } else {
            throw new NumberFormatException("Invalid number format.");
        }
        return returnValue;
    }

    /**
     * Updates the summary stats of the transfer task with the values of the {@link TransferUpdate}
     * @param oldTransferTask the existing transfer task
     * @param transferUpdate the transfer progress to date
     * @return the updated transfer task
     */
    public static TransferTask updateSummaryStats(TransferTask oldTransferTask, TransferUpdate transferUpdate) {

        if (oldTransferTask != null && transferUpdate != null) {
            oldTransferTask.setBytesTransferred(oldTransferTask.getBytesTransferred() + transferUpdate.getBytesTransferred());
            oldTransferTask.setTotalFiles(oldTransferTask.getTotalFiles() + transferUpdate.getTotalFiles());
            oldTransferTask.setTotalSize(oldTransferTask.getTotalSize() + transferUpdate.getBytesTransferred());
            oldTransferTask.setTotalSkippedFiles(oldTransferTask.getTotalSkippedFiles() + transferUpdate.getTotalSkippedFiles());
            oldTransferTask.setLastUpdated(Instant.now());
        }

        return updateTransferRate(oldTransferTask);
    }

    /**
     * Converts an Agave URL to standard HTTP URL for reference in the hypermedia links
     * included in the JSON response. If the link is not an Agave url, it is returned as is.
     *
     * @param endpoint to resolve. Can be of the form agave:// or http(s)://
     * @return String resolved url
     */
    public static String resolveEndpointToUrl(String endpoint)
    {
        URI endpointUri = URI.create(endpoint);

        try {
            if (AgaveUriUtil.isInternalURI(endpointUri)) {
                if (StringUtils.equalsIgnoreCase(endpointUri.getScheme(), "agave")) {
                    return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE)
                            + "/media/system/" + endpointUri.getHost() + endpointUri.getPath();
                }
            }
        } catch (AgaveNamespaceException e) {
            // ignoring this and returning the original endpoint
        }

        return endpoint;
    }
}
