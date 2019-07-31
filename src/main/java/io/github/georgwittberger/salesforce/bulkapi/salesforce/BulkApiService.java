package io.github.georgwittberger.salesforce.bulkapi.salesforce;

import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import io.micronaut.context.annotation.Value;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Service providing access to the Salesforce Bulk API. It provides connections to the API and offers methods to
 * transfer records given as Java objects.
 * <p>
 * See https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_code_walkthrough.htm
 */
@Singleton
@Slf4j
public class BulkApiService {

  private final String authBaseURL;
  private final String apiVersion;
  private final String username;
  private final String password;
  private final String securityToken;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Inject
  public BulkApiService(@Value("${salesforce.auth-base-url}") String authBaseURL,
                        @Value("${salesforce.api-version}") String apiVersion,
                        @Value("${salesforce.username}") String username,
                        @Value("${salesforce.password}") String password,
                        @Value("${salesforce.security-token}") String securityToken) {
    this.authBaseURL = authBaseURL;
    this.apiVersion = apiVersion;
    this.username = username;
    this.password = password;
    this.securityToken = securityToken;
  }

  /**
   * Create a new Bulk API connection using the connection configuration of this service bean.
   *
   * @return New Bulk API connection.
   * @throws ConnectionException if the connection for authentication cannot be established.
   * @throws AsyncApiException   if the Bulk API connection cannot be established.
   */
  public BulkConnection createBulkConnection() throws ConnectionException, AsyncApiException {
    ConnectorConfig partnerConfig = new ConnectorConfig();
    partnerConfig.setUsername(username);
    partnerConfig.setPassword(password + securityToken);
    partnerConfig.setAuthEndpoint(authBaseURL + "/services/Soap/u/" + apiVersion);

    // Creating the connection automatically handles login and stores the session in partnerConfig.
    new PartnerConnection(partnerConfig);

    // When PartnerConnection is instantiated, a login is implicitly executed and, if successful,
    // a valid session is stored in the ConnectorConfig instance. Use this session to initialize a BulkConnection:
    ConnectorConfig config = new ConnectorConfig();
    config.setSessionId(partnerConfig.getSessionId());

    // The endpoint for the Bulk API service is the same as for the normal SOAP URI until the /Soap/ part.
    // From here it's '/async/versionNumber'.
    String soapEndpoint = partnerConfig.getServiceEndpoint();
    String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
    config.setRestEndpoint(restEndpoint);

    // Enable compression. This should only be false when doing debugging.
    config.setCompression(true);

    // Set this to true to see HTTP requests and responses on stdout when doing debugging.
    config.setTraceMessage(false);

    return new BulkConnection(config);
  }

  /**
   * Insert or update records for the given Salesforce object type using the given external ID. The records are passed
   * as Java objects which will be automatically converted to CSV data for transfer via the Bulk API.
   *
   * @param connection      Open Bulk API connection.
   * @param sObjectType     Salesforce object type.
   * @param externalIdField External ID field of the object type.
   * @param records         List of records to insert or update.
   * @param maxWaitSeconds  Maximum duration (in seconds) to wait for the Bulk API job to finish.
   * @return Result summary of the Bulk API job.
   * @throws AsyncApiException if communication with the API failed.
   * @throws IOException       if some read/write error occurred.
   */
  public BulkApiJobResult upsert(BulkConnection connection, String sObjectType, String externalIdField, List<?> records,
                                 long maxWaitSeconds) throws AsyncApiException, IOException {

    JobInfo jobInfo = createUpsertJob(connection, sObjectType, externalIdField);
    List<BatchInfo> batchInfos = createBatches(connection, jobInfo, records);
    closeJob(connection, jobInfo.getId());

    BulkApiJobResult jobResult;
    if (awaitCompletion(connection, jobInfo, batchInfos, maxWaitSeconds)) {
      jobResult = summarizeResults(connection, jobInfo, batchInfos);
    } else {
      jobResult = new BulkApiJobResult(jobInfo.getId(), false, 0, 0);
    }
    return jobResult;
  }

  private JobInfo createUpsertJob(BulkConnection connection, String sObjectType, String externalIdField)
    throws AsyncApiException {

    JobInfo job = new JobInfo();
    job.setObject(sObjectType);
    job.setExternalIdFieldName(externalIdField); // Only needed for "update" and "upsert" operations.
    job.setOperation(OperationEnum.upsert); // Can also use "insert" or "delete" operations here.
    job.setContentType(ContentType.CSV); // Can also use "JSON" or "XML" but CSV is the compactest one.
    return connection.createJob(job);
  }

  private void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setId(jobId);
    job.setState(JobStateEnum.Closed);
    connection.updateJob(job);
  }

  private List<BatchInfo> createBatches(BulkConnection connection, JobInfo jobInfo, List<?> records)
    throws AsyncApiException, IOException {

    // Create just a single batch for simplicity. Only 10,000 records can be processed in this case.
    try (InputStream inputStream = createRecordsInputStream(records)) {
      return List.of(connection.createBatchFromStream(jobInfo, inputStream));
    }
  }

  private <T> InputStream createRecordsInputStream(final List<T> records) {
    final PipedOutputStream outputStream = new PipedOutputStream();
    PipedInputStream inputStream;
    try {
      inputStream = new PipedInputStream(outputStream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create stream for records processing.");
    }

    executorService.execute(() -> {
      try (Writer writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
        log.debug("Converting records to CSV data.");
        StatefulBeanToCsv<T> beanToCsv = new StatefulBeanToCsvBuilder<T>(writer).build();
        beanToCsv.write(records);
        log.debug("Successfully converted records to CSV data.");
      } catch (CsvDataTypeMismatchException e) {
        log.error("Field value cannot be converted to CSV data.", e);
      } catch (CsvRequiredFieldEmptyException e) {
        log.error("Required fields are missing.", e);
      } catch (IOException e) {
        log.error("CSV data could not be written.", e);
      }
    });

    return inputStream;
  }

  private boolean awaitCompletion(final BulkConnection connection, final JobInfo jobInfo,
                                  final List<BatchInfo> batchInfos, long maxWaitSeconds) {

    Future<?> pollingFuture = executorService.submit(() -> pollBatchStates(connection, jobInfo, batchInfos));

    try {
      pollingFuture.get(maxWaitSeconds, TimeUnit.SECONDS);
      return true;
    } catch (InterruptedException e) {
      log.debug("Waiting for result of Bulk API job {} was cancelled.", jobInfo.getId());
    } catch (ExecutionException e) {
      log.error("Failed to wait for result of Bulk API job " + jobInfo.getId(), e.getCause());
    } catch (TimeoutException e) {
      log.error("Waiting for result of Bulk API job {} timed out.", jobInfo.getId());
      pollingFuture.cancel(true);
    }
    return false;
  }

  private void pollBatchStates(BulkConnection connection, JobInfo jobInfo, List<BatchInfo> batchInfos) {
    Set<String> incompleteBatchIds = new HashSet<>();
    for (BatchInfo batchInfo : batchInfos) {
      incompleteBatchIds.add(batchInfo.getId());
    }

    long sleepTime = 0L;
    while (!incompleteBatchIds.isEmpty()) {
      try {
        Thread.sleep(sleepTime);
        sleepTime = 10000L;
        BatchInfo[] batchInfoList = connection.getBatchInfoList(jobInfo.getId()).getBatchInfo();
        for (BatchInfo batchInfo : batchInfoList) {
          if (batchInfo.getState() == BatchStateEnum.Completed || batchInfo.getState() == BatchStateEnum.Failed) {
            log.debug("Bulk API batch {} finished with state {}.", batchInfo.getId(), batchInfo.getState());
            incompleteBatchIds.remove(batchInfo.getId());
          }
        }
      } catch (AsyncApiException e) {
        log.error("Failed to retrieve batch info for Bulk API job " + jobInfo.getId(), e);
        break;
      } catch (InterruptedException e) {
        log.debug("Polling batch info for Bulk API job {} was cancelled.", jobInfo.getId());
        break;
      }
    }

    if (incompleteBatchIds.isEmpty()) {
      log.debug("Finished all batches for Bulk API job {}.", jobInfo.getId());
    } else {
      log.error("Failed to finish batches for Bulk API job {} in time. Batches still running: {}",
        jobInfo.getId(), incompleteBatchIds);
    }
  }

  private BulkApiJobResult summarizeResults(BulkConnection connection, JobInfo jobInfo, List<BatchInfo> batchInfos) {
    BulkApiJobResult jobResult = new BulkApiJobResult();
    jobResult.setJobId(jobInfo.getId());

    boolean success = true;
    for (BatchInfo batchInfo : batchInfos) {
      try (InputStream resultStream = connection.getBatchResultStream(jobInfo.getId(), batchInfo.getId())) {
        // Retrieve the results of the current batch.
        List<BulkApiBatchResult> batchResults = new CsvToBeanBuilder<BulkApiBatchResult>(
          new BufferedReader(new InputStreamReader(resultStream, StandardCharsets.UTF_8)))
          .withType(BulkApiBatchResult.class).build().parse();

        // Separate the successful from the failed records.
        Map<Boolean, List<BulkApiBatchResult>> resultsBySuccess = batchResults.stream()
          .collect(Collectors.groupingBy(BulkApiBatchResult::isSuccess));

        List<BulkApiBatchResult> successfulResults = resultsBySuccess.get(true);
        if (successfulResults != null) {
          log.debug("Bulk API batch {} successfully processed {} records.", batchInfo.getId(), successfulResults.size());
          jobResult.setSuccessfulRecords(jobResult.getSuccessfulRecords() + successfulResults.size());
        }

        List<BulkApiBatchResult> failedResults = resultsBySuccess.get(false);
        if (failedResults != null) {
          log.error("Bulk API batch {} failed to process {} records.", batchInfo.getId(), failedResults.size());
          failedResults.forEach(result -> log.error("Batch error: {}", result.getError()));
          success = false;
          jobResult.setFailedRecords(jobResult.getFailedRecords() + failedResults.size());
        }
      } catch (AsyncApiException | IOException e) {
        log.error("Failed to retrieve result for Bulk API batch " + batchInfo.getId(), e);
        success = false;
      }
    }

    jobResult.setSuccess(success);
    return jobResult;
  }

}
