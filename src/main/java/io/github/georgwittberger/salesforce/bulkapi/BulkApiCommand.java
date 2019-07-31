package io.github.georgwittberger.salesforce.bulkapi;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.ws.ConnectionException;
import io.github.georgwittberger.salesforce.bulkapi.convert.AccountConverter;
import io.github.georgwittberger.salesforce.bulkapi.input.CsvAccount;
import io.github.georgwittberger.salesforce.bulkapi.input.InputDataService;
import io.github.georgwittberger.salesforce.bulkapi.salesforce.Account;
import io.github.georgwittberger.salesforce.bulkapi.salesforce.BulkApiJobResult;
import io.github.georgwittberger.salesforce.bulkapi.salesforce.BulkApiService;
import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * CLI command to import records from a local CSV file to Salesforce using the Bulk API.
 */
@Command(name = "BulkApi", description = "Salesforce Bulk API Micronaut Example", mixinStandardHelpOptions = true)
@Slf4j
public class BulkApiCommand implements Callable<Integer> {

  private final InputDataService inputDataService;
  private final AccountConverter accountConverter;
  private final BulkApiService bulkApiService;

  @Inject
  public BulkApiCommand(InputDataService inputDataService, AccountConverter accountConverter,
                        BulkApiService bulkApiService) {
    this.inputDataService = inputDataService;
    this.accountConverter = accountConverter;
    this.bulkApiService = bulkApiService;
  }

  public static void main(String[] args) {
    try {
      Integer exitCode = PicocliRunner.call(BulkApiCommand.class, args);
      System.exit(exitCode);
    } catch (Exception e) {
      log.error("Unhandled error", e);
      System.exit(1);
    }
  }

  public Integer call() {
    // Read the input account data from the local CSV file.
    List<CsvAccount> inputAccounts;
    try {
      inputAccounts = inputDataService.getAccounts();
    } catch (IOException e) {
      log.error("Failed to read accounts from input file.", e);
      return 1;
    }

    // Transform the input account data to Salesforce account records.
    List<Account> accounts = inputAccounts.stream().map(accountConverter::convert).collect(Collectors.toList());

    // Connect to the Salesforce Bulk API.
    BulkConnection connection;
    try {
      connection = bulkApiService.createBulkConnection();
    } catch (ConnectionException e) {
      log.error("Failed to authenticate with Salesforce.", e);
      return 1;
    } catch (AsyncApiException e) {
      log.error("Failed to connect to Salesforce Bulk API.", e);
      return 1;
    }

    // Transmit the Salesforce account records via the Bulk API.
    try {
      log.info("Importing accounts to Salesforce.");

      BulkApiJobResult jobResult = bulkApiService.upsert(connection, Account.SOBJECT_TYPE, Account.EXTERNAL_ID_FIELD,
        accounts, 60L);

      if (jobResult.isSuccess()) {
        log.info("Successfully imported {} accounts to Salesforce.", jobResult.getSuccessfulRecords());
        return 0;
      } else {
        log.error("Failed to import accounts to Salesforce. Please see the other error messages for reasons.");
        return 1;
      }
    } catch (AsyncApiException e) {
      log.error("Failed to import accounts via Bulk API.", e);
      return 1;
    } catch (IOException e) {
      log.error("Failed to read or write accounts data.", e);
      return 1;
    } catch (RuntimeException e) {
      log.error("Unknown error occurred.", e);
      return 1;
    }
  }

}
