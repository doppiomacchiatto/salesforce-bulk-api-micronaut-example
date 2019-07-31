package io.github.georgwittberger.salesforce.bulkapi.input;

import com.opencsv.bean.CsvToBeanBuilder;
import io.micronaut.context.annotation.Value;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Service providing the input data from the local CSV file.
 */
@Singleton
public class InputDataService {

  private final Path accountFilePath;

  @Inject
  public InputDataService(@Value("${input.account-file-name}") String accountFileName) {
    this.accountFilePath = Path.of(accountFileName);
  }

  /**
   * Get the list of accounts from the input CSV file.
   *
   * @return List of account objects from the input file.
   * @throws IOException if the accounts cannot be read from the input file.
   */
  public List<CsvAccount> getAccounts() throws IOException {
    return new CsvToBeanBuilder<CsvAccount>(Files.newBufferedReader(accountFilePath, StandardCharsets.UTF_8))
      .withType(CsvAccount.class).build().parse();
  }

}
