package io.github.georgwittberger.salesforce.bulkapi.convert;

import io.github.georgwittberger.salesforce.bulkapi.input.CsvAccount;
import io.github.georgwittberger.salesforce.bulkapi.salesforce.Account;

import javax.inject.Singleton;

/**
 * Converter used to transform the account data from the CSV file to Salesforce account records.
 */
@Singleton
public class AccountConverter {

  /**
   * Convert a CSV account object to a Salesforce account record.
   *
   * @param inputAccount Account data from the input file.
   * @return Salesforce account record.
   */
  public Account convert(CsvAccount inputAccount) {
    Account outputAccount = new Account();
    outputAccount.setName(inputAccount.getCompanyName());
    outputAccount.setAccountId(inputAccount.getCompanyId());
    return outputAccount;
  }

}
