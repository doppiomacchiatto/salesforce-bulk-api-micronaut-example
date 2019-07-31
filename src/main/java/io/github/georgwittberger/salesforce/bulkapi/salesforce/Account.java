package io.github.georgwittberger.salesforce.bulkapi.salesforce;

import com.opencsv.bean.CsvBindByName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {

  public static final String SOBJECT_TYPE = "Account";
  public static final String EXTERNAL_ID_FIELD = "AccountId__c";

  @CsvBindByName(column = "Name", required = true)
  private String name;
  @CsvBindByName(column = EXTERNAL_ID_FIELD, required = true)
  private String accountId;

}
