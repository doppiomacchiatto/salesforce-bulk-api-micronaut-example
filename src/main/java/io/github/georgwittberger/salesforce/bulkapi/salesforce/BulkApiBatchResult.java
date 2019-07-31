package io.github.georgwittberger.salesforce.bulkapi.salesforce;

import com.opencsv.bean.CsvBindByName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BulkApiBatchResult {

  @CsvBindByName(column = "Success")
  private boolean success;
  @CsvBindByName(column = "Created")
  private boolean created;
  @CsvBindByName(column = "Id")
  private String id;
  @CsvBindByName(column = "Error")
  private String error;

}
