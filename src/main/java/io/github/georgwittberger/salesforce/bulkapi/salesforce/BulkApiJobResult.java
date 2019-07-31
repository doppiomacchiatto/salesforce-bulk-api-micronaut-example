package io.github.georgwittberger.salesforce.bulkapi.salesforce;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BulkApiJobResult {

  private String jobId;
  private boolean success;
  private int successfulRecords;
  private int failedRecords;

}
