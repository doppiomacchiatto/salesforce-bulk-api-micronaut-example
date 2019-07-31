package io.github.georgwittberger.salesforce.bulkapi.input;

import com.opencsv.bean.CsvBindByName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CsvAccount {

  @CsvBindByName(column = "Company Name", required = true)
  private String companyName;
  @CsvBindByName(column = "Company ID", required = true)
  private String companyId;

}
