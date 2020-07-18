package com.google.cloud.bigquery.connector.common;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Optional;

import static com.google.cloud.bigquery.connector.common.BigQueryClientModule.calculateBillingProjectId;

public class BigQueryClientFactory
    implements Serializable { // TODO: delete and just add insertAll functionality to BigQueryClient

  String billingProjectId;
  UserAgentHeaderProvider userAgentHeaderProvider;
  Credentials credentials;
  String materializationProject = null;
  String materializationDataset = null;

  @Inject
  public BigQueryClientFactory(
      BigQueryConfig config,
      UserAgentHeaderProvider userAgentHeaderProvider,
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier) {
    this.billingProjectId =
        calculateBillingProjectId(
            config.getParentProjectId(), bigQueryCredentialsSupplier.getCredentials());
    this.userAgentHeaderProvider = userAgentHeaderProvider;
    this.credentials = bigQueryCredentialsSupplier.getCredentials();
    if (config.getMaterializationProject().isPresent()) {
      this.materializationProject = config.getMaterializationProject().get();
    }
    if (config.getMaterializationDataset().isPresent()) {
      this.materializationDataset = config.getMaterializationDataset().get();
    }
  }

  public BigQueryClient createBigQueryClient() {
    BigQueryOptions.Builder options =
        BigQueryOptions.newBuilder()
            .setHeaderProvider(userAgentHeaderProvider)
            .setProjectId(billingProjectId)
            .setCredentials(credentials);
    Optional<String> inputMaterializationProject =
        materializationProject == null ? Optional.empty() : Optional.of(materializationProject);
    Optional<String> inputMaterializationDataset =
        materializationDataset == null ? Optional.empty() : Optional.of(materializationDataset);
    return new BigQueryClient(
        options.build().getService(), inputMaterializationProject, inputMaterializationDataset);
  }
}
