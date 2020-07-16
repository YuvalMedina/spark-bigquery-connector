package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import java.io.Serializable;

import static com.google.cloud.bigquery.connector.common.BigQueryClientModule.calculateBillingProjectId;

public class BigQueryFactory implements Serializable {

    BigQueryConfig config;
    UserAgentHeaderProvider userAgentHeaderProvider;
    BigQueryCredentialsSupplier bigQueryCredentialsSupplier;

    public BigQueryFactory(BigQueryConfig config, UserAgentHeaderProvider userAgentHeaderProvider, BigQueryCredentialsSupplier bigQueryCredentialsSupplier) {
        this.config = config;
        this.userAgentHeaderProvider = userAgentHeaderProvider;
        this.bigQueryCredentialsSupplier = bigQueryCredentialsSupplier;
    }

    public BigQuery createBigQuery() {
        String billingProjectId =
                calculateBillingProjectId(
                        config.getParentProjectId(), bigQueryCredentialsSupplier.getCredentials());
        BigQueryOptions.Builder options =
                BigQueryOptions.newBuilder()
                        .setHeaderProvider(userAgentHeaderProvider)
                        .setProjectId(billingProjectId)
                        .setCredentials(bigQueryCredentialsSupplier.getCredentials());
        return options.build().getService();
    }
}
