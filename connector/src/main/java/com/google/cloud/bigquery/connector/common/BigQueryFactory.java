package com.google.cloud.bigquery.connector.common;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

import javax.inject.Inject;
import java.io.Serializable;

import static com.google.cloud.bigquery.connector.common.BigQueryClientModule.calculateBillingProjectId;

public class BigQueryFactory implements Serializable {

    String billingProjectId;
    UserAgentHeaderProvider userAgentHeaderProvider;
    Credentials credentials;

    @Inject
    public BigQueryFactory(BigQueryConfig config, UserAgentHeaderProvider userAgentHeaderProvider, BigQueryCredentialsSupplier bigQueryCredentialsSupplier) {
        this.billingProjectId =
                calculateBillingProjectId(
                        config.getParentProjectId(), bigQueryCredentialsSupplier.getCredentials());
        this.userAgentHeaderProvider = userAgentHeaderProvider;
        this.credentials = bigQueryCredentialsSupplier.getCredentials();
    }

    public BigQuery createBigQuery() {
        BigQueryOptions.Builder options =
                BigQueryOptions.newBuilder()
                        .setHeaderProvider(userAgentHeaderProvider)
                        .setProjectId(billingProjectId)
                        .setCredentials(credentials);
        return options.build().getService();
    }
}
