/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_06.task.s3;

/*
 * This program handles user requests that require archive access.
 * Copyright (C) 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.lang3.StringUtils;

// logger
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1>PTH-06 S3 Client</h1> Class for creating a Amazon S3 client with required settings.
 *
 * @since 12/03/2021
 * @author Mikko Kortelainen
 * @author Kimmo Leppinen
 * @author Motoko Kusanagi
 */
public class Pth06S3Client {

    final Logger LOGGER = LoggerFactory.getLogger(Pth06S3Client.class);

    private final String S3endPoint;
    private final String S3identity;
    private final String S3credential;

    private SignerKind signerKind = SignerKind.SIGNER_V2;

    /**
     * Initializes the builder with the required settings.
     *
     * @param S3endPoint   S3 compatible endpoint without bucket name.
     * @param S3identity   S3 access key.
     * @param S3credential S3 access secret.
     */
    public Pth06S3Client(String S3endPoint, String S3identity, String S3credential) {
        this.S3endPoint = S3endPoint;
        this.S3identity = S3identity;
        this.S3credential = S3credential;
    }

    /**
     * Defines which signing algorithm is used. Default is V2.
     *
     * @param signerKind Signing algorithm kind.
     * @return Reference to this builder.
     */
    public Pth06S3Client withSigner(SignerKind signerKind) {
        this.signerKind = signerKind;
        return this;
    }

    /**
     * Performs the build.
     *
     * @return AmazonS3Client using the builder's values.
     */
    public AmazonS3 build() {
        if (LOGGER.isDebugEnabled())
            LOGGER.info("Building S3APIWrapper for endpoint {}.", S3endPoint);

        if (StringUtils.isBlank(this.S3endPoint)) {
            throw new IllegalStateException("S3 endpoint is required.");
        }
        if (StringUtils.isBlank(this.S3identity)) {
            throw new IllegalStateException("S3 access key is required.");
        }
        if (StringUtils.isBlank(this.S3credential)) {
            throw new IllegalStateException("S3 secret is required.");
        }

        final ClientConfiguration clientConfiguration = new ClientConfiguration();

        //clientConfiguration.setSignerOverride(signerKind.getSignerTypeName());
        clientConfiguration.setSignerOverride("S3SignerType");
        // HCP / TOS supports at most 200 open connections so limit max connections.
        clientConfiguration.setMaxConnections(25);
        clientConfiguration.setMaxErrorRetry(1);
        clientConfiguration.setConnectionTimeout(300 * 1000);
        clientConfiguration.setSocketTimeout(60 * 1000);

        final AWSCredentials credentials = new BasicAWSCredentials(S3identity, S3credential);

        final AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                this.S3endPoint,
                ""
        );

        final AmazonS3 s3Client1 = AmazonS3ClientBuilder
                .standard()
                .withClientConfiguration(clientConfiguration)
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpointConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        return s3Client1;
    }

    /**
     * S3 signing algorithm kinds.
     */
    public enum SignerKind {

        // These constants are from com.amazonaws.services.s3.AmazonS3Client. Constants are private so cannot be used
        // here directly. Values select the authorization signing algorithm. V2 has to be used with TOS, V4 with AWS.
        SIGNER_V2("S3SignerType"), SIGNER_V4("AWSS3V4SignerType");

        private final String signerTypeName;

        SignerKind(final String typeName) {
            signerTypeName = typeName;
        }

        /**
         * @return the signerTypeName
         */
        public String getSignerTypeName() {
            return signerTypeName;
        }
    }

}
