package org.embulk.output.snowflake;

import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.PEMParser;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import java.io.IOException;
import java.io.StringReader;
import java.security.PrivateKey;
import java.security.Security;

// ref: https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure#privatekey-property-in-connection-properties
public class PrivateKeyReader
{
    public static PrivateKey get(String pemString) throws IOException {
        Security.addProvider(new BouncyCastleProvider());
        PEMParser pemParser = new PEMParser(new StringReader(pemString));
        Object pemObject = pemParser.readObject();
        pemParser.close();

        PrivateKeyInfo privateKeyInfo;
        if (pemObject instanceof PrivateKeyInfo) {
            privateKeyInfo = (PrivateKeyInfo) pemObject;
        } else {
            throw new IllegalArgumentException("Provided PEM does not contain a valid Private Key");
        }
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
        return converter.getPrivateKey(privateKeyInfo);
    }

}
