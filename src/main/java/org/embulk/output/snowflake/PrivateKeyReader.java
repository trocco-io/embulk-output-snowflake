package org.embulk.output.snowflake;

import java.io.IOException;
import java.io.StringReader;
import java.security.PrivateKey;
import java.security.Security;
import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.PEMParser;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.InputDecryptorProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.OperatorCreationException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCSException;

// ref:
// https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure#privatekey-property-in-connection-properties
public class PrivateKeyReader {
  public static PrivateKey get(String pemString, String passphrase)
      throws IOException, OperatorCreationException, PKCSException {
    Security.addProvider(new BouncyCastleProvider());
    Object pemObject;
    try (PEMParser pemParser = new PEMParser(new StringReader(pemString))) {
      pemObject = pemParser.readObject();
    }

    PrivateKeyInfo privateKeyInfo;
    if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
      // Handle the case where the private key is encrypted.
      PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo =
          (PKCS8EncryptedPrivateKeyInfo) pemObject;
      InputDecryptorProvider pkcs8Prov =
          new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
      privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
    } else if (pemObject instanceof PrivateKeyInfo) {
      privateKeyInfo = (PrivateKeyInfo) pemObject;
    } else {
      throw new IllegalArgumentException("Provided PEM does not contain a valid Private Key");
    }
    JcaPEMKeyConverter converter =
        new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
    return converter.getPrivateKey(privateKeyInfo);
  }
}
