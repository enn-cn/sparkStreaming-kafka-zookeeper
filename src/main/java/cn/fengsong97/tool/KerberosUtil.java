package cn.fengsong97.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Hashtable;
import java.util.Map;


public class KerberosUtil implements Serializable {
    private static final long serialVersionUID = 8582955268342319037L;
    static Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);
    private static Map<String, UserGroupInformation> UGIMAP = new Hashtable<String, UserGroupInformation>();

    public static synchronized void loginUserFromKeytab(String user, String keyTabLocation) throws IOException {
        String simpleUser = user;
        if (user.contains("@")) {
            simpleUser = user.split("@")[0];
        }
        try {
            if (UGIMAP.containsKey(simpleUser)) {
                UserGroupInformation ugi = UGIMAP.get(simpleUser);
                ugi.checkTGTAndReloginFromKeytab();
                LOG.info("checkTGTAndReloginFromKeytab {}, {}", user, keyTabLocation);
            } else {
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                LOG.info("servicePrincipal:" + user);
                LOG.info("keyTabLocation:" + keyTabLocation);
                UserGroupInformation.setConfiguration(conf);
                boolean login = false;
                String keyTabLocationAsString = JarResource.getResourcePath(keyTabLocation);
                LOG.info("keytab exists:" + new File(keyTabLocationAsString).exists() + "," + keyTabLocationAsString);
                try {
                    UserGroupInformation.loginUserFromKeytab(user, keyTabLocationAsString);
                    login = true;
                } catch (Exception e) {
                    LOG.error("keytab in jar ,login fail!!!", e);
                }
                if (!login) {
                    InputStream in = JarResource.getResource(keyTabLocation);
                    OutputStream out = new FileOutputStream(keyTabLocation);
                    byte[] bytes = new byte[in.available()];
                    in.read(bytes);
                    out.write(bytes);
                    out.close();
                    UserGroupInformation.loginUserFromKeytab(user, keyTabLocation);
                    login = true;
                }

                if (!login) {
                    throw new IOException("kerbose login fail");
                }

                LOG.info("login user:"
                        + UserGroupInformation.getLoginUser().getUserName());
                UGIMAP.put(simpleUser, UserGroupInformation.getLoginUser());
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("UserGroupInformation login error", e);
            e.printStackTrace();
        }

    }
}
