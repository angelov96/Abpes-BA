package fabric.connection;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import fabric.helper.Utils;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.sdk.Enrollment;
import org.hyperledger.fabric.sdk.exception.CryptoException;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.security.CryptoSuite;
import org.hyperledger.fabric_ca.sdk.HFCAClient;
import org.hyperledger.fabric_ca.sdk.RegistrationRequest;
import org.hyperledger.fabric_ca.sdk.exception.EnrollmentException;
import org.hyperledger.fabric_ca.sdk.exception.RegistrationException;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.Properties;

public class CaClient {

    private static final Logger LOG = Logger.getLogger(CaClient.class);
    private final String caUrl;
    private final Properties caProperties;
    private HFCAClient instance;
    private UserContext adminContext;

    public CaClient(final String caUrlConstructor, final Properties caPropertiesConstructor) {
        this.caUrl = caUrlConstructor;
        this.caProperties = caPropertiesConstructor;
        try {
            init();
        } catch (MalformedURLException | IllegalAccessException | InstantiationException | ClassNotFoundException | CryptoException | InvalidArgumentException | NoSuchMethodException | InvocationTargetException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    private void init() throws MalformedURLException, IllegalAccessException, InstantiationException,
            ClassNotFoundException, CryptoException, InvalidArgumentException, NoSuchMethodException,
            InvocationTargetException {
        CryptoSuite cryptoSuite = CryptoSuite.Factory.getCryptoSuite();
        instance = HFCAClient.createNewInstance(caUrl, caProperties);
        instance.setCryptoSuite(cryptoSuite);
    }

    @Suspendable
    public void setAdminUserContext(final UserContext userContext) {
        this.adminContext = userContext;
    }

    @Suspendable
    public UserContext enrollAdminUser(final String username, final String password) {
        UserContext userContext = Utils.readUserContext(username, adminContext.getAffiliation());
        if (userContext != null) {
            LOG.info(username + " :admin is already enrolled at " + caUrl);
            return userContext;
        }
        Enrollment adminEnrollment = null;
        try {
            adminEnrollment = instance.enroll(username, password);
        } catch (EnrollmentException | org.hyperledger.fabric_ca.sdk.exception.InvalidArgumentException ex) {
            ExceptionHandler.logException(ex);
        }
        adminContext.setEnrollment(adminEnrollment);
        LOG.info(username + " :enrolled as admin at " + caUrl);
        Utils.writeUserContext(adminContext);
        return adminContext;
    }

    @Suspendable
    public String registerUser(final String username, final String organization) throws Exception {
        UserContext userContext = Utils.readUserContext(username, adminContext.getAffiliation());
        if (userContext != null) {
            LOG.info(username + " :user is already registered at " + caUrl);
            return "";
        }
        String enrollmentSecret = "";
        try {
            RegistrationRequest registrationRequest = new RegistrationRequest(username, organization);
            enrollmentSecret = instance.register(registrationRequest, adminContext);

            LOG.info(username + " :registered as user at " + caUrl);
            return enrollmentSecret;
        } catch (RegistrationException | org.hyperledger.fabric_ca.sdk.exception.InvalidArgumentException ex) {
            ExceptionHandler.logException(ex);
        }
        return enrollmentSecret;
    }

    @Suspendable
    public UserContext enrollUser(final UserContext user, final String secret) {
        UserContext userContext = Utils.readUserContext(user.getName(), adminContext.getAffiliation());
        if (userContext != null) {
            LOG.info(user.getName() + " :user is already enrolled at " + caUrl);
            return userContext;
        }
        try {
            Enrollment enrollment = instance.enroll(user.getName(), secret);
            user.setEnrollment(enrollment);
        } catch (EnrollmentException | org.hyperledger.fabric_ca.sdk.exception.InvalidArgumentException ex) {
            ExceptionHandler.logException(ex);
        }
        Utils.writeUserContext(user);
        LOG.info(user.getName() + " :enrolled user at " + caUrl);
        return user;
    }
}
