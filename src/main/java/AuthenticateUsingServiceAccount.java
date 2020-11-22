import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.dfareporting.Dfareporting;
import com.google.api.services.dfareporting.DfareportingScopes;
import com.google.api.services.dfareporting.model.*;
import com.google.common.collect.ImmutableSet;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.FileInputStream;

public class AuthenticateUsingServiceAccount extends JSONArray {
    private static final String USER_PROFILE_ID = System.getenv("USER_PROFILE_ID");
    private static final String URL_API = String.format(System.getenv("URL_API"), USER_PROFILE_ID);
    private static final String CONVERSION_KIND = System.getenv("CONVERSION_KIND");
    private static final String FLOODLIGHT_ACTIVITY_ID = System.getenv("FLOODLIGHT_ACTIVITY_ID");
    private static final String PATH_TO_JSON_FILE = System.getenv("PATH_TO_JSON_FILE");
    private static final String EMAIL_TO_IMPERSONATE = System.getenv("EMAIL_TO_IMPERSONATE");;
    private static final String APP_NAME = System.getenv("APP_NAME");;

    private static String[] getDclidList() {
        return new String[] {"CNyKiLuejO0CFQ2HUQod344Nvg"};
    }

    private static final ImmutableSet<String> OAUTH_SCOPES =
            ImmutableSet.of(DfareportingScopes.DDMCONVERSIONS, DfareportingScopes.DFAREPORTING, DfareportingScopes.DFATRAFFICKING);

    private static Credential getServiceAccountCredential() throws Exception {
        GoogleCredential credential =
                GoogleCredential.fromStream(new FileInputStream(AuthenticateUsingServiceAccount.PATH_TO_JSON_FILE));

        credential = new GoogleCredential.Builder()
                        .setTransport(credential.getTransport())
                        .setJsonFactory(credential.getJsonFactory())
                        .setServiceAccountId(credential.getServiceAccountId())
                        .setServiceAccountPrivateKey(credential.getServiceAccountPrivateKey())
                        .setServiceAccountScopes(OAUTH_SCOPES)
                        .setServiceAccountUser(AuthenticateUsingServiceAccount.EMAIL_TO_IMPERSONATE)
                        .build();

        return credential;
    }

    private static JSONObject getConversion(long floodlightConfigurationId, String DCLID) {
        long currentTimeInMilliseconds = System.currentTimeMillis();
        Long floodLightActivityId = Long.parseLong(FLOODLIGHT_ACTIVITY_ID);
        JSONObject conversion = new JSONObject();
        conversion.put("floodlightActivityId", floodLightActivityId);
        conversion.put("floodlightConfigurationId", floodlightConfigurationId);
        conversion.put("dclid", DCLID);
        conversion.put("ordinal", String.valueOf(currentTimeInMilliseconds));
        conversion.put("timestampMicros", currentTimeInMilliseconds * 1000);
        conversion.put("quantity", 1L);
        return conversion;
    }

    private static JSONObject getBody(JSONArray conversionList) {
        JSONObject body = new JSONObject();
        body.put("conversions", conversionList);
        body.put("kind", CONVERSION_KIND);

        return body;
    }

    private static void batchInsert(String accessToken, JSONArray body) throws UnirestException {
        Unirest.setTimeouts(0, 0);
        HttpResponse<String> response = Unirest.post(URL_API)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer "+ accessToken)
                .body(getBody(body).toString())
                .asString();

        System.out.println("Status: " + response.getStatus() + "/" + response.getStatusText());
        System.out.println(response.getBody());
    }

    private static void conversions(Dfareporting reporting, String accessToken, Long userProfileId, Long floodLightActivityId) throws Exception {
        FloodlightActivity floodlightActivity = reporting.floodlightActivities()
                .get(userProfileId, floodLightActivityId).execute();
        long floodlightConfigurationId = floodlightActivity.getFloodlightConfigurationId();

        JSONArray conversionList = new JSONArray();
        for (String dclid : getDclidList()) {
            conversionList.put(getConversion(floodlightConfigurationId, dclid));
        }

        batchInsert(accessToken, conversionList);
    }

    public static void main(String[] args) throws Exception {
        Long userProfileId = Long.parseLong(USER_PROFILE_ID);
        Long floodLightActivityId = Long.parseLong(FLOODLIGHT_ACTIVITY_ID);

        Credential credential = getServiceAccountCredential();
        credential.refreshToken();
        String accessToken = credential.getAccessToken();
        System.out.println("token: " + accessToken);

        Dfareporting reporting =
                new Dfareporting.Builder(credential.getTransport(), credential.getJsonFactory(), credential)
                        .setApplicationName(APP_NAME)
                        .build();

        conversions(reporting, accessToken, userProfileId, floodLightActivityId);
    }
}