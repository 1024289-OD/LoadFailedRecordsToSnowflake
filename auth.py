import httpx
import snowflake.connector
from controller import SnowflakeCred, SalesforceCred
import sys
import jwt
import datetime


def get_snowflake_client(snowflake_cred: SnowflakeCred) -> snowflake.connector.SnowflakeConnection:
    """
    Returns a snowflake client if the test query executes correctly.
    :param snowflake_cred: The Snowflake credentials loaded from the environment.
    :return:
    """

    snowflake_client = snowflake.connector.connect(
        user=snowflake_cred.username,
        account=snowflake_cred.account,
        schema=snowflake_cred.db_schema,
        warehouse=snowflake_cred.warehouse,
        database=snowflake_cred.database,
        authenticator="externalbrowser",
        client_session_keep_alive=True,
    )

    cs = snowflake_client.cursor()
    try:
        cs.execute("SELECT current_version()")
        cs.fetchone()
        cs.close()
        return snowflake_client
    except Exception as e:
        print(e, sys.stderr)
        cs.close()
    return None



# def get_salesforce_client(salesforce_cred: SalesforceCred = SalesforceCred()) -> httpx.Client:

#     """
#     Login into salesforce and return a httpx client if the authentication is successful.
#     :param salesforce_cred:
#     :return:
#     """
#     client = httpx.Client()

#     if salesforce_cred.environment.lower() == "sandbox":
#         client.base_url = "https://test.salesforce.com"
#     elif salesforce_cred.environment.lower() == "production":
#         client.base_url = "https://login.salesforce.com"
#     elif salesforce_cred.environment.lower() == "https://login.salesforce.com":
#         client.base_url = "https://login.salesforce.com"
#     elif salesforce_cred.environment.lower() == "https://test.salesforce.com":
#         client.base_url = "https://test.salesforce.com"
#     else:
#         raise EnvironmentError(
#             f"SFDC_SANDBOX_ENVIRONMENT must be sandbox or production, got {salesforce_cred.environment}"
#         )

#     jwt_payload = jwt.encode(
#         {
#             "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=30),
#             "iss": salesforce_cred.client_id,
#             "aud": str(client.base_url),
#             "sub": salesforce_cred.username,
#         },
#         salesforce_cred.private_key,
#         algorithm="RS256",
#     )

#     # This makes a request againts the oath service endpoint in SFDC.
#     # There are two urls, login.salesforce.com for Production and test.salesforce.com
#     # for sanboxes/dev/testing environments. When using test.salesforce.com,
#     # the sandbox name should be appended to the username.

#     result = client.post(
#         # https://login.salesforce.com/services/oauth2/token -> PROD
#         # https://test.salesforce.com/services/oauth2/token -> sandbox
#         url="/services/oauth2/token",
#         data={
#             "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
#             "assertion": jwt_payload,
#         },
#     )
#     body = result.json()
#     if result.status_code != 201 and result.status_code != 200:

#         raise RuntimeError(
#             f"Authentication Failed: <error: {body['error']}, description: {body['error_description']}>"
#         )
#     salesforce_cred.token = str(body["access_token"])
#     salesforce_cred.base_url = str(body["instance_url"])

#     sfdc_user_token_check = httpx.post(
#         f"{salesforce_cred.base_url}/services/oauth2/introspect",
#         auth=(
#             salesforce_cred.client_id,
#             salesforce_cred.client_secret,
#         ),
#         data={"token": salesforce_cred.token},
#     )

#     if sfdc_user_token_check.status_code != 200:
#         raise RuntimeError(
#             f"ErrorCode: {httpx.codes.FORBIDDEN} : Could not validate credentials",
#         )

#     if "api" not in sfdc_user_token_check.json().get("scope"):
#         raise RuntimeError("Required scopes are missing.")

#     return httpx.Client(
#         base_url=salesforce_cred.base_url,
#         headers={"Authorization": f"Bearer {salesforce_cred.token}"},
#         timeout=httpx.Timeout(
#             salesforce_cred.client_timeout,
#             connect=salesforce_cred.client_connect_timeout,
#         ),
    # )

