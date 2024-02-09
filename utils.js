var https = require('https');
function validateJson(json) {
  if (
    !json.hasOwnProperty("insert") &&
    !json.hasOwnProperty("update") &&
    !json.hasOwnProperty("delete") &&
    !json.hasOwnProperty("select")
  ) {
    throw new Error(
      "JSON should have insert or update or delete or select key"
    );
  }
  if (json.hasOwnProperty("filter") && json.hasOwnProperty("insert")) {
    throw new Error("JSON should have not have filter with insert");
  }

  if (
    json.hasOwnProperty("limit") ||
    json.hasOwnProperty("having") ||
    json.hasOwnProperty("groupby")
  ) {
    if (!json.hasOwnProperty("select")) {
      throw new Error("Cannot use limit,having and groupby without select");
    }
  }

  if (
    json.hasOwnProperty("insert") &&
    (json.hasOwnProperty("update") ||
      json.hasOwnProperty("delete") ||
      json.hasOwnProperty("select"))
  ) {
    throw new Error("Cannot use insert with update,delete or select");
  }
  if (
    json.hasOwnProperty("update") &&
    (json.hasOwnProperty("insert") ||
      json.hasOwnProperty("delete") ||
      json.hasOwnProperty("select"))
  ) {
    throw new Error("Cannot use update with insert,delete or select");
  }
  if (
    json.hasOwnProperty("delete") &&
    (json.hasOwnProperty("update") ||
      json.hasOwnProperty("insert") ||
      json.hasOwnProperty("select"))
  ) {
    throw new Error("Cannot use delete with update,insert or select");
  }
  if (
    json.hasOwnProperty("select") &&
    (json.hasOwnProperty("update") ||
      json.hasOwnProperty("delete") ||
      json.hasOwnProperty("insert"))
  ) {
    throw new Error("Cannot use select with update,delete or insert");
  } else {
    return "";
  }
}
/*
Input param - json
example
json.url - login.abc.com
json.path - xxxxx-xxx-xxx-xxxxx/oauth2/2.0/token
json.clientId - xxxxx-xxxx-xxxxx-xxxx
json.clientSecret -XXXXXXX-XXX-XXXX-XXXXX
json.grantType - client_credentials
json.scope - https://abc.com/xxxx-xxx-xxx-xxxx-xxxxxx/.default
*/
function getAccessToken(json) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: json.url,
      path: "/" + json.path,
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      },
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => {
        data += chunk;
      });
      res.on("end", () => {
        const oauthResponse = JSON.parse(data);
        const accessToken = oauthResponse.access_token;
        resolve(accessToken);
      });
    });
    req.on("error", (error) => {
      console.error("Error obtaining access token:", error.message);
      reject(error);
    });
    req.write(
      new URLSearchParams({
        client_id: json.clientId,
        client_secret: json.clientSecret,
        grant_type: json.grantType,
        scope: json.scope,
      }).toString()
    );
    req.end();
  });
}
module.exports = {
  validateJson: validateJson,
  getAccessToken: getAccessToken,
};
