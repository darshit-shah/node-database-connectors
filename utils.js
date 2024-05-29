var https = require('https');
function validateJson(json) {
  if (!json.hasOwnProperty("insert") && !json.hasOwnProperty("update") && !json.hasOwnProperty("delete") && !json.hasOwnProperty("select")) {
    throw new Error("JSON should have insert or update or delete or select key");
  }
  if (json.hasOwnProperty("filter") && json.hasOwnProperty("insert")) {
    throw new Error("JSON should have not have filter with insert");
  }

  if (json.hasOwnProperty("limit") || json.hasOwnProperty("having") || json.hasOwnProperty("groupby")) {
    if (!json.hasOwnProperty("select")) {
      throw new Error("Cannot use limit,having and groupby without select");
    }
  }

  if (json.hasOwnProperty("insert") && (json.hasOwnProperty("update") || json.hasOwnProperty("delete") || json.hasOwnProperty("select"))) {
    throw new Error("Cannot use insert with update,delete or select");
  }
  if (json.hasOwnProperty("update") && (json.hasOwnProperty("insert") || json.hasOwnProperty("delete") || json.hasOwnProperty("select"))) {
    throw new Error("Cannot use update with insert,delete or select");
  }
  if (json.hasOwnProperty("delete") && (json.hasOwnProperty("update") || json.hasOwnProperty("insert") || json.hasOwnProperty("select"))) {
    throw new Error("Cannot use delete with update,insert or select");
  }
  if (json.hasOwnProperty("select") && (json.hasOwnProperty("update") || json.hasOwnProperty("delete") || json.hasOwnProperty("insert"))) {
    throw new Error("Cannot use select with update,delete or insert");
  } else {
    return "";
  }
}
function getAccessToken(json,clientSecret) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: json.extraparam.url,
      path: "/" + json.extraparam.path,
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
        client_id: json.extraparam.clientId,
        client_secret: clientSecret,
        grant_type: json.extraparam.grantType,
        scope: json.extraparam.scope,
      }).toString()
    );
    req.end();
  });
}
module.exports={
    validateJson:validateJson,
    getAccessToken:getAccessToken
}
