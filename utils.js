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
  if (json.hasOwnProperty("udpate") && (json.hasOwnProperty("insert") || json.hasOwnProperty("delete") || json.hasOwnProperty("select"))) {
    throw new Error("Cannot use udpate with insert,delete or select");
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

module.exports={
    validateJson:validateJson 
}