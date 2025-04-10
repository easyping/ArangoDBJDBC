const arangojs = require('arangojs');
const path = require("path");
const fs = require("fs");

const dbConfig = {
  dbUrl: 'http://localhost:8529',
  dbName: '',
  dbUser: '',
  dbPassword: ''
}

// Path to Typescript interface class, which will imported
const directoryPath = path.join(__dirname, "../src/app/models");

const db = new arangojs.Database({
  url: dbConfig.dbUrl,
  databaseName: dbConfig.dbName
});
db.useBasicAuth(dbConfig.dbUser, dbConfig.dbPassword);

function convertInterfaceToJsonSchema(lines) {
  let jsonSchema = {
    type: "object",
    properties: {},
    required: []
  };
  const lstSchemas = [];

  let insideInterface = false;
  let currentInterface = "";
  let defs = [];

  const mapTsTypeToJsonType = (tsType) => {
    if (tsType.startsWith("{")) {
      // Behandlung von geschachtelten `{ [key: ...]: ... }`-Definitionen
      return parseNestedRecord(tsType);
    }
    // mehrere Typen?
    if (tsType.includes("|") && !tsType.trim().startsWith('{')) {
      const types = tsType.split("|");
      // Sind die Typen Enum-Werte?
      const enumValues = [];
      types.forEach(type => {
        type = type.trim();
        if (type.toString().startsWith("'") || type.toString().startsWith('"'))
          enumValues.push(type.toString().slice(1, -1));
        else if (!isNaN(parseFloat(type)) && isFinite(type))
          enumValues.push(parseFloat(type));
        else if (type === 'true')
          enumValues.push(true);
        else if (type === 'false')
          enumValues.push(false);
      });
      // Enum-Werte gefunden?
      if (enumValues.length > 0) {
        return {enum: enumValues};
      } else {
        if (types.find(t => t.includes("undefined"))) {
          types.splice(types.indexOf("undefined"), 1);
        }
        const unionTypes = types.map(type => mapTsTypeToJsonType(type.trim()));
        if (unionTypes.length === 1)
          return unionTypes[0];
        else {
          return {
            oneOf: unionTypes
          };
        }
      }
    }
    // TS-Datentypen zu JSON-Schema-Datentypen mappen
    switch (tsType) {
      case "string":
        return {type: "string"};
      case "Date":
        return {type: "string", format: 'yyyy-MM-ddTHH:mm:ss.SSSZ'};
      case "number":
        return {type: "number"};
      case "integer":
        // return { type: "integer" };
        // Definition in ArangoDB for type integer (https://docs.arangodb.com/3.12/concepts/data-structure/documents/schema-validation/#json-schema-rule)
        return {type: "number", multipleOf: 1};
      case "boolean":
        return {type: "boolean"};
      case "null":
        return {type: "null"};
      case "object":
      case "TableState":
        return {type: "object"};
      case "any":
        return {
          oneOf: [
            {type: "string"},
            {type: "number"},
            {type: "boolean"},
            {type: "object"},
            {type: "array"}
          ]
        };
      default:
        if (tsType.endsWith("[]")) {
          // Arrays erkennen
          const itemType = mapTsTypeToJsonType(tsType.slice(0, -2));
          return {type: "array", items: itemType};
        } else {
          // Annahme: Andere Interfaces werden referenziert
          if (tsType.startsWith('I'))
            tsType = tsType.substring(1);
          if (!defs.includes(tsType))
            defs.push(tsType);
          return {$ref: `#/$defs/${tsType}`};
        }
    }
  };
  const parseNestedRecord = (typeStr) => {
    const match = typeStr.match(/^\{\s*\[([^\]]+)\]:\s*(.*)\}$/);
    if (match) {
      const [, keyType, valueType] = match;
      return {
        type: "object",
        additionalProperties: mapTsTypeToJsonType(valueType.trim())
      };
    }

    // Wenn geschachtelte Typen, rekursiv analysieren
    const nestedMatch = typeStr.match(
      /^\{\s*\[([^\]]+)\]:\s*\{\s*\[([^\]]+)\]:\s*(.*)\}\s*\}$/
    );
    if (nestedMatch) {
      const [, outerKey, innerKey, innerValue] = nestedMatch;
      return {
        type: "object",
        additionalProperties: {
          type: "object",
          additionalProperties: {
            type: "object",
            additionalProperties: mapTsTypeToJsonType(innerValue.trim())
          }
        }
      };
    }
    return {};
  };

  let insideExtend = false;
  let insideEnum = false;

  lines.forEach((line) => {
    line = line.trim();
    if (line.length > 0 && !line.startsWith("//")) {

      // Start eines Interface erkennen
      if (line.startsWith("export interface ")) {
        if (jsonSchema.required && jsonSchema.required.length === 0) {
          delete jsonSchema.required;
        }
        // Die Defines vormerken
        if (defs.length > 0) {
          jsonSchema.$defs = {};
          for (const tsType of defs)
            jsonSchema.$defs[tsType] = {
              type: "object",
              properties: {}
            };
        }
        currentInterface = "";
        defs = [];
        insideInterface = true;
        jsonSchema = {
          type: "object",
          properties: {},
          required: []
        };
        lstSchemas.push(jsonSchema);
        const parts = line.split(" ");
        currentInterface = parts[2];
        if (currentInterface.startsWith('I'))
          currentInterface = currentInterface.substring(1);
        if (parts.length >= 5) {
          for (let i = 4; i < parts.length; ++i) {
            if (parts[i].endsWith(',') || parts[i].endsWith('{'))
              parts[i] = parts[i].substring(0, parts[i].length - 1);
            parts[i] = parts[i].trim();
            if ('Identifiable' === parts[i] || 'EdgesIdentifiable' === parts[i]) {
              jsonSchema.properties["_key"] = {type: "string"};
              jsonSchema.properties["_id"] = {type: "string"};
              jsonSchema.properties["_rev"] = {type: "string"};
              if ('EdgesIdentifiable' === parts[i]) {
                jsonSchema.properties["_from"] = {type: "string"};
                jsonSchema.properties["_to"] = {type: "string"};
              }
            } else if (parts[i].length > 0) {
              if (!jsonSchema.extends)
                jsonSchema.extends = [];
              jsonSchema.extends.push(parts[i]);
            }
          }
        }
        jsonSchema.title = currentInterface;
      } else if (line.startsWith("}")) {
        // Ende des Interface
        if (insideExtend)
          insideExtend = false;
        else if (insideEnum)
          insideEnum = false;
        else
          insideInterface = false;
      } else if (insideInterface && !insideExtend) {
        // Eigenschaften des Interface analysieren
        let colon = line.indexOf(':');
        if (line.startsWith("'")) {
          const endQuote = line.indexOf("'", 1);
          if (colon < endQuote)
            colon = line.indexOf(':', endQuote);
        } else if (line.startsWith('[')) {
          colon = line.indexOf(':', line.indexOf('['));
        }
        const parts = [
          line.substring(0, colon),
          line.substring(colon + 1).trim()
        ];
        if (parts.length === 2 && !parts[0].startsWith('[')) {
          let [propName, propType] = parts;
          propName = propName.trim().replace("?", ""); // Fragezeichen entfernen
          if (propName !== '_extend') {
            let isRequired = !parts[0].includes("?"); // Fragezeichen bedeutet optional
            propType = propType.substring(0, propType.indexOf(';')).trim();

            // Typ mappen und hinzufügen
            jsonSchema.properties[propName] = mapTsTypeToJsonType(propType);

            if (isRequired) {
              jsonSchema.required.push(propName);
            }
          } else if (propType.trim().startsWith('{')) {
            insideExtend = true;
          }
        } else if (parts[0].startsWith('[')) {
          jsonSchema.additionalProperties = true;
        }
      } else if (line.startsWith("export enum ")) {
        insideEnum = true;
        const parts = line.split(" ");
        currentInterface = parts[2];
        jsonSchema = {
          title: currentInterface,
          enum: []
        };
        lstSchemas.push(jsonSchema);
      } else if (insideEnum) {
        if (line.includes('=')) {
          const parts = line.split('=');
          const value = parts[1].trim();
          if (value.toString().startsWith("'") || value.toString().startsWith('"'))
            jsonSchema.enum.push(value.toString().slice(1, -1));
          else if (!isNaN(parseFloat(value)) && isFinite(value))
            jsonSchema.enum.push(parseFloat(value));
          else if (value === 'true')
            jsonSchema.enum.push(true);
          else if (value === 'false')
            jsonSchema.enum.push(false);
        } else
          jsonSchema.enum.push(line.trim());
      }
    }
  });

  if (jsonSchema.required.length === 0) {
    delete jsonSchema.required;
  }
  // Die Defines vormerken
  if (defs.length > 0) {
    jsonSchema.$defs = {};
    for (const tsType of defs)
      jsonSchema.$defs[tsType] = {
        type: "object",
        properties: {}
      };
  }
  return lstSchemas;
}

function copyExtendsProperties(doc, lstDocs, addDefTo) {
  if (doc.extends) {
    for (let extend of doc.extends) {
      if (extend.startsWith('I'))
        extend = extend.substring(1);
      if (!lstDocs[extend]) {
        console.log('Extends-Definition', extend, 'not found in', doc.title);
        process.exit(1);
      }
      const colDef = lstDocs[extend];
      for (const prop of Object.keys(colDef.properties)) {
        if (!doc.properties[prop])
          doc.properties[prop] = colDef.properties[prop];
        if (colDef.required && colDef.required.includes(prop)) {
          if (!doc.required)
            doc.required = [prop];
          else if (!doc.required.includes(prop))
            doc.required.push(prop);
        }
        if (doc.properties[prop]['$ref']) {
          const ref = doc.properties[prop]['$ref'];
          if (ref.startsWith('#/$defs/')) {
            const defKey = ref.substring('#/$defs/'.length);
            if (!doc['$defs'])
              doc['$defs'] = {};
            if (!doc['$defs'][defKey]) {
              doc['$defs'][defKey] = {
                type: 'object',
                properties: {}
              };
            }
          }
        } else if (doc.properties[prop].additionalProperties && doc.properties[prop].additionalProperties['$ref']) {
          const ref = doc.properties[prop].additionalProperties['$ref'];
          if (ref.startsWith('#/$defs/')) {
            const defKey = ref.substring('#/$defs/'.length);
            if (!doc['$defs'])
              doc['$defs'] = {};
            if (!doc['$defs'][defKey]) {
              doc['$defs'][defKey] = {
                type: 'object',
                properties: {}
              };
            }
          }
        } else if (doc.properties[prop].oneOf) {
          doc.properties[prop].oneOf.forEach(oneOf => {
            if (oneOf['$ref']) {
              const ref = oneOf['$ref'];
              if (ref.startsWith('#/$defs/')) {
                const defKey = ref.substring('#/$defs/'.length);
                if (!doc['$defs'])
                  doc['$defs'] = {};
                if (!doc['$defs'][defKey]) {
                  doc['$defs'][defKey] = {
                    type: 'object',
                    properties: {}
                  }
                }
              }
            }
          });
        }
      }
    }
    delete doc.extends;
  }
}

function copyDefProperties(doc, lstDocs, addDefTo) {
  if (doc['$defs']) {
    for (const defKey of Object.keys(doc['$defs'])) {
      if (!lstDocs[defKey]) {
        console.log('Definition', defKey, 'not found in', doc.title);
        process.exit(1);
      }
      const colDef = lstDocs[defKey];
      if (addDefTo) {
        if (addDefTo['$defs'][defKey])
          continue;
        addDefTo['$defs'][defKey] = {
          type: 'object',
          properties: {}
        };
      }
      const def = (addDefTo ? addDefTo : doc)['$defs'][defKey];
      if (colDef.properties) {
        def.properties = JSON.parse(JSON.stringify(colDef.properties));
        delete def.properties['_key'];
        delete def.properties['_id'];
        delete def.properties['_rev'];
        delete def.properties['_from'];
        delete def.properties['_to'];
      }
      if (colDef.enum)
        def.enum = JSON.parse(JSON.stringify(colDef.enum));
      if (colDef.required)
        def.required = JSON.parse(JSON.stringify(colDef.required));
      if (colDef['$defs'])
        copyDefProperties(colDef, lstDocs, addDefTo || doc);
    }
  }
}


fs.readdir(directoryPath, function (err, files) {
  if (err) {
    console.log("Error getting directory information.")
  } else {
    const lstDocs = {};
    files.forEach(function(file) {
      const lines = fs.readFileSync(path.join(directoryPath, file), 'utf-8').split(/\r?\n/);
      console.log(file, lines.length);
      for (const doc of convertInterfaceToJsonSchema(lines)) {
        lstDocs[doc.title] = doc;
        if (doc.enum)
          delete doc.title;
      }
    });
    console.log('Schema extends auflösen');
    for (const doc of Object.values(lstDocs)) {
      copyExtendsProperties(doc, lstDocs);
    }
    // $def von den Schema auflösen
    console.log('Schema $def auflösen');
    for (const doc of Object.values(lstDocs)) {
      copyDefProperties(doc, lstDocs);
    }

    console.log('Import JSON-Schema in database:');
    (async function f() {
      const collections = await db.collections();
      for (const dbCol of collections) {
        const doc = lstDocs[dbCol.name];
        if (doc) {
          const schema = {
            level: 'none',
            rule: {
              properties: doc.properties,
              additionalProperties: true
            }
          };
          if (doc.required)
            schema.rule.required = doc.required;
          if (doc['$defs'])
            schema.rule['$defs'] = doc['$defs'];

          await dbCol.properties({ "schema": schema });
        }
      }
    })();
  }
})

function addFields(fields, prop, lstDocs, level, ownColName, prevEqOwn) {
  const require = [];
  fields.forEach(field => {
    if (level === 0 || (field.col !== '_key' && field.col !== '_id')) {
      if (field.dataType === 'Date')
        prop[field.col] = {type: 'string', format: 'yyyy-MM-ddTHH:mm:ss.SSSZ'};
      else if (field.dataType === 'string' || field.dataType === 'number' || field.dataType === 'integer' || field.dataType === 'array' || field.dataType === 'object' || field.dataType === 'boolean')
        prop[field.col] = {type: field.dataType};
      else {
        prop[field.col] = {type: 'object'};
        const udoc = lstDocs[field.dataType];
        if (udoc) {
          prop[field.col].properties = {};
          prop[field.col].additionalProperties = true;
          if (field.dataType !== ownColName && !prevEqOwn) {
            const req = addFields(udoc.fields, prop[field.col].properties, lstDocs, level + 1, field.dataType, field.dataType === ownColName);
            if (req != null)
              prop[field.col].required = req;
          }
        }
      }
      if (field.dataList) {
        prop[field.col].items = {type: prop[field.col].type};
        if (prop[field.col].type === 'object') {
          prop[field.col].items.properties = prop[field.col].properties;
          prop[field.col].items.required = prop[field.col].required;
          prop[field.col].items.additionalProperties = true;
          delete prop[field.col].properties;
          delete prop[field.col].additionalProperties;
          delete prop[field.col].required;
        }
        prop[field.col].type = 'array';
      }
      if (field.require)
        require.push(field.col);
    }
  });
  if (require.length > 0)
    return require;
  return null;
}
