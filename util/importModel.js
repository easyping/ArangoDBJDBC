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

fs.readdir(directoryPath, function (err, files) {
  if (err) {
    console.log("Error getting directory information.")
  } else {
    const lstDocs = {};
    files.forEach(function (file) {
      const lines = fs.readFileSync(path.join(directoryPath, file), 'utf-8').split(/\r?\n/);
      console.log(file, lines.length);
      let doc;
      lines.forEach(line => {
        if (line.startsWith('export interface')) {
          const parts = line.split(' ');
          doc = {};
          if (parts[2].startsWith('I'))
            doc._key = parts[2].substring(1);
          else
            doc._key = parts[2];
          lstDocs[doc._key] = doc;
          doc.fields = [];
          if (parts.length >= 5) {
            if ('Identifiable' === parts[4] || 'EdgesIdentifiable' === parts[4]) {
              doc.fields.push({col: "_key", dataType: "string"});
              doc.fields.push({col: "_id", dataType: "string"});
              if ('EdgesIdentifiable' === parts[4]) {
                doc.fields.push({col: "_from", dataType: "string"});
                doc.fields.push({col: "_to", dataType: "string"});
              }
            }
          }
        } else if (line.startsWith("}")) {
          // console.log(doc);
          doc = null;
        } else if (doc) {
          line = line.trim();
          if (line.length > 0) {
            const props = line.split(';');
            for (let i = 0; i < props.length; i++) {
              const prop = props[i].trim();
              if (prop.startsWith('//'))
                continue;
              const v = prop.split(':');
              if (v.length < 2)
                continue;
              let require = false;
              if (v[0].endsWith('?'))
                v[0] = v[0].substring(0, v[0].length - 1);
              else
                require = true;
              if (v[0] === '_extend')
                continue;
              v[1] = v[1].trim();
              const ary = v[1].endsWith('[]');
              if (ary)
                v[1] = v[1].substring(0, v[1].length - 2);
              if (v[1].startsWith('I'))
                v[1] = v[1].substring(1);
              const p = {col: v[0], dataType: v[1], require};
              if (ary)
                p.dataList = ary;
              doc.fields.push(p);
            }
          }
        }
      });
    });

    console.log('JSON-Schema');
    (async function f() {
      const collections = await db.collections();
      for(dbCol of collections) {
        const doc = lstDocs[dbCol.name];
        if (doc) {
          const schema = {level: 'none', rule: {properties: {}, additionalProperties: true}};
          const req = addFields(doc.fields, schema.rule.properties, lstDocs, 0, dbCol.name, false);
          if (req != null)
            schema.rule.required = req;

          await dbCol.properties({"schema": schema});
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
          if(field.dataType !== ownColName && !prevEqOwn) {
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
