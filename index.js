console.log("Loading sierra s3 item reader");

const avro = require('avsc');
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})
const wrapper = require('sierra-wrapper')

//main function
exports.handler = function(event, context){
  var fs = require('fs'),
  JSONStream = require('JSONStream'),
  es = require('event-stream');

  var getStream = function () {
      var jsonData = 'items.ndjson',
          stream = fs.createReadStream(jsonData, {encoding: 'utf8'}),
          parser = JSONStream.parse('*');
          return stream.pipe(parser);
  };

   getStream()
    .pipe(es.mapSync(function (data) {
      console.log(data);
    }));
};
