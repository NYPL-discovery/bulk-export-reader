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
  var exports = event.exports;

  exports.forEach(function(exportFile) {
    var getStream = function () {
        var jsonData = exportFile.exportFile,
            stream = fs.createReadStream(jsonData, {encoding: 'utf8'}),
            parser = JSONStream.parse();
            return stream.pipe(parser);
    };

     getStream()
      .pipe(es.mapSync(function (data) {
        kinesisHandler(data, context);
      }));
  });
};

//kinesis stream handler
var kinesisHandler = function(record, context) {
  console.log('Processing ' + record );
  console.log(record);
  postKinesisStream(record);
}

//get schema
var request = require('request');
var schema = function(url, context) {
  return new Promise(function(resolve, reject) {
    request(url, function(error, response, body) {
      if(!error && response.statusCode == 200){
        resolve(JSON.parse(body).data.schema);
      }else{
        console.log("An error occurred - " + response.statusCode);
        reject("Error occurred while getting schema - " + response.statusCode);
      }
    })
  })
}

//send data to kinesis Stream
var postKinesisStream = function(record){
  const type = avro.infer(record);
  const record_in_avro_format = type.toBuffer(record);
  console.log("Avro formatted record: " + record_in_avro_format);
  console.log("NAME " + type.getSchema() + "<<<<<<");
  var params = {
    Data: record_in_avro_format, /* required */
    PartitionKey: crypto.randomBytes(20).toString('hex').toString(), /* required */
    StreamName: 'testS3ItemReader', /* required */
  }
  kinesis.putRecord(params, function (err, data) {
    if (err) console.log(err, err.stack) // an error occurred
    else     console.log(data)           // successful response
    //cb(null,data)
  })

}
