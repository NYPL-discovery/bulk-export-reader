console.log('Loading bulk export reader')

const avro = require('avsc')
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})
const highland = require('highland')

var schema_stream_retriever = null

// main function
exports.handler = function (event, context) {
  var exps = [ {'exportFile': 'items.ndjson', 'postStream': process.env.itemPostStream, 'recordType': 'item', 'apiSchema': 'https://api.nypltech.org/api/v0.1/current-schemas/ItemPostRequest'},
  {'exportFile': 'items.ndjson', 'postStream': process.env.bibPostStream, 'recordType': 'bib', 'apiSchema': 'https://api.nypltech.org/api/v0.1/current-schemas/BibPostRequest'}]
  exps.forEach(function (exportFile) {
    var s3 = new AWS.S3({apiVersion: '2006-03-01'})
    var params = {Bucket: 'bulk-export-reader', Key: exportFile.exportFile }
    var getStream = function () {
      var data_array = []
      var idx = 0
      var loadIndex = 1

      highland(s3.getObject(params).createReadStream())
        .split()
        .compact()
        .map(JSON.parse)
        .stopOnError((err, data) => {
          if (err) console.log('error occurred - ' + err.message)
        })
        .each((data) => {
          idx = idx + 1
          data['nyplSource'] = 'sierra-nypl'
          data['nyplType'] = exportFile.recordType
          data_array.push(data)
          if (idx > 499) {
            idx = 0
            kinesisHandler(loadIndex, data_array, context, exportFile)
            data_array = []
          }
          loadIndex += 1
        })
        .on('end', function () {
          kinesisHandler(loadIndex, data_array, context, exportFile) // once more for the last batch, whatever size it is.
          console.log(`Posting last ${idx} records. End of stream.`)
        })
    }

    getStream()
  })
}

// kinesis stream handler
var kinesisHandler = function (loadIndex, records, context, exportFile) {
  if (schema_stream_retriever === null) {
    schema(exportFile.apiSchema)
    .then(function (schema_data) {
      schema_stream_retriever = schema_data
      setTimeout(() => {
        postKinesisStream(records, exportFile, schema_data)
      }, loadIndex)
    })
    .catch(function (e) {
      console.log(records[0])
      console.log(e, e.stack)
    })
  } else {
    setTimeout(() => {
      postKinesisStream(records, exportFile, schema_stream_retriever)
    }, 10)
  }
}

// get schema
var request = require('request')
var schema = function (url, context) {
  return new Promise(function (resolve, reject) {
    request(url, function (error, response, body) {
      if (!error && response.statusCode === 200) {
        resolve(JSON.parse(body).data.schema)
      } else {
        console.log(`An error occurred ${response.statusCode}`)
        reject(`Error occurred while getting schema ${response.statusCode}`)
      }
    })
  })
}

var postedToKinesis = 0

// send data to kinesis Stream
var postKinesisStream = function (records, exportFile, schemaData) {
  var avro_schema = avro.parse(schemaData)
  var records_in_avro_format = []
  records.forEach(function (rec) {
    var amazon_hash = {
      Data: avro_schema.toBuffer(rec),
      PartitionKey: crypto.randomBytes(20).toString('hex').toString() /* required */
    }
    records_in_avro_format.push(amazon_hash)
  })
  var params = {
    Records: records_in_avro_format, /* required */
    StreamName: exportFile.postStream /* required */
  }

  putRecordsUntilCompletelySuccessful(params, exportFile)
}

var putRecordsUntilCompletelySuccessful = (params, exportFile) => {
  var recordsToSend = params.Records
  console.log('Records to post count: ' + recordsToSend.length)
  kinesis.putRecords(params, (err, data) => {
    if (err) {
      console.log(err, err.stack) // an error occurred
    } else {
      var successCount = recordsToSend.length - data.FailedRecordCount
      postedToKinesis += successCount
      console.log('Successfully posted to kinesis: ' + postedToKinesis)
      if (data.FailedRecordCount > 0) {
        console.log('Hit putRecords error. Going to retrieve failed records')
        var failedRecords = getFailedRecords(data, recordsToSend)
        var params = {
          Records: failedRecords,
          StreamName: exportFile.postStream
        }
        putRecordsUntilCompletelySuccessful(params, exportFile)
      }
    }
  })
}

var getFailedRecords = (data, recordsThatWerePosted) => {
  var records = data.Records
  var failedRecords = []
  for (var i = 0; i < records.length; i++) {
    if (records[i].ErrorCode) {
      failedRecords.push(recordsThatWerePosted[i])
    }
  }
  console.log('Failed records count : ' + failedRecords.length)
  return failedRecords
}
