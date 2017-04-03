# sierra-items-s3-reader
Reads items from Sierra export saved in S3 export file, and publishes SierraItem to the SierraItemPostRequest Stream

# Setup
To build/test install https://www.npmjs.com/package/node-lambda globally (`npm i -g node-lambda`) if you haven't already done so for other projects.

This particular lambda requires:

* Local items.ndjson and bibs.ndjson files of NYPL Sierra catalog item and bib records: This will be the source of the records the app will encode and pass to the Kinesis streams. (Example files with two records is included. Change the name to 'items.ndjson' and 'bibs.ndjeson' to use them.)
* A Kinesis stream to post records to. (Feel free to use 'testS3ItemReader' and 'testS3BibReader' to test.)
* A schema to use to validate avro conversion. Current lambda uses https://api.nypltech.org/api/v0.1/current-schemas/SierraItemPostRequest and https://api.nypltech.org/api/v0.1/current-schemas/SierraBibPostRequest

To add AWS credentials, type `aws configure` or `vi ~/.aws/credentials` (or your editor of choice) and enter in the credentials as provided to you by a colleague.  

```
AWS Access Key ID: (add secret)
AWS Secret Access: (add secret)
Default region name [None]: us-east-1
Default output format [None]: (blank / json)
```

More documentation can be found [here](https://docs.google.com/document/d/1RW47fDEvuIjUC-lJu_OFVylPQtyiX2OfjW_8QJpcm38/edit#)


To start, run `npm install` to install the dependencies.  

For testing, run `cp items.ndjson.example items.ndjson` and `cp bibs.ndjson.example bibs.ndjson` and try `node-lambda run`.


# What it Does
This is a simple lambda that reads from large local dumps of files, encodes the streamed records into avro, and posts to a kinesis stream for more processing.

Once you have the files, run the application with the standard command: `node-lambda run`.

# In the Future the lambda may want to think about if it wants to...
* Watch the directories proactively and run when a new ndjson file is added or a current file is updated, instead of requiring manual copying of files and manually running the lambda.
* Possibly accept records from other external sources other than the NYPL Sierra catalog.
