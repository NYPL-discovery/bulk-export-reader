# sierra-items-s3-reader
Reads items from Sierra export saved in S3 export file, and publishes SierraItem to the SierraItemPostRequest Stream

#Setup
First, you'll need access to the AWS streams, or will need to create your own test streams. This lambda requires:

* Local items.ndjson file of NYPL Sierra catalog item records: This will be the source of the records the app will encode and pass to the Kinesis stream. (An example file with two records is included. Change the name to 'items.ndjson' to use it.)
* A Kinesis stream to post records to. (Feel free to use 'testS3ItemReader' to test.)
* A schema to use to validate avro conversion. Current lambda uses https://api.nypltech.org/api/v0.1/current-schemas/SierraItemRetrievalRequest

To configure AWS appropriately, install the AWS SDK, then run 'aws confingure' and set your keys to the appropriate values. Get the credentials from the team.

#What it Does
This is a simple lambda that reads from a large local dump of files, encodes the streamed records into avro, and posts to a kinesis stream for more processing.

Once you have the items.ndjson file at the root of the lambda, run the application with the standard command: 'node-lambda run'.

#In the Future the lambda may want to think about if it wants to... 
* Watch the directory proactively and run when a new ndjson file is added or a current file is updated, instead of requiring manual copying of files and manually running the lambda.
* Possible accept records from other external sources other than the NYPL Sierra catalog.
