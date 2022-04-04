const {
  S3Client,
  ListBucketsCommand,
  GetObjectCommand,
  ListObjectsCommand,
} = require("@aws-sdk/client-s3");

const { v4: uuidv4 } = require("uuid");

var AWS = require("aws-sdk");
AWS.config.update({
  region: "ap-southeast-1",
  endpoint: "https://dynamodb.ap-southeast-1.amazonaws.com",
});

var docClient = new AWS.DynamoDB.DocumentClient();

const client = new S3Client({ region: "us-east-1" });

const run = async () => {
  try {
    const data = await client.send(new ListBucketsCommand({}));
    console.log("Success", data.Buckets);
    return data; // For unit tests.
  } catch (err) {
    console.log("Error", err);
  }
};

const readObjectTest = async (prefix) => {
  try {
    // Create a helper function to convert a ReadableStream to a string.
    const streamToString = (stream) =>
      new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      });

    // Get the object} from the Amazon S3 bucket. It is returned as a ReadableStream.
    const data = await client.send(
      new GetObjectCommand({
        Bucket: "iot-swift-demo-bucket",
        Key: prefix,
      })
    );
    //return data; // For unit tests.
    // Convert the ReadableStream to a string.
    const bodyContents = await streamToString(data.Body);
    console.log(bodyContents);
    const jsonbody = JSON.parse(bodyContents);

    return jsonbody;
    //return bodyContents.type;
  } catch (e) {
    console.log(e);
  }
};

const readObject = async (prefix) => {
  try {
    // Create a helper function to convert a ReadableStream to a string.
    const streamToString = (stream) =>
      new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      });

    // Get the object} from the Amazon S3 bucket. It is returned as a ReadableStream.
    const data = await client.send(
      new GetObjectCommand({
        Bucket: "iot-swift-demo-bucket",
        Key: prefix,
      })
    );
    //return data; // For unit tests.
    // Convert the ReadableStream to a string.
    const bodyContents = await streamToString(data.Body);
    //console.log(bodyContents);
    const jsonbody = JSON.parse(bodyContents);
    var datas = [];
    Object.entries(jsonbody.properties.observations).forEach(
      async ([key, value]) => {
        //console.log(key, value);
        const resultbody = {
          TableName: "swift_iot",
          Item: {
            id: uuidv4(),
            loggerID: jsonbody.properties.loggerID,
            timestamp: key,
            slrfd_w: value[0],
            rain_mm_tot: value[1],
            ws_ms: value[2],
            winddir: value[3],
            alrt_c: value[4],
            vp_mbar: value[5],
            bp_mbar: value[6],
            rh: value[7],
          },
        };

        //return resultbody
        //console.log(resultbody)

        await docClient.put(resultbody, function (err, data) {
          if (err) {
            console.error(
              "Unable to add data",
              jsonbody.properties.loggerID,
              ". Error JSON:",
              JSON.stringify(err, null, 2)
            );
          } else {
            console.log("PutItem succeeded:", jsonbody.properties.loggerID);
          }
        });
        //console.log(resultbody)
      }
    );
    //return jsonbody.properties;
    //return datas
    //return bodyContents.type;
  } catch (e) {
    console.log(e);
  }
};

const listObject = async () => {
  try {
    const data = await client.send(
      new ListObjectsCommand({
        Bucket: "iot-swift-demo-bucket",
      })
    );

    const keys = data.Contents;
    var keysdata = [];
    var datass = [];
    keys.forEach(async (key) => {
      if (key.Key.includes("iot-mqtt")) {
        const keyOnly = {
          path: key.Key,
        };
        //keysdata.push(keyOnly);
        const res = await readObject(key.Key);
        //console.log(res)
      }
    });

    //console.log("Success", datas);
    //return data; // For unit tests.
    return datass;
  } catch (err) {
    console.log("Error", err);
  }
};

module.exports = { run, readObject, listObject, readObjectTest };
