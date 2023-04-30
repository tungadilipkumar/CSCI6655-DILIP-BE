const asyncHandler = require('express-async-handler')
const express = require('express')
const path = require('path')
const cors = require('cors')
const mqtt = require('async-mqtt')
const app = express()
var randomZip = require('random-zipcode');
const { MongoClient, ServerApiVersion } = require('mongodb');

const PORT = 3000;
const TOPIC = process.env.TOPIC || 'dilip-final-project-temperature-data';
const DB = 'sensordata'
const COLLECTION = 'temperature'

const uri = process.env.MONGODB_URI;
// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const dbclient = new MongoClient(uri, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
});
const db = dbclient.db(DB)

const mqttclient = mqtt.connect(
    {
        host: 'mqtt-dashboard.com',//'broker.hivemq.com',//'test.mosquitto.org',
        protocolVersion: 5,
    }
)
mqttclient.on('connect', async function () {
    console.log("Connection to MQTT broker success")
    let res = await mqttclient.subscribe(TOPIC)
    console.log(res)
    if (res) {
        console.log(`Subscription to topic ${TOPIC} success`)
    }
    mqttclient.on('message', async function (topic, message) {
        let doc = message.toString()
        console.log(topic, doc)
        let collection = db.collection(COLLECTION)
        doc = JSON.parse(doc)
        try {
            await collection.insertOne(doc)
        } catch (e) {
            console.log("Insertion failed");
        }
    });
})
app.use(cors())
app.options('*', cors())

app.get('/test', function (req, res, next) {
    res.send("Server is up and running")
})
app.get('/', function (req, res, next) {
    res.sendFile('portfolio.html', {root: path.join(__dirname, "/public/")});
})

app.get('/api', asyncHandler(async (req, res, next) => {
    let collection = db.collection(COLLECTION)
    let data = await collection.find().limit(50).toArray()
    res.send(data)
}))

function getRandomArbitrary(min, max, double = false, round = false) {
    var p = Math.random() * (max - min);
    if (round) {
        p = Math.round((p + Number.EPSILON) * 100) / 100;
    }
    if (double) {
        return p + min;
    }
    return Math.floor(p) + min;
}

app.get('/publish_random', asyncHandler(async (req, res, next) => {
    doc = {
        "temperature": getRandomArbitrary(20, 75, true, true),
        "humidity": getRandomArbitrary(0, 100),
        "zip_code": randomZip(),
        "sensor_id": getRandomArbitrary(0, 5000),
        "timestamp": Date.now() / 1000 | 0,
        "lat": getRandomArbitrary(-90, 90, true),
        "long": getRandomArbitrary(-180, 180, true)
    }
    await mqttclient.publish(TOPIC, JSON.stringify(doc))
    res.json(doc)
}))

app.on('ready', function () {
    let server = app.listen(PORT, function () {
        console.log(`CORS-enabled web server listening on port ${PORT}`)
    })

})
// cleanup = () => server.close(async () => {
//     mqttclient.end();
//     await dbclient.close();
//     console.log("Closed db and mqtt connections")
//     process.exit(1)
// })

// process.on('SIGINT', cleanup);
// process.on('SIGTERM', cleanup);
async function init() {
    // console.log("Start connections to database and mqtt")


    console.log("waiting for MongoDB connection. Make sure you're connected to internet")
    await dbclient.connect();
    console.log('Connected successfully to mongodb server');
    // const db = dbclient.db('sensordata');

    // Mongo DB collection and schema details
    // const collection = db.createCollection("temperature", {
    //     validator: {
    //        $jsonSchema: {
    //           bsonType: "object",
    //           title: "Temperature Sensor data Validation",
    //           required: [ "temperature", "humidity", "timestamp", "zip_code", "lat", "long", "sensor_id" ],
    //           properties: {
    //              temperature: {
    //                 bsonType: "double",
    //                 minimum: 20.0,
    //                 maximum: 75.0,
    //                 description: "'temperature' must be a double in range 20-75 which represents US temperatuers in F"
    //              },
    //              lat: {
    //                 bsonType: "double",
    //                 minimum: -90,
    //                 maximum: 90,
    //                 description: "Latitude"
    //              },
    //              long: {
    //                 bsonType: "double",
    //                 minimum: -180,
    //                 maximum: 180,
    //                 description: "Longitude"
    //              },
    //              sensor_id: {
    //                 bsonType: "int",
    //                 minimum: 0,
    //                 maximum: 5000,
    //                 description: "'sensor' id is required and must be an integer below 5000'"
    //              },
    //              humidity: {
    //                 bsonType: "int",
    //                 minimum: 0,
    //                 maximum: 100,
    //                 description: "'humidity' id is required and must be an integer below 5000'"
    //              },
    //              timestamp: {
    //                 bsonType: "int",
    //                 description: "'timestamp': required epoch timestamp in seconds"
    //              },
    //              zip_code: {
    //                 bsonType: "string",
    //                 description: "'zip_code': required US zip code where sensor is installed"
    //              },
    //           }
    //        }
    //     }
    //  } )
    // database = sensordata
    // collection = temperature
    /* temperature sensor data {
        temperature: 23.3 (25-72 F),
        humidity: 55 (0-100 %),
        timestamp: 1600235625 (epoch),
        zip_code: 25502 (integer),
        sensor_id: 359 (integer),
        lat: 16.202221 (-90 to 90),
        long: 26.02235 (-180 to 180),
    } */
    app.emit('ready')
}

init().catch(console.error).finally(() => {
    console.log("Server setup complete")
})