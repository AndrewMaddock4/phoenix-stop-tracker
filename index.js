import fs from "fs";
import process from "process";
import JSONStreamReader from "./JSONStreamReader.js";

const inputFilename = process.argv[2].replaceAll('"', "");
const outputFilename = process.argv[3].replaceAll('"', "");

const fileStream = fs.createReadStream(inputFilename);
const jsonReader = new JSONStreamReader(fileStream);

let vesselsData = {};
let vesselIDS = new Set();
let stoppedVessels = [];

jsonReader.on("data", processRow);

jsonReader.on("end", () => {
  fs.writeFileSync(outputFilename, JSON.stringify(stoppedVessels));
});

function processRow(row) {
  const ALLOWED_MESSAGE_IDS = new Set([1, 2, 3, 18, 19, 27]);
  const KMS_KNOT_COEFFECIENT = 1943.844492;
  if (!ALLOWED_MESSAGE_IDS.has(row.Message.MessageID)) {
    return;
  }

  const { UserID, Longitude, Latitude, MessageID } = row.Message;
  const Timestamp = row.UTCTimeStamp;

  if (!vesselIDS.has(UserID)) {
    //start recording new vessel
    vesselsData[UserID] = { UserID, Longitude, Latitude, Timestamp, TimestampStopped: Timestamp, MessageID, Stopped: false };
    vesselIDS.add(UserID);
  } else {
    //This calculate distance from previous message for this vessel
    const prevEntry = vesselsData[UserID];

    let currentPos = [Latitude, Longitude];
    let previousPos = [prevEntry.Latitude, prevEntry.Longitude];
    const distanceInKm = haversineKm(currentPos, previousPos);

    //calculate speed
    const secondsSinceLastMessage = Timestamp - prevEntry.Timestamp;
    const speedInKmPerSecond = distanceInKm / secondsSinceLastMessage;
    const speedInKnots = speedInKmPerSecond * KMS_KNOT_COEFFECIENT;

    if (speedInKnots >= 1) {
      //moving, update position and timestamps
      vesselsData[UserID] = { UserID, Longitude, Latitude, Timestamp, TimestampStopped: Timestamp, MessageID, Stopped: false };
      return;
    }

    const secondsStopped = Timestamp - prevEntry.TimestampStopped;

    if (secondsStopped >= 60 * 60 && !prevEntry.Stopped) {
      //only record a vessel that hasn't been marked as stopped for this location yet and has been stopped for an hour
      stoppedVessels.push({
        type: "Feature",
        geometry: { type: "Point", coordinates: [Longitude, Latitude] },
        properties: { name: `Stopped Vessel ${UserID}` },
      });
      vesselsData[UserID] = { UserID, Longitude, Latitude, Timestamp, TimestampStopped: prevEntry.Timestamp, MessageID, Stopped: true };
      return;
    }

    vesselsData[UserID] = { UserID, Longitude, Latitude, Timestamp, TimestampStopped: prevEntry.Timestamp, MessageID, Stopped: false };
  }
}

function haversineKm(pos1, pos2) {
  /**
   * Not fully my own code, I found implementation details in python and wrote this code using it.
   */

  let [lat1, long1] = pos1;
  let [lat2, long2] = pos2;

  const degToRad = (deg) => deg * (Math.PI / 180);
  const R = 6371; // radius of Earth in km

  let phi_1 = degToRad(lat1);
  let phi_2 = degToRad(lat2);

  let delta_phi = degToRad(lat2 - lat1);
  let delta_lambda = degToRad(long2 - long1);

  let a = Math.sin(delta_phi / 2.0) ** 2 + Math.cos(phi_1) * Math.cos(phi_2) * Math.sin(delta_lambda / 2.0) ** 2;

  let c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
}
