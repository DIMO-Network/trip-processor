import "./App.css";
import ReactMapboxGl, { Layer } from "react-mapbox-gl";
import { useEffect, useState } from "react";
import { json } from "stream/consumers";

const MapBox = ReactMapboxGl({
  accessToken:
    "pk.eyJ1IjoiYmVua29jaGFub3dza2kiLCJhIjoiY2t6eDdlZzRnOGUyeTJvbXphdXdvZnJjZSJ9.S1WS1tkPKQnGt3A5Y72ZUA",
  attributionControl: false,
  doubleClickZoom: false,
  // maxZoom: 7,
  // minZoom: 2,
});
const styleUrl = "mapbox://styles/mapbox/dark-v10";

interface DeviceInfo {
  lat: number;
  lon: number;
  order: number;
  timestamp: string;
  speed: number;
}

interface DeviceMetaData {
  lat: number;
  lon: number;
  deviceID: string;
  zoom?: number;
}

interface Coordinates {
  lat: number;
  lon: number;
  zoom?: number;
}

function App() {
  const [deviceIDs, setDeviceIDs] = useState<DeviceMetaData[]>([]);
  const [deviceInfo, setDeviceInfo] = useState<DeviceInfo[]>([]);
  const [mapCenter, setMapCenter] = useState<Coordinates>({} as Coordinates);
  const [selectedDevice, setSelectedDevice] = useState("");

  useEffect(() => {
    fetch("http://localhost:8000/alldevices")
      .then((r) => r.json())
      .then((data) => setDeviceIDs(data as []))
      .catch((e) => console.error(e));
  }, []);

  useEffect(() => {
    if (selectedDevice !== "") {
      fetch(`http://localhost:8000/device/${selectedDevice}`)
        .then((r) => r.json())
        .then((data) => setDeviceInfo(data as []))
        .catch((e) => console.error(e));
    }
  }, [selectedDevice]);

  const addHeatMap = (map: mapboxgl.Map, tileServer: string) => {
    map.addSource("public.trips", {
      type: "vector",
      tiles: [tileServer],
    });
    map.addLayer({
      id: "public.trips",
      type: "circle",
      source: "public.trips",
      "source-layer": "default",
      paint: {
        "circle-radius": 3,
        "circle-color": "#223b53",
        "circle-stroke-color": "white",
        "circle-stroke-width": 1,
        "circle-opacity": 0.5,
      },
    });

    map.addSource("public.device_trips", {
      type: "vector",
      tiles: [tileServer],
    });
    map.addLayer({
      id: "public.device_trips",
      type: "line",
      source: "public.device_trips",
      "source-layer": "default",
      layout: {
        "line-join": "round",
        "line-cap": "round",
      },
      paint: {
        "line-color": "#ff69b4",
        "line-width": 1,
      },
    });

    map.on("mousemove", "public.trips", () => {
      map.getCanvas().style.cursor = "pointer";
    });

    map.on("click", "public.trips", (event) => {
      setMapCenter({ lat: event.lngLat.lat, lon: event.lngLat.lng, zoom: map.getZoom() });
      setSelectedDevice(event.features![0].properties!.devicekey as string);
      console.log("Event Features!!", event.features);
      console.log("Selected DEvice ID: ", event.features![0].properties!.devicekey);
      console.log("Selected Point Num: ", event.features![0].properties!.pointnum);
      console.log(
        "Selected Coord Timestamp: ",
        event.features![0].properties!.coord_timestamp,
      );
    });
  };

  return (
    <div className="App">
      <h1>DIMO User Trips 🚗</h1>
      <div className="Container">
        <MapBox
          onStyleLoad={(map) => {
            addHeatMap(
              map,
              "http://localhost:7800/public.device_trips,public.trips/{z}/{x}/{y}.pbf",
            );
          }}
          zoom={mapCenter.zoom ? [mapCenter.zoom] : [2.5]}
          center={mapCenter.lat ? [mapCenter.lon, mapCenter.lat] : [-50.200489, 37.38948]}
          style={styleUrl}
          containerStyle={{
            height: "45vh",
            width: "100%",
          }}
        ></MapBox>
        <div className="Table">
          <div className="DropDown">
            <h2>This is the selected device: {selectedDevice}</h2>
            <label htmlFor="DeviceIDs">Select a Device ID:</label>
            <select
              name="DeviceIDs"
              id="DeviceIDs"
              onChange={(e) => {
                const data = JSON.parse(e.target.value) as DeviceMetaData;
                setMapCenter({ lat: data.lat, lon: data.lon, zoom: 7 });
                console.log("logging data", data);
                setSelectedDevice(data.deviceID);
              }}
              value={selectedDevice}
            >
              <option key="" value="">
                Select a device
              </option>
              {deviceIDs.map((device) => (
                <option key={device.deviceID} value={JSON.stringify(device)}>
                  {device.deviceID}
                </option>
              ))}
            </select>
            <div>
              <table>
                <tr>
                  <th></th>
                  <th>Longitude</th>
                  <th>Longitude</th>
                  <th>Speed</th>
                  <th>Timestamp</th>
                </tr>
                {selectedDevice !== "" &&
                  deviceInfo.map((info, i) => {
                    return (
                      <tr key={i}>
                        <th>{info.order}</th>
                        <th>{info.lat}</th>
                        <th>{info.lon}</th>
                        <th>{info.speed}</th>
                        <th>{info.timestamp}</th>
                      </tr>
                    );
                  })}
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
