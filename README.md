# DIMO Trips API

AutoPis often transmit engine start (`vehicle/engine/running`) and stop (`vehicle/engine/stopped`) events. I've heard these are unreliable.

From Teslas, we get a rich data point roughly every minute that the car is online. The plan is to do some simple [session windowing](https://developer.confluent.io/tutorials/create-session-windows/confluent.html) on data points with non-zero speed.

![](./flowchart.png "Flowchart of Segment Processing Logic")

```mermaid
flowchart TD
  new --> C1{New\nVehicle?}
  C1 -->|Yes| FU[latest = new]
  C1 -->|No| S2{Active\nsegment?}

  S2 -->|Yes| S3{"Move ≥ 10m"}
  S3 -->|Yes| S4{"lastMove ≥ 5min"}
  S4 -->|Yes| S5["End segment"]

  S3 -->|Yes| S6["lastMove = new"]

  S2 -->|No| S7{"Moved ≥ 10m"}
  S6 --> FU
  S5 --> FU
  S7 -->|Yes| S8["Start segment"]
  S7 -->|No| FU
  S8 --> FU
```
