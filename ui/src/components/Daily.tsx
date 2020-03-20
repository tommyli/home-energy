import React, { useEffect, useState } from "react";
import Grid from "@material-ui/core/Grid";
import Plot from "react-plotlyjs-ts";
import db from "../shared/firestore";

function Daily() {
  const [dayData, updateDayData] = useState({ meterConsumptions: [] });

  useEffect(() => {
    let docRef = db
      .collection("sites")
      .doc("6408091979")
      .collection("dailies")
      .doc("20141127");

    docRef.onSnapshot(
      docSnapshot => {
        let docData: any = docSnapshot.data();
        updateDayData({ meterConsumptions: docData.meter_consumptions });
      },
      err => {
        console.log(`Encountered error: ${err}`);
      }
    );
  }, []);

  return (
    <div>
      <Grid
        container
        direction="row"
        spacing={2}
        justify={"flex-start"}
        alignItems={"flex-start"}
      >
        <Grid item>
          <DailyChart dayData={dayData} />
        </Grid>
      </Grid>
    </div>
  );
}

export default Daily;

function DailyChart({ dayData }: { dayData: any }) {
  let data = [
    {
      x: dayData.meterConsumptions.map((v: number, i: number) => i + 1),
      y: dayData.meterConsumptions,
      type: "bar"
    }
  ];

  return (
    <div className="item">
      <div>
        <Plot data={data} />
      </div>
    </div>
  );
}
