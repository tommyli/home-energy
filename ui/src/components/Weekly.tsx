import * as React from "react";
import Grid from "@material-ui/core/Grid";
import Plot from "react-plotlyjs-ts";

function Weekly() {
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
          <WeeklyChart />
        </Grid>
      </Grid>
    </div>
  );
}

export default Weekly;

function WeeklyChart() {
  let data = [
    {
      x: ["d", "e", "f", "g"],
      y: [1, 2, 3, 4],
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
