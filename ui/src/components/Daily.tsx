import * as React from "react";
import Grid from "@material-ui/core/Grid";
import Plot from "react-plotlyjs-ts";

function Daily() {
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
          <DailyGraph />
        </Grid>
      </Grid>
    </div>
  );
}

export default Daily;

function DailyGraph() {
  let data = [
    {
      x: ["a", "b", "c"],
      y: [1, 2, 3],
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
