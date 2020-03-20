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
  const consumptions = {
    x: [1, 2, 3, 4],
    y: [0.42, 0.3, 1.1, 0.56],
    name: "Consumption",
    type: "bar"
  };
  const generations = {
    x: [1, 2, 3, 4],
    y: [-1.19, -0.5, -0.87, 0.0],
    name: "Generation",
    type: "bar"
  };

  const data = [consumptions, generations];

  const layout = {
    barmode: "relative"
  };

  const config = {
    editable: false,
    scrollZoom: false,
    displayModeBar: false,
  };
  return (
    <div className="item">
      <div>
        <Plot data={data} layout={layout} config={config} />
      </div>
    </div>
  );
}
