import React from "react";
import Plot from "react-plotlyjs-ts";
import { DayData, EMPTY_YESTERDAY } from "../models/DayData";
import { UOM } from "../daily";

export interface DailyChartProps {
  dayData?: DayData;
}

export default function DailyChart({
  dayData = EMPTY_YESTERDAY
}: DailyChartProps) {
  const chartConsumptions = {
    x: dayData.intervalLables,
    y: dayData.meterConsumptions,
    name: `Consumption (${dayData.meterConsumptionTotal} ${UOM.name})`,
    type: "bar"
  };
  const chartGenerations = {
    x: dayData.intervalLables,
    y: dayData.meterGenerations,
    name: `Generation (${dayData.meterGenerationTotal} ${UOM.name})`,
    type: "bar"
  };

  const data = [chartConsumptions, chartGenerations];

  const layout = {
    title: "Meter",
    barmode: "relative",
    xaxis: {
      title: "Time of Day",
      tickangle: -45
    },
    yaxis: {
      title: `Usage (${UOM.name})`
    },
    legend: {
      x: -0.2,
      y: 1.3
    }
  };

  const config = {
    editable: false,
    scrollZoom: false,
    displayModeBar: true
  };

  return (
    <div className="item">
      <div>
        <Plot data={data} layout={layout} config={config} />
      </div>
    </div>
  );
}
