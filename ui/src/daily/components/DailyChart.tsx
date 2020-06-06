import { zip } from "lodash";
import React from "react";
import Plot from "react-plotlyjs-ts";
import { DayData, EMPTY_YESTERDAY, UOM } from "../models/DayData";
export interface DailyChartProps {
  dayData?: DayData;
}

export default function DailyChart({
  dayData = EMPTY_YESTERDAY,
}: DailyChartProps) {
  const BAR_COLORS: Record<string, string> = {
    "Battery Discharge": "#aec7e8",
    "Solar Self Use": "#1f77b4",
    "Grid Consumption": "#c5b0d5",
    "Gross Solar Generation": "#c49c94",
    "Grid Generation": "#dbdb8d",
    "Battery Charge": "#8c564b",
  };

  const CHART_LAYOUT = {
    width: 800,
    height: 600,
    title: `Energy Breakdown for ${dayData.intervalDate.format("YYYY-MM-DD")}`,
    barmode: "relative",
    xaxis: {
      title: "Time of Day",
      tickangle: -45,
    },
    yaxis: {
      title: `Usage (${UOM.name})`,
    },
    legend: {
      x: -0.2,
      y: 1.3,
    },
  };

  const CHART_CONFIG = {
    editable: false,
    scrollZoom: false,
    displayModeBar: true,
  };

  const solarSelfs: number[] = zip(
    dayData.solarGenerations,
    dayData.meterGenerations,
    dayData.chargeQuantities
  ).map((arr: any[]) => arr[0] - arr[1] - arr[2]);

  const chartGriConsumptions = {
    x: dayData.intervalLables,
    y: dayData.meterConsumptions,
    name: `Grid Consumption`,
    type: "bar",
    marker: {
      color: BAR_COLORS["Grid Consumption"],
    },
  };

  const chartSolarSelfs = {
    x: dayData.intervalLables,
    y: solarSelfs,
    name: `Solar Self Use`,
    type: "bar",
    marker: {
      color: BAR_COLORS["Solar Self Use"],
    },
  };

  const chartBatteryDischarges = {
    x: dayData.intervalLables,
    y: dayData.dischargeQuantities,
    name: `Battery Discharge`,
    type: "bar",
    marker: {
      color: BAR_COLORS["Battery Discharge"],
    },
  };

  const chartGridGenerations = {
    x: dayData.intervalLables,
    y: dayData.meterGenerations.map((v) => v * -1),
    name: `Grid Generation`,
    type: "bar",
    marker: {
      color: BAR_COLORS["Grid Generation"],
    },
  };

  const chartBatteryCharges = {
    x: dayData.intervalLables,
    y: dayData.chargeQuantities.map((v) => v * -1),
    name: `Battery Charge`,
    type: "bar",
    marker: {
      color: BAR_COLORS["Battery Charge"],
    },
  };

  const data = [
    chartGriConsumptions,
    chartSolarSelfs,
    chartBatteryDischarges,
    chartBatteryCharges,
    chartGridGenerations,
  ];

  return (
    <div className="item">
      <div>
        <Plot data={data} layout={CHART_LAYOUT} config={CHART_CONFIG} />
      </div>
    </div>
  );
}
