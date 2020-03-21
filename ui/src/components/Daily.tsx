import React, { useEffect, useState } from "react";
import Grid from "@material-ui/core/Grid";
import Plot from "react-plotlyjs-ts";
import db from "../shared/firestore";
import dayjs, { Dayjs } from "dayjs";
import { DatePicker } from "@material-ui/pickers";
import { update } from "plotly.js";

const INTERVAL_LENGTH = 30;
const UOM = {
  id: "KWH",
  name: "kWh"
};

function Daily() {
  const emptyDay = {
    intervalDate: dayjs().subtract(1, "day"),
    batteryCharges: [],
    meterConsumptions: [],
    meterGenerations: []
  };
  const [dayData, updateDayData] = useState(emptyDay);
  const [minDate, updateMinDate] = useState(dayjs(new Date(2000, 1, 1)));
  const [maxDate, updateMaxDate] = useState(dayjs().subtract(1, "day"));
  const [selectedDate, updateSelectedDate] = useState(
    dayjs().subtract(1, "day")
  );

  useEffect(() => {
    const latestDaily = db
      .collection("sites")
      .doc("6408091979")
      .collection("dailies")
      .orderBy("interval_date", "desc")
      .limit(1);
    const earliestDaily = db
      .collection("sites")
      .doc("6408091979")
      .collection("dailies")
      .orderBy("interval_date")
      .limit(1);

    latestDaily.onSnapshot(
      querySnapshot => {
        querySnapshot.docChanges().forEach(change => {
          const docData: any = change.doc.data();
          console.log(
            `First update latest docData=${JSON.stringify(
              docData
            )}, maxDate=${dayjs(docData.interval_date.toDate())}`
          );
          updateSelectedDate(dayjs(docData.interval_date.toDate()));
          updateMaxDate(dayjs(docData.interval_date.toDate()));
          updateDayData(docToDailyData(docData));
        });
      },
      err => {
        console.log(`Encountered error: ${err}`);
      }
    );

    earliestDaily.onSnapshot(
      querySnapshot => {
        querySnapshot.docChanges().forEach(change => {
          const docData: any = change.doc.data();
          console.log(
            `First update earliest docData=${JSON.stringify(
              docData
            )}, minDate=${dayjs(docData.interval_date.toDate())}`
          );
          updateMinDate(dayjs(docData.interval_date.toDate()));
        });
      },
      err => {
        console.log(`Encountered error: ${err}`);
      }
    );
  }, []);

  const onDateChange = (date: Dayjs) => {
    console.log(`on date change ${date.format("YYYYMMDD")}`);
    if (date.isSame(minDate) || date.isAfter(minDate)) {
      updateSelectedDate(date);
      const docRef = db
        .collection("sites")
        .doc("6408091979")
        .collection("dailies")
        .doc(`${date.format("YYYYMMDD")}`);

      docRef.onSnapshot(
        docSnapshot => {
          const docData: any = docSnapshot.data();
          updateDayData(docToDailyData(docData));
        },
        err => {
          console.log(`Encountered error: ${err}`);
        }
      );
    }
  };

  return (
    <div>
      <Grid
        container
        direction="row"
        spacing={2}
        justify={"center"}
        alignItems={"flex-start"}
      >
        <Grid item>
          <DailyChart dayData={dayData} />
        </Grid>
        <Grid item>
          <DayCalendar
            date={selectedDate}
            onDateChange={onDateChange}
            minDate={minDate}
            maxDate={maxDate}
          />
        </Grid>
      </Grid>
    </div>
  );
}

export default Daily;

function DayCalendar({
  date,
  onDateChange,
  minDate,
  maxDate
}: {
  date: Dayjs;
  onDateChange: any;
  minDate: Dayjs;
  maxDate: Dayjs;
}) {
  return (
    <DatePicker
      autoOk
      orientation="landscape"
      variant="static"
      openTo="date"
      initialFocusedDate={date}
      value={date}
      minDate={minDate}
      maxDate={maxDate}
      onChange={onDateChange}
    />
  );
}

function DailyChart({ dayData }: { dayData: any }) {
  const intervals = dayData.meterConsumptions.map(
    (v: number, i: number) => i + 1
  );
  const xLabels = intervalsToTimeLabels(dayData.intervalDate, intervals);

  const meterCumsumptionTotal = dayData.meterConsumptions.reduce(
    (sum: number, n: number) => sum + n,
    0
  );
  const generationCumsumptionTotal = dayData.meterGenerations.reduce(
    (sum: number, n: number) => sum + n,
    0
  );
  const meterConsumptions = {
    x: xLabels,
    y: dayData.meterConsumptions,
    name: `Consumption (${parseFloat(meterCumsumptionTotal.toFixed(1))} ${
      UOM.name
    })`,
    type: "bar"
  };
  const meterGenerations = {
    x: xLabels,
    y: dayData.meterGenerations,
    name: `Generation (${parseFloat(generationCumsumptionTotal.toFixed(1))} ${
      UOM.name
    })`,
    type: "bar"
  };

  const data = [meterConsumptions, meterGenerations];

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

function docToDailyData(docData: any) {
  return {
    intervalDate: dayjs(docData.interval_date.toDate()),
    batteryCharges: docData.battery_charges,
    meterConsumptions: docData.meter_consumptions,
    meterGenerations: docData.meter_generations
  };
}

function intervalsToTimeLabels(
  intervalDate: Dayjs,
  intervals: number[]
): string[] {
  return intervals.map((i: number) => {
    const label = intervalDate
      .startOf("day")
      .add((i - 1) * INTERVAL_LENGTH, "minute")
      .format("HH:mm");

    return label;
  });
}
