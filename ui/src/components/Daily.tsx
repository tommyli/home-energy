import React, { useEffect, useState } from "react";
import Grid from "@material-ui/core/Grid";
import Plot from "react-plotlyjs-ts";
import db from "../shared/firestore";
import dayjs, { Dayjs } from "dayjs";
import { DatePicker } from "@material-ui/pickers";
import { update } from "plotly.js";

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
          let docData: any = change.doc.data();
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
          let docData: any = change.doc.data();
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
      let docRef = db
        .collection("sites")
        .doc("6408091979")
        .collection("dailies")
        .doc(`${date.format("YYYYMMDD")}`);

      docRef.onSnapshot(
        docSnapshot => {
          let docData: any = docSnapshot.data();
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

function docToDailyData(docData: any) {
  return {
    intervalDate: dayjs(docData.interval_date.toDate()),
    batteryCharges: docData.battery_charges,
    meterConsumptions: docData.meter_consumptions,
    meterGenerations: docData.meter_generations
  };
}
