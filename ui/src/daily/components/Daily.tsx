import React, { useEffect, useState } from "react";
import Grid from "@material-ui/core/Grid";
import db from "../../shared/firestore";
import dayjs, { Dayjs } from "dayjs";
import DailyChart from "./DailyChart";
import DayCalendar from "./DayCalendar";
import { docToDailyData } from "../daily";
import { DayData, EMPTY_YESTERDAY } from "../models/DayData";

function Daily() {
  const [dayData, updateDayData] = useState<DayData>(EMPTY_YESTERDAY);
  const [minDate, updateMinDate] = useState<Dayjs>(dayjs(new Date(2000, 1, 1)));
  const [maxDate, updateMaxDate] = useState<Dayjs>(dayjs().subtract(1, "day"));
  const [selectedDate, updateSelectedDate] = useState<Dayjs>(
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
          console.debug(`New latestDaily, doc=${JSON.stringify(docData)}`);
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
          console.debug(`New earliestDaily, doc=${JSON.stringify(docData)}`);
          updateMinDate(dayjs(docData.interval_date.toDate()));
        });
      },
      err => {
        console.log(`Encountered error: ${err}`);
      }
    );
  }, []);

  const onDateChange = (date: Dayjs) => {
    if (date && (date.isSame(minDate) || date.isAfter(minDate))) {
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
            value={selectedDate}
            minDate={minDate}
            maxDate={maxDate}
            onDateChange={onDateChange}
          />
        </Grid>
      </Grid>
    </div>
  );
}

export default Daily;
