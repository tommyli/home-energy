import Grid from "@material-ui/core/Grid";
import dayjs, { Dayjs } from "dayjs";
import React, { useEffect, useState } from "react";
import db from "../../shared/firestore";
import { DayData, EMPTY_YESTERDAY, fromFirestoreDoc } from "../models/DayData";
import DailyChart from "./DailyChart";
import DayCalendar from "./DayCalendar";

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
      (querySnapshot) => {
        querySnapshot.docChanges().forEach((change) => {
          const docData: any = change.doc.data();
          console.debug(`New latestDaily, doc=${JSON.stringify(docData)}`);
          updateSelectedDate(dayjs(docData.interval_date.toDate()));
          updateMaxDate(dayjs(docData.interval_date.toDate()));
          updateDayData(fromFirestoreDoc(docData));
        });
      },
      (err) => {
        console.log(`Encountered error: ${err}`);
      }
    );

    earliestDaily.onSnapshot(
      (querySnapshot) => {
        querySnapshot.docChanges().forEach((change) => {
          const docData: any = change.doc.data();
          console.debug(`New earliestDaily, doc=${JSON.stringify(docData)}`);
          updateMinDate(dayjs(docData.interval_date.toDate()));
        });
      },
      (err) => {
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
        (docSnapshot) => {
          const docData: any = docSnapshot.data();
          updateDayData(fromFirestoreDoc(docData));
        },
        (err) => {
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
          <DayCalendar
            value={selectedDate}
            minDate={minDate}
            maxDate={maxDate}
            onDateChange={onDateChange}
          />
        </Grid>
        <Grid item>
          <DailyChart dayData={dayData} />
        </Grid>
      </Grid>
    </div>
  );
}

export default Daily;
