import dayjs, { Dayjs } from "dayjs";
import { DayData, EMPTY_YESTERDAY } from "./models/DayData";

export const INTERVAL_LENGTH: 15 | 30 = 30;
export const UOM = {
  id: "KWH",
  name: "kWh"
};

export function docToDailyData(docData: any): DayData {
  if (docData && docData.interval_date) {
    return new DayData(
      dayjs(docData.interval_date.toDate()),
      docData.meter_consumptions,
      docData.meter_generations,
      docData.battery_charges
    );
  } else {
    console.debug(`Invalid docData ${JSON.stringify(docData)}`);
    return EMPTY_YESTERDAY;
  }
}
