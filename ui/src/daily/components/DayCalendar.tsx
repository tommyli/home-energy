import React from "react";
import dayjs, { Dayjs } from "dayjs";
import { DatePicker } from "@material-ui/pickers";
import { MaterialUiPickersDate } from "@material-ui/pickers/typings/date";

export interface DayCalendarProps {
  value?: Dayjs;
  minDate?: Dayjs;
  maxDate?: Dayjs;
  onDateChange: (date: Dayjs) => void;
}

export default function DayCalendar({
  value = dayjs(),
  minDate = dayjs(new Date(1990, 1, 1)),
  maxDate = dayjs(new Date(2099, 12, 31)),
  onDateChange
}: DayCalendarProps) {
  const onChange = (date: MaterialUiPickersDate): void => {
    if (date) {
      onDateChange(date);
    }
  };

  return (
    <DatePicker
      autoOk
      orientation="landscape"
      variant="static"
      openTo="date"
      value={value}
      minDate={minDate}
      maxDate={maxDate}
      onChange={onChange}
    />
  );
}
