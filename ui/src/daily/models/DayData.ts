import dayjs, { Dayjs } from "dayjs";
import lodash from "lodash";
import { INTERVAL_LENGTH } from "../daily";

class DayData {
  private _intervalDate: Dayjs = dayjs().subtract(1, "day");
  private _meterConsumptions: readonly number[] = Object.freeze([]);
  private _meterGenerations: readonly number[] = Object.freeze([]);
  private _batteryCharges: readonly number[] = Object.freeze([]);

  constructor(
    intervalDate: Dayjs,
    meterConsumptions: number[],
    meterGenerations: number[],
    batteryCharges: number[]
  ) {
    this._intervalDate = intervalDate;
    this._meterConsumptions = meterConsumptions;
    this._meterGenerations = meterGenerations;
    this._batteryCharges = batteryCharges;
  }

  get intervalDate(): Dayjs {
    return this._intervalDate;
  }

  get intervals(): number[] {
    return this.meterConsumptions.map((v: number, i: number) => i + 1);
  }

  get intervalLables(): string[] {
    return this.intervals.map((i: number) => {
      const label = this.intervalDate
        .startOf("day")
        .add((i - 1) * INTERVAL_LENGTH, "minute")
        .format("HH:mm");

      return label;
    });
  }
  get meterConsumptions(): readonly number[] {
    return this._meterConsumptions;
  }

  get meterGenerations(): readonly number[] {
    return this._meterGenerations;
  }

  get batteryCharges(): readonly number[] {
    return this._batteryCharges;
  }

  get meterConsumptionTotal(): number {
    return parseFloat(
      (lodash.reduce(this.meterConsumptions, (sum, n) => sum + n) ?? 0).toFixed(
        1
      )
    );
  }

  get meterGenerationTotal(): number {
    return parseFloat(
      (lodash.reduce(this.meterGenerations, (sum, n) => sum + n) ?? 0).toFixed(
        1
      )
    );
  }
}

const EMPTY_YESTERDAY = new DayData(dayjs().subtract(1, "day"), [], [], []);

export { DayData, EMPTY_YESTERDAY };
