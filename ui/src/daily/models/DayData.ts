import dayjs, { Dayjs } from "dayjs";
import lodash from "lodash";

class DayData {
  private _intervalDate: Dayjs = dayjs().subtract(1, "day");
  private _meterConsumptions: readonly number[] = Object.freeze([]);
  private _meterGenerations: readonly number[] = Object.freeze([]);
  private _solarGenerations: readonly number[] = Object.freeze([]);
  private _chargeQuantities: readonly number[] = Object.freeze([]);
  private _dischargeQuantities: readonly number[] = Object.freeze([]);

  constructor(
    intervalDate: Dayjs,
    meterConsumptions: number[],
    meterGenerations: number[],
    solarGenerations: number[],
    chargeQuantities: number[],
    dischargeQuantities: number[]
  ) {
    this._intervalDate = intervalDate;
    this._meterConsumptions = meterConsumptions;
    this._meterGenerations = meterGenerations;
    this._solarGenerations = solarGenerations;
    this._chargeQuantities = chargeQuantities;
    this._dischargeQuantities = dischargeQuantities;
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

  get meterConsumptions(): number[] {
    return this._meterConsumptions.map((v) => v);
  }

  get meterGenerations(): number[] {
    return this._meterGenerations.map((v) => v);
  }

  get solarGenerations(): number[] {
    return this._solarGenerations.map((v) => v);
  }

  get chargeQuantities(): number[] {
    return this._chargeQuantities.map((v) => v);
  }

  get dischargeQuantities(): number[] {
    return this._dischargeQuantities.map((v) => v);
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

export const EMPTY_YESTERDAY = new DayData(
  dayjs().subtract(1, "day"),
  [],
  [],
  [],
  [],
  []
);

export const INTERVAL_LENGTH: 15 | 30 = 30;

export const UOM = {
  id: "KWH",
  name: "kWh",
};

function fromFirestoreDoc(docData: any): DayData {
  if (docData && docData.interval_date) {
    const intervalDate: Dayjs = dayjs(docData.interval_date.toDate());
    const meterConsumptions: number[] =
      docData?.meter_consumptions_kwh ?? Object.freeze([]);
    const meterGenerations: number[] =
      docData?.meter_generations_kwh ?? Object.freeze([]);
    const solarGenerations: number[] =
      docData?.solar_generations_kwh ?? Object.freeze([]);
    const chargeQuantities: number[] =
      docData?.charge_quantities_kwh ?? Object.freeze([]);
    const dischargeQuantities: number[] =
      docData?.discharge_quantities_kwh ?? Object.freeze([]);

    return new DayData(
      intervalDate,
      meterConsumptions,
      meterGenerations,
      solarGenerations,
      chargeQuantities,
      dischargeQuantities
    );
  } else {
    console.debug(`Invalid docData ${JSON.stringify(docData)}`);
    return EMPTY_YESTERDAY;
  }
}

export { DayData, fromFirestoreDoc };
