const IEC_BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];

export function formatIecBytes(bytes: number) {
  let amount = bytes;
  let unit = 0;
  while (amount >= 1024 && unit < IEC_BYTE_UNITS.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  return `${amount.toLocaleString('en-US', { maximumFractionDigits: 2 })} ${IEC_BYTE_UNITS[unit]}`;
}
