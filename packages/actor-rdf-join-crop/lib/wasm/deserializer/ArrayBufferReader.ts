export class ArrayBufferReader {
  private readonly dataView: DataView;
  private index: number;

  public constructor(
    buffer: ArrayBuffer,
    offset: number,
  ) {
    this.dataView = new DataView(buffer, offset);
    this.index = 0;
  }

  public readInt(): number {
    const result = this.dataView.getInt32(this.index, true);
    this.index += 4;
    return result;
  }

  public readDouble(): number {
    const result = this.dataView.getFloat64(this.index, true);
    this.index += 8;
    return result;
  }

  public readByte(): number {
    const result = this.dataView.getUint8(this.index);
    this.index += 1;
    return result;
  }
}
