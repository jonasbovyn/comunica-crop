import type { QueryOperator } from '../../crop/QueryOperator';
import type { IQueryLeaf, IQueryNode, IQueryPlan } from '../../crop/QueryPlan';
import { ArrayBufferReader } from './ArrayBufferReader';

export class WasmPlansDeserializer {
  private readonly reader: ArrayBufferReader;

  public constructor(
    wasmMemory: WebAssembly.Memory,
    readAddress: number,
  ) {
    this.reader = new ArrayBufferReader(wasmMemory.buffer, readAddress);
  }

  public deserializePlans(): IQueryPlan[] {
    const plansSize = this.reader.readInt();
    const plans: IQueryPlan[] = [];
    for (let i = 0; i < plansSize; i++) {
      plans.push(this.deserializePlan());
    }
    return plans;
  }

  private deserializePlan(): IQueryPlan {
    const triples = this.reader.readInt();
    const cost = this.reader.readDouble();
    const type = this.reader.readByte();
    if (type === 0) {
      return this.deserializeLeaf(triples, cost);
    }
    // Type === 1
    return this.deserializeNode(triples, cost);
  }

  private deserializeNode(triples: number, cost: number): IQueryNode {
    const left = this.deserializePlan();
    const operator: QueryOperator = this.reader.readByte();
    const right = this.deserializePlan();

    return {
      cost,
      triples,
      left,
      op: operator,
      right,
    };
  }

  private deserializeLeaf(triples: number, cost: number): IQueryLeaf {
    const entryIndex = this.reader.readInt();
    return {
      cost,
      triples,
      entryIndex,
    };
  }
}
