// eslint-disable-next-line eslint-comments/disable-enable-pair
/* eslint-disable id-length*/
/* global BigInt */

import type { MetadataBindings } from '@comunica/types';
import { ActorRdfJoinCrop } from '../ActorRdfJoinCrop';
import type { IQueryPlan } from '../crop/QueryPlan';
import { WasmPlansDeserializer } from './deserializer/WasmPlansDeserializer';

interface IWasmExpectedExports {
  findPlanFromWasm: (k: number, t: number, metadatasSize: number, metadatas: number) => number;
  malloc: (bytesSize: number) => number;
  free: (address: number) => void;
  memory: WebAssembly.Memory;
}

export class WasmIdpFindPlan {
  public constructor(
    private readonly metadatas: MetadataBindings[],
    private readonly wasmExports: IWasmExpectedExports,
  ) {

  }

  private static bitmaskVariables(metadata: MetadataBindings, variableToId: Map<string, number>): number {
    let result = 0;
    for (const variable of metadata.variables) {
      // eslint-disable-next-line no-bitwise
      result |= 1 << variableToId.get(variable.value)!;
    }
    return result;
  }

  private createMetadatas(): number {
    const allVariables = [ ...new Set(this.metadatas.flatMap(binding => binding.variables.map(v => v.value))) ];
    // AllVariables.sort((one, two) => (one > two ? -1 : 1));

    const variableToId: Map<string, number> = new Map();
    allVariables.forEach((value, index) => variableToId.set(value, index));

    const baseAddress: number = this.wasmExports.malloc(3 * 4 * this.metadatas.length);
    const view = new Int32Array(this.wasmExports.memory.buffer);

    let address = baseAddress;
    for (const metadata of this.metadatas) {
      view[address / 4] = metadata.cardinality.value;
      view[(address / 4) + 1] = metadata.pageSize ?? -1;
      view[(address / 4) + 2] = WasmIdpFindPlan.bitmaskVariables(metadata, variableToId);
      address += 3 * 4;
    }

    return baseAddress;
  }

  public findPlan(k: number, t: number): IQueryPlan[] {
    // Global.gc();
    // const initialMem = process.memoryUsage().heapUsed;

    const metadatasAddress = this.createMetadatas();

    // // eslint-disable-next-line no-console
    // console.log(this.wasmExports.memory.buffer.byteLength);

    const resultsAddress = this.wasmExports.findPlanFromWasm(k, t, this.metadatas.length, metadatasAddress);

    // // eslint-disable-next-line no-console
    // console.log(this.wasmExports.memory.buffer.byteLength);

    const deserializer = new WasmPlansDeserializer(this.wasmExports.memory, resultsAddress);
    const result = deserializer.deserializePlans();

    // Wasm memory is never shrunk, only growing, so it's fine to only check once
    // const memUsage = process.memoryUsage();
    // // eslint-disable-next-line no-console
    // console.log(memUsage);
    // ActorRdfJoinCrop.benchmark('crop-memory', memUsage.heapUsed - initialMem);

    ActorRdfJoinCrop.benchmark('crop-memory', this.wasmExports.memory.buffer.byteLength);

    this.wasmExports.free(resultsAddress);
    this.wasmExports.free(metadatasAddress);
    return result;
  }
}
