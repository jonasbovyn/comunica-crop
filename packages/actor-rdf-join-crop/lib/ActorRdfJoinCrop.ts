// eslint-disable-next-line eslint-comments/disable-enable-pair
/* eslint-disable id-length*/

import * as fs from 'fs/promises';
import { argv, env } from 'node:process';
import * as path from 'path';
import { ActorQueryOperation } from '@comunica/bus-query-operation';
import type { IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  MediatorRdfJoin,
  PhysicalJoinType } from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { Bindings, IJoinEntry, IQueryOperationResultBindings, MetadataBindings,
  IActionContext } from '@comunica/types';
import { AsyncIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';

// Run with --experimental-wasi-unstable-preview1

// eslint-disable-next-line import/no-unresolved
import { WASI } from 'wasi';

import { IdpFindPlan } from './crop/idp/IdpFindPlan';
import { selectRobustPlan } from './crop/PlanRobustness';
import { QueryOperator } from './crop/QueryOperator';
import type { IQueryLeaf, IQueryNode, IQueryPlan } from './crop/QueryPlan';
import { WasmIdpFindPlan } from './wasm/WasmIdpFindPlan';

interface ICropSettings {
  k: number;
  t: number;
  mode: 'wasm' | 'js';
  // Overrides the calculated plan, for testing purpose
  plan?: IQueryPlan;
  // Used for benchmarking optimization time
  skipEval: boolean;
}

/**
 * An RDF join actor that joins 3 or more streams by trying to find the optimal
 * order using Iterative Dynamic Programming and Crop
 */
export class ActorRdfJoinCrop extends ActorRdfJoin {
  public readonly mediatorJoin: MediatorRdfJoin;

  public static readonly FACTORY = new Factory();

  private readonly mode: 'js' | 'wasm';

  private wasmInstance?: WebAssembly.WebAssemblyInstantiatedSource;

  private readonly k: number;
  private readonly t = 5;
  private readonly robustnessThreshold = 0.05;
  private readonly costThreshold = 0.3;

  private readonly IActionContext: any;

  private readonly hashType: PhysicalJoinType = 'symmetric-hash';
  private readonly nestedLoopType: PhysicalJoinType = 'nested-loop';

  private context: IActionContext;

  public constructor(args: IActorRdfJoinCropArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'crop',
      limitEntries: 2,
      limitEntriesMin: true,
    });
  }

  private async joinPlanRecursively(
    plan: IQueryPlan, entries: IJoinEntry[], action: IActionRdfJoin,
  ): Promise<IJoinEntry> {
    if (plan.triples === 1) {
      return entries[(<IQueryLeaf>plan).entryIndex];
    }

    const leftEntry: IJoinEntry = await this.joinPlanRecursively((<IQueryNode>plan).left, entries, action);
    const operator = (<IQueryNode>plan).op === QueryOperator.Hash ? this.hashType : this.nestedLoopType;
    const rightEntry: IJoinEntry = await this.joinPlanRecursively((<IQueryNode>plan).right, entries, action);

    return {
      output: ActorQueryOperation.getSafeBindings(await this.mediatorJoin
        .mediate({
          type: action.type,
          physicalType: operator,
          entries: [ leftEntry, rightEntry ],
          context: action.context,
        })),
      operation: ActorRdfJoinCrop.FACTORY
        .createJoin([ leftEntry.operation, rightEntry.operation ], false),
    };
  }

  protected async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const context = action.context;
    this.context = context;
    const entries: IJoinEntry[] = [ ...action.entries ];

    let cropSettings: ICropSettings = { k: this.k, t: this.t, mode: this.mode, skipEval: false };
    if (context.has(KeysInitQuery.overrideCropSettings)) {
      const overrideSettings: ICropSettings = context.get(KeysInitQuery.overrideCropSettings)!;

      if (overrideSettings.plan !== undefined) {
        const result: IJoinEntry = await this.joinPlanRecursively(overrideSettings.plan, entries, action);

        return {
          result: result.output,
        };
      }

      cropSettings = {
        k: overrideSettings.k ?? this.k,
        t: overrideSettings.t ?? this.t,
        mode: overrideSettings.mode ?? this.mode,
        skipEval: overrideSettings.skipEval ?? false,
      };
    }

    const start = process.hrtime.bigint();

    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const bestPlans: IQueryPlan[] = await (cropSettings.mode === 'wasm' ?
      this.getOutputWasm(action, cropSettings) :
      this.getOutputJs(action, cropSettings)
    );

    const bestPlan = selectRobustPlan(bestPlans, this.robustnessThreshold, this.costThreshold, entries, metadatas);

    this.benchmarkTime('crop-idp', start);

    const dataFactory = new DataFactory();
    let output: IQueryOperationResultBindings;
    if (cropSettings.skipEval) {
      output = {
        type: 'bindings',
        bindingsStream: new AsyncIterator<Bindings>(),
        async metadata() {
          return {
            variables: [ dataFactory.variable('v0'), dataFactory.variable('v1') ],
            cardinality: { type: 'exact', value: 0 },
            canContainUndefs: false,
          };
        },
      };
    } else {
      output = (await this.joinPlanRecursively(bestPlan, entries, action)).output;
    }

    return {
      result: output,
    };
  }

  private async getOutputJs(action: IActionRdfJoin, settings: ICropSettings): Promise<IQueryPlan[]> {
    // Determine the two smallest streams by estimated count
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const planFinder = new IdpFindPlan(action.entries, metadatas);
    return planFinder.findPlan(settings.k, settings.t);
  }

  private async instantiateWasm(context: IActionContext): Promise<void> {
    if (this.wasmInstance === undefined) {
      const start = process.hrtime.bigint();

      const wasmBuffer = await fs.readFile(path.resolve(__dirname, 'wasm/bin/cropcpp.wasm'));

      const wasiConfig = new WASI({
        args: argv,
        env,
      });

      const wasmConfig = {
        wasi_snapshot_preview1: wasiConfig.wasiImport,
        env: {
          // https://github.com/WebAssembly/WASI/issues/82
          // eslint-disable-next-line @typescript-eslint/no-empty-function
          emscripten_notify_memory_growth(index: number) { },
        },
      };

      const wasmModule = await WebAssembly.instantiate(wasmBuffer, wasmConfig);
      wasiConfig.initialize(wasmModule.instance);

      this.wasmInstance = wasmModule;

      this.benchmarkTime('optimization-time', start);
    }
  }

  private async getOutputWasm(action: IActionRdfJoin, settings: ICropSettings): Promise<IQueryPlan[]> {
    const entries: IJoinEntry[] = [ ...action.entries ];
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);

    await this.instantiateWasm(action.context);

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-expect-error
    const planFinder = new WasmIdpFindPlan(metadatas, this.wasmInstance.instance.exports);

    return planFinder.findPlan(settings.k, settings.t);
  }

  protected async getJoinCoefficients(
    action: IActionRdfJoin,
    metadatas: MetadataBindings[],
  ): Promise<IMediatorTypeJoinCoefficients> {
    return {
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    };
  }

  private benchmarkTime(id: string, start: bigint): void {
    // Cut off macroseconds
    const elapsedTimeBigint = (process.hrtime.bigint() - start) / BigInt(1_000);
    // Convert to miliseconds with decimal point
    const elapsedTime = Number(elapsedTimeBigint) / 1_000;
    this.benchmark(id, elapsedTime);
  }

  private benchmark(id: string, log: number): void {
    if (this.context.has(KeysInitQuery.benchmarkTimeLog)) {
      const benchmarkTimeLog: (id: string, elapsedTime: number) => void = this.context
        .get(KeysInitQuery.benchmarkTimeLog)!;
      benchmarkTimeLog(id, log);
    }
  }
}

export interface IActorRdfJoinCropArgs extends IActorRdfJoinArgs {
  /**
   * A mediator for joining Bindings streams
   */
  mediatorJoin: MediatorRdfJoin;
  mode: string;
  k: number;
}
